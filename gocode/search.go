package gocode

import (
	"appengine"
	"appengine/datastore"
	"appengine/memcache"
	"github.com/agonopol/go-stem/stemmer"
	"github.com/daviddengcn/go-villa"
	"github.com/daviddengcn/go-index"
	"log"
	"sort"
	"strings"
	"time"
	"unicode"
	"regexp"
)

type SearchResult struct {
	TotalResults int
	Docs         []*DocInfo
}

func authorOfPackage(pkg string) string {
	parts := strings.Split(pkg, "/")
	if len(parts) == 0 {
		return ""
	}

	switch parts[0] {
	case "github.com", "bitbucket.org":
		if len(parts) > 1 {
			return parts[1]
		}
	case "llamaslayers.net":
		return "Nightgunner5"
	case "launchpad.net":
		if len(parts) > 1 && strings.HasPrefix(parts[1], "~") {
			return parts[1][1:]
		}
	}
	return parts[0]
}

type DocInfo struct {
	Name         string    `datastore:",noindex"`
	Package      string    `datastore:",noindex"`
	Author       string    `datastore:",noindex"`
	LastUpdated  time.Time `datastore:",noindex"`
	StarCount    int       `datastore:",noindex"`
	Synopsis     string    `datastore:",noindex"`
	Description  string    `datastore:",noindex"`
	ImportedPkgs []string  `datastore:",noindex"`
	StaticScore  float64   `datastore:",noindex"`
	Imports      []string  `datastore:",noindex"`
	ProjectURL   string    `datastore:",noindex"`
	ReadmeFn     string    `datastore:",noindex"`
	ReadmeData   string    `datastore:",noindex"`

	MatchScore float64 `datastore:"-"`
	Score      float64 `datastore:"-"`
}

func (doc *DocInfo) updateStaticScore() {
	doc.StaticScore = calcStaticRank(doc)
}

func (doc *DocInfo) loadFromDB(c appengine.Context, id string) (err error, exists bool) {
	ddb := NewCachedDocDB(c, "doc")
	return ddb.Get(id, doc)
}

func (doc *DocInfo) saveToDB(c appengine.Context) error {
	ddb := NewCachedDocDB(c, "doc")
	return ddb.Put(doc.Package, doc)
}

func isTermSep(r rune) bool {
	return unicode.IsSpace(r) || unicode.IsPunct(r) || unicode.IsSymbol(r)
}

func normWord(word string) string {
	return string(stemmer.Stem([]byte(word)))
}

func CheckRuneType(last, current rune) index.RuneType {
	if isTermSep(current) {
		return index.TokenSep
	}

	if current > 128 {
		return index.TokenStart
	}

	if unicode.IsLetter(current) {
		if unicode.IsLetter(last) {
			return index.TokenBody
		}
		return index.TokenStart
	}

	if unicode.IsNumber(current) {
		if unicode.IsNumber(last) {
			return index.TokenBody
		}
		return index.TokenStart
	}

	return index.TokenStart
}

func isCamel(token string) bool {
	upper, lower := false, false
	for _, r := range token {
		if !unicode.IsLetter(r) {
			return false
		}

		if unicode.IsUpper(r) {
			upper = true
			if lower {
				break
			}
		} else {
			lower = true
		}
	}

	return upper && lower
}

func CheckCamel(last, current rune) index.RuneType {
	if unicode.IsUpper(current) {
		return index.TokenStart
	}

	return index.TokenBody
}

var patURL = regexp.MustCompile(`http[s]?://\S+`)

func filterURLs(text string) string {
	return patURL.ReplaceAllString(text, " ")
}

func appendTokens(tokens villa.StrSet, text string) villa.StrSet {
	/*
		for _, token := range strings.FieldsFunc(text, isTermSep) {
			token = normWord(token)
			tokens.Put(token)
		}
	*/
	
	text = filterURLs(text)
	
	lastToken := ""
	index.Tokenize(CheckRuneType, villa.NewPByteSlice([]byte(text)), func(token []byte) error {
		tokenStr := string(token)
		if isCamel(tokenStr) {
			last := ""
			index.Tokenize(CheckCamel, villa.NewPByteSlice(token), func(token []byte) error {
				tokenStr = string(token)
				tokenStr = normWord(tokenStr)
				tokens.Put(tokenStr)

				if last != "" {
					tokens.Put(last + "-" + string(tokenStr))
				}

				last = tokenStr
				return nil
			})
		}
		tokenStr = normWord(tokenStr)
		tokens.Put(tokenStr)

		if tokenStr[0] > 128 && len(lastToken) > 0 && lastToken[0] > 128 {
			tokens.Put(lastToken + tokenStr)
		}

		lastToken = tokenStr
		return nil
	})
	/*
		var s scanner.Scanner
		s.Init(bytes.NewReader([]byte(text)))
		for token := s.Scan(); token != scanner.EOF; token = s.Scan() {
			tokens.Put(s.TokenText())
		}
	*/

	return tokens
}

func matchToken(token string, text string, tokens villa.StrSet) bool {
	if strings.Index(text, token) >= 0 {
		return true
	}
	
	if tokens.In(token) {
		return true
	}
	
	for tk := range tokens {
		if strings.HasPrefix(tk, token) || strings.HasSuffix(tk, token) {
			return true
		}
	}
	
	return false
}

func fetchDocs(c appengine.Context, ids []string, docs []DocInfo) {
	var pDocs []*DocInfo
	var keys []*datastore.Key
	// fetch from memcache
	for i, id := range ids {
		mcID := prefixCachedDocDB + kindDocDB + ":" + id
		pDoc := &docs[i]
		if _, err := memcache.Gob.Get(c, mcID, pDoc); err == nil {
			continue
		}
		pDocs = append(pDocs, pDoc)
		keys = append(keys, datastore.NewKey(c, kindDocDB, id, 0, nil))
	}
	
	c.Infof("%d items from memcache", len(ids) - len(pDocs))
	if len(pDocs) == 0 {
		// all from memcache, nothing else to do
		return
	}
	
	var mcItems []*memcache.Item
	// fetch from datastore
	for offs := 0; offs < len(pDocs); {
		n := len(pDocs) - offs
		if n > 200 {
			n = 200
		}
		
		err := datastore.GetMulti(c, keys[offs:offs+n], pDocs[offs:offs+n])
		if err == nil {
			for i := 0; i < n; i ++ {
				key := keys[offs + i]
				id := key.StringID()
				mcID := prefixCachedDocDB + kindDocDB + ":" + id
				mcItems = append(mcItems, &memcache.Item{
					Key: mcID,
					Object: pDocs[offs + i],
				})
			}
		} else if me, ok := err.(appengine.MultiError); ok {
			for i := 0; i < n; i ++ {
				if !DocGetOk(me[i]) {
					continue
				}
				key := keys[offs + i]
				id := key.StringID()
				mcID := prefixCachedDocDB + kindDocDB + ":" + id
				mcItems = append(mcItems, &memcache.Item{
					Key: mcID,
					Object: pDocs[offs + i],
				})
			}
		} else {
			c.Errorf("fetchDocs: %v", err)
		}
		
		offs += n
	}
	
	c.Infof("%d items from datastore", len(mcItems))
	
	// save back to memcache
	err := memcache.Gob.SetMulti(c, mcItems)
	if err != nil {
		c.Errorf("fetchDocs: %v", err)
	}
}

func search(c appengine.Context, q string) (*SearchResult, villa.StrSet, error) {
	ts := NewTokenSet(c, "index:")

	tokens := appendTokens(nil, q)
	if len(tokens) == 0 {
		return &SearchResult{}, nil, nil
	}

	c.Infof("%d tokens for query %s", len(tokens), q)
	
	ids, err := ts.Search("doc", tokens)
	if err != nil {
		return nil, nil, err
	}
	c.Infof("%d ids got for query %s", len(ids), q)

	//ddb := NewCachedDocDB(c, kindDocDB)

	docs := make([]DocInfo, len(ids))
	/*
	for i, id := range ids {
		err, exists := ddb.Get(id, &docs[i])
		if err != nil {
			c.Errorf("  ddb.Get(%s,) failed: %v", id, err)
		}

		if exists {
			if docs[i].StaticScore < 1 {
				docs[i].updateStaticScore()
			}

			docs[i].MatchScore = calcMatchScore(&docs[i], tokens)
			docs[i].Score = (docs[i].StaticScore - 0.9) * docs[i].MatchScore
		}
	}
	*/
	fetchDocs(c, ids, docs)
	pDocs := make([]*DocInfo, 0, len(docs))
	for i := range docs {
		if docs[i].Package != "" {
			docs[i].MatchScore = calcMatchScore(&docs[i], tokens)
			docs[i].Score = (docs[i].StaticScore - 0.9) * docs[i].MatchScore
			
			pDocs = append(pDocs, &docs[i])
		}
	}
	c.Infof("%d available docs", len(pDocs))

	villa.SortF(len(pDocs), func(i, j int) bool {
		// true if doc i is before doc j
		ssi, ssj := pDocs[i].Score, pDocs[j].Score
		if ssi > ssj {
			return true
		}
		if ssi < ssj {
			return false
		}

		sci, scj := pDocs[i].StarCount, pDocs[j].StarCount
		if sci > scj {
			return true
		}
		if sci < scj {
			return false
		}

		pi, pj := pDocs[i].Package, pDocs[j].Package
		if len(pi) < len(pj) {
			return true
		}
		if len(pi) > len(pj) {
			return false
		}

		return pi < pj
	}, func(i, j int) {
		// Swap
		pDocs[i], pDocs[j] = pDocs[j], pDocs[i]
	})

	c.Infof("Docs sorted")
	return &SearchResult{
		TotalResults: len(ids),
		Docs:         pDocs,
	}, tokens, nil
}

func doIndex(c appengine.Context, doc *DocInfo) error {
	ts := NewTokenSet(c, prefixIndex)
	var tokens villa.StrSet
	tokens = appendTokens(tokens, doc.Name)
	tokens = appendTokens(tokens, doc.Package)
	tokens = appendTokens(tokens, doc.Description)
	tokens = appendTokens(tokens, doc.ReadmeData)
	tokens = appendTokens(tokens, doc.Author)

	id := doc.Package

	log.Printf("  indexing %s, %v", id, tokens)
	err := ts.Index(fieldIndex, id, tokens)
	if err != nil {
		return err
	}

	ddb := NewCachedDocDB(c, kindDocDB)
	err = ddb.Put(id, doc)
	if err != nil {
		return err
	}

	return nil
}

func updateImported(c appengine.Context, pkg string) {
	log.Printf("  updateImported of %s ...", pkg)
	var doc DocInfo
	err, exists := doc.loadFromDB(c, pkg)
	if !exists || err != nil {
		// no such entity or fetching error, do nothing
		if err != nil {
			log.Printf("  [updateImported] ddb.Get(%s) failed: %v", pkg, err)
		}
		return
	}

	ts := NewTokenSet(c, "import:")
	importedPkgs, err := ts.Search("import", villa.NewStrSet(pkg))
	if err != nil {
		log.Printf("  [updateImported] ts.Search(%s) failed: %v", pkg, err)
		return
	}

	if len(doc.ImportedPkgs) != len(importedPkgs) {
		log.Printf("  [updateImported] inlinks of %s from %d to %d",
			pkg, len(doc.ImportedPkgs), len(importedPkgs))
	}
	sort.Strings(importedPkgs)
	doc.ImportedPkgs = importedPkgs
	doc.updateStaticScore()

	err = doc.saveToDB(c)
	if err != nil {
		log.Printf("  [updateImported] ddb.Put(%s) failed: %v", pkg, err)
	}
}

func diffStringList(l1, l2 []string) (diff []string) {
	sort.Strings(l1)
	sort.Strings(l2)
	
	for i, j := 0, 0; i < len(l1) || j < len(l2); {
		switch {
		case i == len(l1) || j < len(l2) && l2[j] < l1[i]:
			diff = append(diff, l2[j])
			j ++
			
		case j == len(l2) || i < len(l1) && l1[i] < l2[j]:
			diff = append(diff, l1[i])
			i ++
			
		default:
			i ++
			j ++
		}
	}
	
	return diff
}

func processDocument(c appengine.Context, d *DocInfo) error {
	ddb := NewCachedDocDB(c, kindDocDB)
	
	pkg := d.Package
	
	// fetch saved DocInfo
	var savedD DocInfo
	err, exists := ddb.Get(pkg, &savedD)
	if err != nil {
		return err
	}
	if exists && d.StarCount < 0 {
		d.StarCount = savedD.StarCount
	}

	// get imported packages
	ts := NewTokenSet(c, prefixImports)
	importedPkgs, err := ts.Search(fieldImports, villa.NewStrSet(pkg))
	if err != nil {
		return err
	}
	d.ImportedPkgs = importedPkgs
	
	// index imports
	err = ts.Index(fieldImports, pkg, villa.NewStrSet(d.Imports...))
	if err != nil {
		return err
	}
	
	// update static score and index it
	d.updateStaticScore()
	err = doIndex(c, d)
	if err != nil {
		return err
	}
	
	pkgs := diffStringList(savedD.Imports, d.Imports)
	if len(pkgs) > 0 {
		ddb := NewDocDB(c, kindToUpdate)
		errs := ddb.PutMulti(pkgs, make([]struct{}, len(pkgs)))
		if errs.ErrorCount() > 0 {
			c.Errorf("PutMulti(%d packages) to %s with %d failed: %v", 
				len(pkgs), kindToUpdate, errs.ErrorCount(), errs)
		} else {
			c.Infof("%d packages add to %s", len(pkgs), kindToUpdate)
		}
	}
	
	return nil
}

func score(scoreI, scoreGap, scoreJ, wholeLen, maxBytes int) int {
	return scoreI + scoreJ + (wholeLen - maxBytes)
}

func splitToLines(text string) []string {
	lines := strings.Split(text, "\n")
	newLines := make([]string, 0, len(lines))
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if len(line) == 0 {
			continue
		}

		newLines = append(newLines, line)
	}

	return newLines
}

func selectSnippets(text string, tokens villa.StrSet, maxBytes int) string {
	text = strings.TrimSpace(text)
	if len(text) <= maxBytes {
		return text
	}
	// return text[:maxBytes] + "..."

	lines := splitToLines(text)

	var hitTokens villa.StrSet
	type lineinfo struct {
		idx  int
		line string
	}
	var selLines []lineinfo
	count := 0
	for i, line := range lines {
		line = strings.TrimSpace(line)
		lines[i] = line

		lineTokens := appendTokens(nil, line)
		reserve := false
		for token := range tokens {
			if !hitTokens.In(token) && lineTokens.In(token) {
				reserve = true
				hitTokens.Put(token)
			}
		}

		if i == 0 || reserve && (count+len(line)+1 < maxBytes) {
			selLines = append(selLines, lineinfo{
				idx:  i,
				line: line,
			})
			count += len(line) + 1
			if count == maxBytes {
				break
			}

			lines[i] = ""
		}
	}

	if count < maxBytes {
		for i, line := range lines {
			if len(line) == 0 {
				continue
			}

			if count+len(line) >= maxBytes {
				break
			}

			selLines = append(selLines, lineinfo{
				idx:  i,
				line: line,
			})

			count += len(line) + 1
		}

		villa.SortF(len(selLines), func(i, j int) bool {
			return selLines[i].idx < selLines[j].idx
		}, func(i, j int) {
			selLines[i], selLines[j] = selLines[j], selLines[i]
		})
	}

	var outBuf villa.ByteSlice
	for i, line := range selLines {
		if line.idx > 1 && (i < 1 || line.idx != selLines[i-1].idx+1) {
			outBuf.WriteString("...")
		} else {
			if i > 0 {
				outBuf.WriteString(" ")
			}
		}
		outBuf.WriteString(line.line)
	}

	if selLines[len(selLines)-1].idx != len(lines)-1 {
		outBuf.WriteString("...")
	}

	return string(outBuf)
}
