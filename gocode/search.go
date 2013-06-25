package gocode

import (
	"appengine"
	"bytes"
	"github.com/agonopol/go-stem/stemmer"
	"github.com/daviddengcn/gddo/doc"
	"github.com/daviddengcn/go-villa"
	"github.com/daviddengcn/go-code-crawl"
	"html/template"
	"log"
	"math"
	"sort"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

type SearchResult struct {
	TotalResults int
	Docs         []DocInfo
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

func projectOfPackage(pkg string) string {
	parts := strings.Split(pkg, "/")
	if len(parts) == 0 {
		return ""
	}

	switch parts[0] {
	case "llamaslayers.net", "bazil.org":
		if len(parts) > 1 {
			return parts[1]
		}
	case "github.com", "code.google.com", "bitbucket.org", "labix.org":
		if len(parts) > 2 {
			return parts[2]
		}
	case "golanger.com":
		return "golangers"

	case "launchpad.net":
		if len(parts) > 2 && strings.HasPrefix(parts[1], "~") {
			return parts[2]
		}
		if len(parts) > 1 {
			return parts[1]
		}
	case "cgl.tideland.biz":
		return "tcgl"
	}
	return pkg
}

type DocInfo struct {
	Name         string
	Package      string
	Author       string
	LastUpdated  time.Time
	StarCount    int
	Synopsis     string   `datastore:",noindex"`
	Description  string   `datastore:",noindex"`
	ImportedPkgs []string `datastore:",noindex"`
	StaticScore  float64  `datastore:",noindex"`
	Imports      []string `datastore:",noindex"`
	ProjectURL   string   `datastore:",noindex"`
	ReadmeFn     string   `datastore:",noindex"`
	ReadmeData   string   `datastore:",noindex"`

	MatchScore float64 `datastore:"-"`
	Score      float64 `datastore:"-"`
}

func (doc *DocInfo) updateStaticScore() {
	s := float64(1)

	author := doc.Author
	if author == "" {
		author = authorOfPackage(doc.Package)
	}

	project := projectOfPackage(doc.Package)

	authorCount := make(map[string]int)
	projectCount := make(map[string]int)
	for _, imp := range doc.ImportedPkgs {
		impProject := projectOfPackage(imp)
		projectCount[impProject] = projectCount[impProject] + 1

		impAuthor := authorOfPackage(imp)
		if impAuthor != "" {
			authorCount[impAuthor] = authorCount[impAuthor] + 1
		}
	}

	for _, imp := range doc.ImportedPkgs {
		vl := float64(1.)

		impProject := projectOfPackage(imp)
		vl /= float64(projectCount[impProject])

		if impProject == project {
			vl *= 0.1
		}

		impAuthor := authorOfPackage(imp)
		if impAuthor != "" {
			cnt := authorCount[impAuthor]
			total := cnt
			if total > 2 {
				total = 2
			}
			if impAuthor == author {
				vl *= 0.5 * float64(total) / float64(cnt)
			} else {
				vl *= float64(total) / float64(cnt)
			}
		}

		s += vl
	}

	desc := strings.TrimSpace(doc.Description)
	if len(desc) > 0 {
		s += 1
		if len(desc) > 100 {
			s += 0.5
		}

		if strings.HasPrefix(desc, "Package "+doc.Name) {
			s += 0.5
		} else if strings.HasPrefix(desc, "package "+doc.Name) {
			s += 0.4
		}
	}

	starCount := doc.StarCount - 3
	if starCount < 0 {
		starCount = 0
	}
	s += math.Sqrt(float64(starCount)) * 0.5

	doc.StaticScore = s
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

func CheckRuneType(last, current rune) RuneType {
	if isTermSep(current) {
		return TokenSep
	}

	if current > 128 {
		return TokenStart
	}

	if unicode.IsLetter(current) {
		if unicode.IsLetter(last) {
			return TokenBody
		}
		return TokenStart
	}

	if unicode.IsNumber(current) {
		if unicode.IsNumber(last) {
			return TokenBody
		}
		return TokenStart
	}

	return TokenStart
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

func CheckCamel(last, current rune) RuneType {
	if unicode.IsUpper(current) {
		return TokenStart
	}

	return TokenBody
}

func appendTokens(tokens villa.StrSet, text string) villa.StrSet {
	/*
		for _, token := range strings.FieldsFunc(text, isTermSep) {
			token = normWord(token)
			tokens.Put(token)
		}
	*/
	lastToken := ""
	Tokenize(CheckRuneType, bytes.NewReader([]byte(text)), func(token string) {
		if isCamel(token) {
			last := ""
			Tokenize(CheckCamel, bytes.NewReader([]byte(token)), func(token string) {
				token = normWord(token)
				tokens.Put(token)

				if last != "" {
					tokens.Put(last + "-" + token)
				}

				last = token
			})
		}
		token = normWord(token)
		tokens.Put(token)

		if token[0] > 128 && len(lastToken) > 0 && lastToken[0] > 128 {
			tokens.Put(lastToken + token)
		}

		lastToken = token
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

func calcMatchScore(doc *DocInfo, tokens villa.StrSet) float64 {
	if len(tokens) == 0 {
		return 1.
	}

	s := float64(0.02 * float64(len(tokens)))

	synopsis := strings.ToLower(doc.Synopsis)
	name := strings.ToLower(doc.Name)
	pkg := strings.ToLower(doc.Package)

	for token := range tokens {
		if strings.Index(synopsis, token) >= 0 {
			s += 0.25
		}

		if strings.Index(name, token) >= 0 {
			s += 0.4
		}

		if strings.Index(pkg, token) >= 0 {
			s += 0.1
		}
	}

	return s
}

func search(c appengine.Context, q string) (*SearchResult, villa.StrSet, error) {
	ts := NewTokenSet(c, "index:")

	tokens := appendTokens(nil, q)

	ids, err := ts.Search("doc", tokens)
	if err != nil {
		return nil, nil, err
	}
	log.Printf("  %d ids got for query %s", len(ids), q)

	ddb := NewCachedDocDB(c, "doc")

	docs := make([]DocInfo, len(ids))
	for i, id := range ids {
		err, exists := ddb.Get(id, &docs[i])
		if err != nil {
			log.Printf("  ddb.Get(%s,) failed: %v", id, err)
		}

		if exists {
			if docs[i].StaticScore < 1 {
				docs[i].updateStaticScore()
			}

			docs[i].MatchScore = calcMatchScore(&docs[i], tokens)
			docs[i].Score = (docs[i].StaticScore - 0.9) * docs[i].MatchScore
		}
	}

	villa.SortF(len(docs), func(i, j int) bool {
		// true if doc i is before doc j
		ssi, ssj := docs[i].Score, docs[j].Score
		if ssi > ssj {
			return true
		}
		if ssi < ssj {
			return false
		}

		sci, scj := docs[i].StarCount, docs[j].StarCount
		if sci > scj {
			return true
		}
		if sci < scj {
			return false
		}

		pi, pj := docs[i].Package, docs[j].Package
		if len(pi) < len(pj) {
			return true
		}
		if len(pi) > len(pj) {
			return false
		}

		return pi < pj
	}, func(i, j int) {
		// Swap
		docs[i], docs[j] = docs[j], docs[i]
	})

	return &SearchResult{
		TotalResults: len(ids),
		Docs:         docs,
	}, tokens, nil
}

func index(c appengine.Context, doc DocInfo) error {
	ts := NewTokenSet(c, "index:")
	var tokens villa.StrSet
	tokens = appendTokens(tokens, doc.Name)
	tokens = appendTokens(tokens, doc.Package)
	tokens = appendTokens(tokens, doc.Description)
	tokens = appendTokens(tokens, doc.ReadmeData)
	tokens = appendTokens(tokens, doc.Author)

	id := doc.Package

	log.Printf("  indexing %s, %v", id, tokens)
	err := ts.Index("doc", id, tokens)
	if err != nil {
		return err
	}

	ddb := NewCachedDocDB(c, "doc")
	err = ddb.Put(id, &doc)
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

func updateDocument(c appengine.Context, pdoc *gcc.Package) {
	var d DocInfo

	ddb := NewCachedDocDB(c, "doc")

	// Set initial values. Error is ignored
	ddb.Get(pdoc.ImportPath, &d)

	d.Name = pdoc.Name
	d.Package = pdoc.ImportPath
	d.Synopsis = pdoc.Synopsis
	d.Description = pdoc.Doc
	d.LastUpdated = time.Now()
	d.Author = authorOfPackage(pdoc.ImportPath)
	d.ProjectURL = pdoc.ProjectURL
	if pdoc.StarCount >= 0 {
		// if pdoc.StarCount < 0, it is not correctly fetched, remain old value
		d.StarCount = pdoc.StarCount
	}

	d.ReadmeFn, d.ReadmeData = pdoc.ReadmeFn, pdoc.ReadmeData
	
//	for fn, data := range pdoc.ReadmeFiles {
//		d.ReadmeFn, d.ReadmeData = fn, string(data)
//	}
	//log.Printf("Readme of %s: %v", d.Package, pdoc.ReadmeFiles)

	//log.Printf("[updateDocument] pdoc.References: %v", pdoc.References)

	d.Imports = nil
	for _, imp := range pdoc.Imports {
		if doc.IsValidRemotePath(imp) {
			d.Imports = append(d.Imports, imp)
		}
	}

	ts := NewTokenSet(c, "import:")
	importedPkgs, err := ts.Search("import", villa.NewStrSet(d.Package))
	if err == nil {
		d.ImportedPkgs = importedPkgs
	}

	d.updateStaticScore()

	err = index(c, d)
	if err != nil {
		log.Printf("Indexing %s failed: %v", d.Package, err)
	}
	c.Infof("Indexing %s success", d.Package)

	// index importing links
	ts = NewTokenSet(c, "import:")
	imports := villa.NewStrSet(d.Imports...)
	id := d.Package
	c.Infof("  indexing imports of %s: %d", id, len(imports))
	err = ts.Index("import", id, imports)
	if err != nil {
		log.Printf("Indexing imports of %s failed: %v", d.Package, err)
	}
/*	
	// update imported packages
	for _, imp := range d.Imports {
		updateImported(c, imp)
	}
*/

	if strings.HasPrefix(d.Package, "github.com/") {
		appendPerson(c, "github.com", d.Author)
	} else if strings.HasPrefix(d.Package, "bitbucket.org/") {
		appendPerson(c, "bitbucket.org", d.Author)
	}
}

func markText(text string, tokens villa.StrSet, markFunc func(word string) template.HTML) template.HTML {
	var outBuf bytes.Buffer
	inBuf := []byte(text)
	if len(inBuf) == 0 {
		return ""
	}
	if len(tokens) == 0 {
		// no tokens, simply convert text to HTML
		template.HTMLEscape(&outBuf, inBuf)
		return template.HTML(outBuf.String())
	}
	p := 0
	r, sz := utf8.DecodeRune(inBuf)
	for len(inBuf) > 0 {
		// seperator
		for isTermSep(r) {
			p += sz
			if p < len(inBuf) {
				r, sz = utf8.DecodeRune(inBuf[p:])
			} else {
				break
			}
		}
		if p > 0 {
			template.HTMLEscape(&outBuf, inBuf[:p])
			inBuf, p = inBuf[p:], 0
			if len(inBuf) == 0 {
				break
			}
		}

		// word
		for !isTermSep(r) {
			p += sz
			if p < len(inBuf) {
				r, sz = utf8.DecodeRune(inBuf[p:])
			} else {
				break
			}
		}
		wordBuf := inBuf[:p]
		word := string(wordBuf)
		if tokens.In(normWord(word)) {
			outBuf.WriteString(string(markFunc(word)))
		} else {
			outBuf.Write(wordBuf)
		}
		inBuf, p = inBuf[p:], 0
	}

	return template.HTML(outBuf.String())
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

	var outBuf bytes.Buffer
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

	return outBuf.String()
}
