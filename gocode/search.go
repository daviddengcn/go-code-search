package gocode

import (
	"appengine"
	"github.com/daviddengcn/go-villa"
	"log"
	"sort"
	"strings"
	"time"
	"unicode"
	"math"
	//"text/scanner"
	//"bytes"
	"github.com/daviddengcn/gddo/doc"
	"unicode/utf8"
	"html/template"
	"bytes"
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
	Description  string   `datastore:",noindex"`
	Readme       string   `datastore:",noindex"`
	ImportedPkgs []string `datastore:",noindex"`
	StaticScore  float64  `datastore:",noindex"`
	Imports      []string `datastore:",noindex"`
	ProjectURL   string   `datastore:",noindex"`
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
			s += 1
		} else if strings.HasPrefix(desc, "package "+doc.Name) {
			s += 0.5
		}
	}
	
	starCount := doc.StarCount - 3
	if starCount < 0 {
		starCount = 0
	}
	s += math.Sqrt(float64(starCount))*0.5
	
	doc.StaticScore = s
}

func (doc *DocInfo) loadFromDB(c appengine.Context, id string) error {
	ddb := NewDocDB(c, "doc")
	return ddb.Get(id, doc)
}

func (doc *DocInfo) saveToDB(c appengine.Context) error {
	ddb := NewDocDB(c, "doc")
	return ddb.Put(doc.Package, doc)
}

func isTermSep(r rune) bool {
	return unicode.IsSpace(r) || unicode.IsPunct(r) || unicode.IsSymbol(r)
}

func normWord(word string) string {
	return strings.ToLower(word)
}

func appendTokens(text string, tokens villa.StrSet) villa.StrSet {
	for _, token := range strings.FieldsFunc(text, isTermSep) {
		token = normWord(token)
		tokens.Put(token)
	}
	/*
		var s scanner.Scanner
		s.Init(bytes.NewReader([]byte(text)))
		for token := s.Scan(); token != scanner.EOF; token = s.Scan() {
			tokens.Put(s.TokenText())
		}
	*/

	return tokens
}

func search(c appengine.Context, q string) (*SearchResult, villa.StrSet, error) {
	ts := NewTokenSet(c, "index:")

	tokens := appendTokens(q, nil)

	ids, err := ts.Search("doc", tokens)
	if err != nil {
		return nil, nil, err
	}
	log.Printf("  %d ids got for query %s", len(ids), q)

	ddb := NewDocDB(c, "doc")

	docs := make([]DocInfo, len(ids))
	for i, id := range ids {
		err := ddb.Get(id, &docs[i])
		if err != nil {
			log.Printf("  ddb.Get(%s,) failed: %v", id, err)
		}

		if docs[i].StaticScore < 1 {
			docs[i].updateStaticScore()
		}
	}

	villa.SortF(len(docs), func(i, j int) bool {
		// Less
		ssi, ssj := docs[i].StaticScore, docs[j].StaticScore
		if ssi > ssj {
			return true
		}
		if ssi < ssj {
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
	tokens = appendTokens(doc.Name, tokens)
	tokens = appendTokens(doc.Package, tokens)
	tokens = appendTokens(doc.Description, tokens)
	tokens = appendTokens(doc.Readme, tokens)
	tokens = appendTokens(doc.Author, tokens)

	id := doc.Package

	log.Printf("  indexing %s, %v", id, tokens)
	err := ts.Index("doc", id, tokens)
	if err != nil {
		return err
	}

	ddb := NewDocDB(c, "doc")
	err = ddb.Put(id, &doc)
	if err != nil {
		return err
	}

	return nil
}

func updateImported(c appengine.Context, pkg string) {
	log.Printf("  updateImported of %s ...", pkg)
	var doc DocInfo
	err := doc.loadFromDB(c, pkg)
	if err != nil {
		log.Printf("  [updateImported] ddb.Get(%s) failed: %v", pkg, err)
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

func updateDocument(c appengine.Context, pdoc *doc.Package) {
	var d DocInfo
	
	ddb := NewDocDB(c, "doc")

	// Set initial values
	ddb.Get(pdoc.ImportPath, &d)
	
	
	d.Name = pdoc.Name
	d.Package = pdoc.ImportPath
	d.Description = pdoc.Doc
	d.LastUpdated = time.Now()
	d.Author = authorOfPackage(pdoc.ImportPath)
	d.ProjectURL = pdoc.ProjectURL
	if pdoc.StarCount >= 0 {
		d.StarCount = pdoc.StarCount
	}

	//log.Printf("[updateDocument] pdoc.References: %v", pdoc.References)

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

	// index importing links
	ts = NewTokenSet(c, "import:")
	imports := villa.NewStrSet(d.Imports...)
	id := d.Package
	log.Printf("  indexing imports of %s: %d", id, len(imports))
	err = ts.Index("import", id, imports)
	if err != nil {
		log.Printf("Indexing imports of %s failed: %v", d.Package, err)
	}
	// update imported packages
	for _, imp := range d.Imports {
		updateImported(c, imp)
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

func selectSnippets(text string, tokens villa.StrSet, maxBytes int) string {
	
	if len(text) <= maxBytes {
		return text
	}
	return text[:300] + "..."
	
	inBuf := []byte(text)
	
	wordStarts, wordEnds := []int{}, []int{}
	p, pIn := 0, 0
	r, sz := utf8.DecodeRune(inBuf)
	for pIn < len(inBuf) {
		// seperator
		for isTermSep(r) {
			p += sz
			if pIn+p < len(inBuf) {
				r, sz = utf8.DecodeRune(inBuf[pIn+p:])
			} else {
				break
			}
		}
		if p > 0 {
			pIn, p = pIn+p, 0
			if pIn == len(inBuf) {
				break
			}
		}
		
		// word
		for !isTermSep(r) {
			p += sz
			if pIn+p < len(inBuf) {
				r, sz = utf8.DecodeRune(inBuf[pIn+p:])
			} else {
				break
			}
		}
		wordStarts = append(wordStarts, pIn)
		wordEnds = append(wordEnds, pIn+p)
		
		pIn, p = pIn+p, 0
	}
	
	log.Printf("starts: %v, ends: %v", wordStarts, wordEnds)
	
	tokenList := tokens.Elements()
	tokenIndexes := make(map[string]int)
	for i, token := range tokenList {
		// 0 for not a token
		tokenIndexes[token] = i + 1
	}
	
	tokenCountI := make([]int, len(tokenList))
	tokenCountJ := make([]int, len(tokenList))
	
	bestS := -1
	var bestI, bestJ int
	
	scoreI := 0
	for i := range wordEnds {
		wholeLen := wordEnds[i]
		if wholeLen > maxBytes {
			break
		}
		
		idx1 := tokenIndexes[tokenList[i]]
		if idx1 > 0 {
			idx := idx1 - 1
			if tokenCountI[idx] == 0 {
				scoreI += 100
			} else {
				scoreI += 10
			}
			tokenCountI[idx] ++
		}
		
		s := score(scoreI, 50, 0, wholeLen, maxBytes)
		if s > bestS {
			bestS = s
			bestI, bestJ = i, -1
		}
		
		copy(tokenCountJ, tokenCountI)
		scoreJ := 0
		leftLen := maxBytes - wholeLen
		var k int
		for k = i + 1; k < len(wordEnds); k ++ {
			if wordEnds[k] - wordStarts[i + 1] > leftLen {
				break
			}
		}
		for j := i + 1; j < len(wordEnds); j ++ {
			//s := score(scoreI, 0, scoreJ
		}
		
		_ = scoreJ
	}
	
	_, _ = bestI, bestJ
	
	return text
}
