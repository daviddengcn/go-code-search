package gocode

import (
	"appengine"
	"fmt"
	"github.com/daviddengcn/go-code-crawl"
	"github.com/daviddengcn/go-villa"
	"github.com/daviddengcn/go-index"
	"html/template"
	"log"
	"net/http"
	"strings"
	"time"
	godoc "go/doc"
	"strconv"
)

var templates = template.Must(template.ParseGlob(`web/*`))

func init() {
	http.HandleFunc("/search", pageSearch)
	http.HandleFunc("/add", pageAdd)
	http.HandleFunc("/view", pageView)
	http.HandleFunc("/update", pageUpdate)
	
	http.HandleFunc("/crawler", pageCrawler)
	http.HandleFunc("/db", pageDb)

	http.HandleFunc("/index", pageIndex)
	
	gcc.Register(new(CrawlerServer))

	// http//.HandleFunc("/clear", pageClear)

	http.HandleFunc("/", pageRoot)

	http.HandleFunc("/try", pageTry)
}

func pageRoot(w http.ResponseWriter, r *http.Request) {
	err := templates.ExecuteTemplate(w, "index.html", nil)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

type SubProjectInfo struct {
	MarkedName template.HTML
	Package    string
	SubPath    string
	Info       string
}

type ShowDocInfo struct {
	*DocInfo
	Index         int
	Summary       template.HTML
	MarkedName    template.HTML
	MarkedPackage template.HTML
	Subs          []SubProjectInfo
}

type ShowResults struct {
	TotalResults int
	TotalEntries int
	Folded       int
	Docs         []ShowDocInfo
}

func markWord(word []byte) []byte {
	buf := villa.ByteSlice("<b>")
	template.HTMLEscape(&buf, word)
	buf.Write([]byte("</b>"))
	return buf
}

func markText(text string, tokens villa.StrSet,
		markFunc func([]byte) []byte) template.HTML {
	if len(text) == 0 {
		return ""
	}
	
	var outBuf villa.ByteSlice
	
	index.MarkText([]byte(text), CheckRuneType, func(token []byte) bool {
		// needMark
		return tokens.In(normWord(string(token)))
	}, func(text []byte) error {
		// output
		template.HTMLEscape(&outBuf, text)
		return nil
	}, func(token []byte) error {
		outBuf.Write(markFunc(token))
		return nil
	})
	
	return template.HTML(string(outBuf))
}

type Range struct {
	start, count int
}

func (r Range) In(idx int) bool {
	return idx >= r.start && idx < r.start + r.count
}

func showSearchResults(results *SearchResult, tokens villa.StrSet,
	r Range) *ShowResults {
	docs := make([]ShowDocInfo, 0, len(results.Docs))

	projToIdx := make(map[string]int)
	folded := 0

	cnt := 0
mainLoop:
	for _, d := range results.Docs {
		if d.Name == "main" {
			d.Name = "main - " + projectOfPackage(d.Package)
		}

		parts := strings.Split(d.Package, "/")
		if len(parts) > 2 {
			for i := len(parts) - 1; i >= 2; i-- {
				pkg := strings.Join(parts[:i], "/")
				if idx, ok := projToIdx[pkg]; ok {
					markedName := markText(d.Name, tokens, markWord)
					if r.In(idx) {
						docsIdx := idx - r.start
						docs[docsIdx].Subs = append(docs[docsIdx].Subs,
							SubProjectInfo{
								MarkedName: markedName,
								Package:    d.Package,
								SubPath:    "/" + strings.Join(parts[i:], "/"),
								Info:       d.Synopsis,
							})
					}
					folded++
					continue mainLoop
				}
			}
		}
		
//		if len(docs) >= 1000 {
//			continue
//		}

		projToIdx[d.Package] = cnt
		if r.In(cnt) {
			markedName := markText(d.Name, tokens, markWord)
			raw := selectSnippets(d.Description+"\n"+d.ReadmeData, tokens, 300)
	
			if d.StarCount < 0 {
				d.StarCount = 0
			}
			docs = append(docs, ShowDocInfo{
				DocInfo:       d,
				Index:         cnt + 1,
				MarkedName:    markedName,
				Summary:       markText(raw, tokens, markWord),
				MarkedPackage: markText(d.Package, tokens, markWord),
			})
		}
		cnt ++
	}

	return &ShowResults{
		TotalResults: results.TotalResults,
		TotalEntries: cnt,
		Folded:       folded,
		Docs:         docs,
	}
}

const itemsPerPage = 10

func pageSearch(w http.ResponseWriter, r *http.Request) {
	// current page, 1-based
	p, err := strconv.Atoi(r.FormValue("p"))
	if err != nil {
		p = 1
	}
	
	startTime := time.Now()

	c := appengine.NewContext(r)
	q := strings.TrimSpace(r.FormValue("q"))
	results, tokens, err := search(c, q)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	
	showResults := showSearchResults(results, tokens,
						Range{(p - 1)*itemsPerPage, itemsPerPage})
	totalPages := (showResults.TotalEntries + itemsPerPage - 1) / itemsPerPage
	c.Infof("totalPages: %d", totalPages)
	var beforePages, afterPages []int
	for i := 1; i <= totalPages; i ++ {
		if i < p && p - i < 10 {
			beforePages = append(beforePages, i)
		} else if i > p && i - p < 10 {
			afterPages = append(afterPages, i)
		}
	}
	
	prevPage, nextPage := p - 1, p + 1
	if prevPage < 0 || prevPage > totalPages {
		prevPage = 0
	}
	if nextPage < 0 || nextPage > totalPages {
		nextPage = 0
	}
	
	data := struct {
		Q           string
		Results     *ShowResults
		SearchTime  time.Duration
		BeforePages []int
		PrevPage    int
		CurrentPage int
		NextPage    int
		AfterPages  []int
		BottomQ     bool
	}{
		Q:           q,
		Results:     showResults,
		SearchTime:  time.Now().Sub(startTime),
		BeforePages: beforePages,
		PrevPage:    prevPage,
		CurrentPage: p,
		NextPage:    nextPage,
		AfterPages:  afterPages,
		BottomQ:     len(results.Docs) >= 5,
	}
	c.Infof("Search results ready")
	err = templates.ExecuteTemplate(w, "search.html", data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	c.Infof("Search results rendered")
}

func pageAdd(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)
	pkgsStr := r.FormValue("pkg")
	if pkgsStr != "" {
		pkgs := strings.Split(pkgsStr, "\n")
		log.Printf("%d packaged submitted!", len(pkgs))
		for _, pkg := range pkgs {
			pkg = strings.TrimSpace(pkg)
			if appendPackage(c, pkg) {
				//				addCheckTask(c, "package", pkg)
			}
		}
	}
	err := templates.ExecuteTemplate(w, "add.html", nil)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func pageView(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimSpace(r.FormValue("id"))
	if id != "" {
		c := appengine.NewContext(r)
		ddb := NewCachedDocDB(c, "doc")
		var doc DocInfo
		err, exists := ddb.Get(id, &doc)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		if !exists {
			fmt.Fprintf(w, `<html><body>No such entry!`)

			ent, _ := findCrawlingEntry(c, kindCrawlerPackage, id)
			if ent != nil {
				fmt.Fprintf(w, ` Scheduled to be crawled at %s`,
					ent.ScheduleTime.Format("2006-01-02 15:04:05"))
			} else {
				fmt.Fprintf(w, ` Not found yet!`)
			}
			fmt.Fprintf(w, ` Click to <a href="crawl?id=%s">crawl</a>.</body></html>`,
				template.URLQueryEscaper(id))
			return
		}
		
		if doc.StarCount < 0 {
			doc.StarCount = 0
		}
		
		var descHTML villa.ByteSlice
		godoc.ToHTML(&descHTML, doc.Description, nil)
		
		showReadme := len(doc.Description) < 10 && len(doc.ReadmeData) > 0

		err = templates.ExecuteTemplate(w, "view.html", struct {
			DocInfo
			DescHTML template.HTML
			ShowReadme bool
		}{
			DocInfo:    doc,
			DescHTML:   template.HTML(descHTML),
			ShowReadme: showReadme,
		})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}
}

func pageUpdate(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimSpace(r.FormValue("id"))
	if id != "" {
		c := appengine.NewContext(r)
		updateDocInfo(c, id)
		
		http.Redirect(w, r, "view?id="+template.URLQueryEscaper(id), 302)
	}
}

func pageCrawler(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)
	err := templates.ExecuteTemplate(w, "crawler.html", fetchCrawlerInfo(c))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func pageDb(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)
	err := templates.ExecuteTemplate(w, "db.html", statDatabaseInfo(c))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func pageClear(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)
	log.Println("Clearing import:import ...")
	ts := NewTokenSet(c, "import:")
	err := ts.Clear("import")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	log.Println("Clearing index:doc ...")
	ts = NewTokenSet(c, "index:")
	err = ts.Clear("doc")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	fmt.Fprintf(w, "Clear success!")
}

func pageTry(w http.ResponseWriter, r *http.Request) {
	tokens := appendTokens(nil, "justTellsMeWhy goes going lied lie lies chicks efg1234.43 中文字符")
	fmt.Fprintf(w, "Tokens: %v", tokens.Elements())
}

func pageIndex(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)
	cntIndex := indexFetchedDocs(c, 9*time.Minute)
	cntUpdate := processToUpdate(c, 9*time.Minute)
	
	fmt.Fprintf(w, "Index: %d, Update: %d", cntIndex, cntUpdate)
}

type CrawlerServer struct{}

func (cs *CrawlerServer) FetchPackageList(r *http.Request, l int) (pkgs []string) {
	c := appengine.NewContext(r)
	return listCrawlEntries(c, kindCrawlerPackage, l)
}

func (cs *CrawlerServer) FetchPersonList(r *http.Request, l int) (ids []string) {
	c := appengine.NewContext(r)
	return listCrawlEntries(c, kindCrawlerPerson, l)
}

func (cs *CrawlerServer) PushPackage(r *http.Request, p *gcc.Package) {
	c := appengine.NewContext(r)
	pushPackage(c, p)
}

func (cs *CrawlerServer) ReportBadPackage(r *http.Request, pkg string) {
	c := appengine.NewContext(r)
	deletePackage(c, pkg)
}

func (cs *CrawlerServer) PushPerson(r *http.Request, p *gcc.Person) (NewPackage bool) {
	c := appengine.NewContext(r)
	return pushPerson(c, p)
}

func (cs *CrawlerServer) TouchPackage(r *http.Request, pkg string) (earlySchedule bool) {
	c := appengine.NewContext(r)
	return touchPackage(c, pkg)
}

func (cs *CrawlerServer) AppendPackages(r *http.Request, pkgs []string) (newNum int) {
	c := appengine.NewContext(r)
	for _, pkg := range pkgs {
		if appendPackage(c, pkg) {
			newNum++
		}
	}
	return newNum
}

func (cs *CrawlerServer) LastError() error {
	return nil
}
