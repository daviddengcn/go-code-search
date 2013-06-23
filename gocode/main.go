package gocode

import (
	"appengine"
	"fmt"
	"github.com/daviddengcn/go-villa"
	"github.com/daviddengcn/go-code-crawl"
	"html/template"
	"log"
	"net/http"
	"strings"
	"time"
	"encoding/json"
	"strconv"
)

var templates = template.Must(template.ParseFiles("web/header.html",
	"web/footer.html", "web/index.html", "web/search.html", "web/add.html",
	"web/view.html", "web/crawler.html"))

func init() {
	http.HandleFunc("/_ah/start", startBackend)
	http.HandleFunc("/_ah/stop", stopBackend)
	http.HandleFunc("/search", pageSearch)
	http.HandleFunc("/add", pageAdd)
	http.HandleFunc("/view", pageView)
	http.HandleFunc("/update", pageUpdate)
	http.HandleFunc("/crawl", pageCrawl)
	http.HandleFunc("/crawler", pageCrawler)
	http.HandleFunc("/crawlloop", pageCrawlLoop)
	http.HandleFunc("/check", pageCheck)
	
	http.HandleFunc("/crawlentries", pageCrawlEntries)
	http.HandleFunc("/pushpkg", pagePushPkg)
	http.HandleFunc("/pushpsn", pagePushPerson)

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
	DocInfo
	Summary       template.HTML
	MarkedName    template.HTML
	MarkedPackage template.HTML
	Subs          []SubProjectInfo
}

type ShowResults struct {
	TotalResults int
	Folded       int
	Docs         []ShowDocInfo
}

func markWord(word string) template.HTML {
	return "<b>" + template.HTML(template.HTMLEscapeString(word)) + "</b>"
}

func showSearchResults(results *SearchResult, tokens villa.StrSet) *ShowResults {
	docs := make([]ShowDocInfo, 0, len(results.Docs))

	projToIdx := make(map[string]int)
	folded := 0

mainLoop:
	for _, d := range results.Docs {
		if d.Name == "main" {
			d.Name = "main - " + projectOfPackage(d.Package)
		}

		markedName := markText(d.Name, tokens, markWord)

		parts := strings.Split(d.Package, "/")
		if len(parts) > 2 {
			for i := len(parts) - 1; i >= 2; i-- {
				pkg := strings.Join(parts[:i], "/")
				if idx, ok := projToIdx[pkg]; ok {
					docs[idx].Subs = append(docs[idx].Subs, SubProjectInfo{
						MarkedName: markedName,
						Package:    d.Package,
						SubPath:    "/" + strings.Join(parts[i:], "/"),
						Info:       d.Synopsis,
					})
					folded++
					continue mainLoop
				}
			}
		}

		raw := selectSnippets(d.Description+"\n"+d.ReadmeData, tokens, 300)

		projToIdx[d.Package] = len(docs)
		docs = append(docs, ShowDocInfo{
			DocInfo:       d,
			MarkedName:    markedName,
			Summary:       markText(raw, tokens, markWord),
			MarkedPackage: markText(d.Package, tokens, markWord),
		})
	}

	return &ShowResults{
		TotalResults: results.TotalResults,
		Folded:       folded,
		Docs:         docs,
	}
}

func pageSearch(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)
	q := strings.TrimSpace(r.FormValue("q"))
	results, tokens, err := search(c, q)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	data := struct {
		Q       string
		Results *ShowResults
	}{
		Q:       q,
		Results: showSearchResults(results, tokens),
	}
	err = templates.ExecuteTemplate(w, "search.html", data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
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
		ddb := NewDocDB(c, "doc")
		var doc DocInfo
		err, exists := ddb.Get(id, &doc)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		if !exists {
			fmt.Fprintf(w, `<html><body>No such entry!`)

			ent, _ := findCrawlingEntry(c, crawlerPackageKind, id)
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

		err = templates.ExecuteTemplate(w, "view.html", struct {
			DocInfo
		}{
			DocInfo: doc,
		})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}
}

func pageUpdate(w http.ResponseWriter, r *http.Request) {
	id := r.FormValue("id")
	if id != "" {
		c := appengine.NewContext(r)
		updateImported(c, id)
	}

	http.Redirect(w, r, "view?id="+template.URLQueryEscaper(id), 301)
}

func pageCrawl(w http.ResponseWriter, r *http.Request) {
	id := r.FormValue("id")
	if id != "" {
		c := appengine.NewContext(r)
		httpClient := genHttpClient(c)
		
		err := crawlPackage(c, httpClient, id)
		if err != nil {
	        http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}

	http.Redirect(w, r, "view?id="+template.URLQueryEscaper(id), 301)
}

func pageCrawler(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)
	err := templates.ExecuteTemplate(w, "crawler.html", fetchCrawlerInfo(c))
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

func pageCrawlLoop(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)
//	doCrawlingLoop(c, 1 * time.Minute)
	doCrawlingLoop(c, 10 * time.Second)
}

func pageCheck(w http.ResponseWriter, r *http.Request) {
	tp := strings.TrimSpace(r.FormValue("tp"))
	id := strings.TrimSpace(r.FormValue("id"))
	c := appengine.NewContext(r)
	switch tp {
	case "package":
		checkPackage(c, id)
	case "person":
		c := appengine.NewContext(r)
		checkPerson(c, id)
	}
}

func pageCrawlEntries(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)
	l, _ := strconv.Atoi(r.FormValue("l"))
	kind := strings.TrimSpace(r.FormValue("kind"))
	pkgs := listCrawlEntries(c, kind, l)
	pkgsJsonBs, err := json.Marshal(pkgs)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	
	w.Write(pkgsJsonBs)
}

func pagePushPkg(w http.ResponseWriter, r *http.Request) {
	p, err := gcc.ParsePushPackage(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	c := appengine.NewContext(r)
	
	pushPackage(c, p)
}

func pagePushPerson(w http.ResponseWriter, r *http.Request) {
	p, err := gcc.ParsePushPerson(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	
	c := appengine.NewContext(r)
	var reply gcc.PushPersonReply
	reply.NewPackage = pushPerson(c, p)
	
	jsonBytes, err := json.Marshal(reply)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	
	w.Write(jsonBytes)
}
