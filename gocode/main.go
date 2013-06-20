package gocode

import (
	"appengine"
	"html/template"
	"github.com/daviddengcn/go-villa"
	"log"
	"net/http"
	"strings"
	"fmt"
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
	
//	http.HandleFunc("/clear", pageClear)

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

type ShowDocInfo struct {
	DocInfo
	Summary      template.HTML
	MarkedName    template.HTML
	MarkedPackage template.HTML
}

type ShowResults struct {
	TotalResults int
	Docs         []ShowDocInfo
}

func markWord(word string) template.HTML {
	return "<b>" + template.HTML(template.HTMLEscapeString(word)) + "</b>"
}

func showSearchResults(results *SearchResult, tokens villa.StrSet) *ShowResults {
	docs := make([]ShowDocInfo, len(results.Docs))

	for i, d := range results.Docs {
		if d.Name == "main" {
			d.Name = "main - " + projectOfPackage(d.Package)
		}
		
		raw := selectSnippets(d.Description, tokens, 300)

		docs[i] = ShowDocInfo{
			DocInfo:      d,
			MarkedName:    markText(d.Name, tokens, markWord),
			Summary:      markText(raw, tokens, markWord),
			MarkedPackage: markText(d.Package, tokens, markWord),
		}
	}
	return &ShowResults{
		TotalResults: results.TotalResults,
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
			appendPackage(c, pkg)
		}
	}
	err := templates.ExecuteTemplate(w, "add.html", nil)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func pageView(w http.ResponseWriter, r *http.Request) {
	id := r.FormValue("id")
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
			fmt.Fprintf(w, `<html><body>No such entry! Click to <a href="crawl?id=%s">crawl</a>.</body></html>`,
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
		crawlPackage(c, id)
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
	c := appengine.NewContext(r)
	fmt.Fprintf(w, "<html><body><pre>")
	tryCrawlPackage(c, w, "github.com/FlyingCar/haunts/game")
	fmt.Fprintf(w, "</pre>")
}