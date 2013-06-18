package gocode

import (
	//    "fmt"
	"appengine"
//	"github.com/daviddengcn/go-villa"
	"github.com/garyburd/gddo/doc"
	"html/template"
	"log"
	"net"
	"net/http"
	"strings"
	"time"
	"crypto/tls"
)

var templates = template.Must(template.ParseFiles("web/header.html",
	"web/footer.html", "web/index.html", "web/search.html", "web/add.html",
	"web/view.html"))

func init() {
	http.HandleFunc("/_ah/start", startBackend)
	http.HandleFunc("/_ah/stop", stopBackend)
	http.HandleFunc("/search", pageSearch)
	http.HandleFunc("/add", pageAdd)
	http.HandleFunc("/view", pageView)
	http.HandleFunc("/update", pageUpdate)
	http.HandleFunc("/crawl", pageCrawl)

	http.HandleFunc("/", pageRoot)

	doc.SetGithubCredentials("94446b37edb575accd8b", "15f55815f0515a3f6ad057aaffa9ea83dceb220b")
}

//

var (
	dialTimeout    = 5 * time.Second
	requestTimeout = 20 * time.Second
)

func timeoutDial(network, addr string) (net.Conn, error) {
	return net.DialTimeout(network, addr, dialTimeout)
}

type transport struct {
	t http.Transport
}

func (t *transport) RoundTrip(req *http.Request) (*http.Response, error) {
	timer := time.AfterFunc(requestTimeout, func() {
		t.t.CancelRequest(req)
		log.Printf("Canceled request for %s", req.URL)
	})
	defer timer.Stop()
	resp, err := t.t.RoundTrip(req)
	return resp, err
}

var httpTransport = &transport {
	t: http.Transport {
		Dial: timeoutDial,
		ResponseHeaderTimeout: requestTimeout / 2,
		TLSClientConfig: &tls.Config {
			InsecureSkipVerify: true,
		},
	},
}
var httpClient = &http.Client{Transport: httpTransport}

//

func pageRoot(w http.ResponseWriter, r *http.Request) {
	err := templates.ExecuteTemplate(w, "index.html", nil)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

type ShowDocInfo struct {
	DocInfo
	Summary string
	Imported     int
}

type ShowResults struct {
	TotalResults int
	Docs         []ShowDocInfo
}

func showSearchResults(results *SearchResult) *ShowResults {
	docs := make([]ShowDocInfo, len(results.Docs))

	for i, d := range results.Docs {
		summary := d.Description
		if len(summary) > 400 {
			summary = summary[:400]
		}
		docs[i] = ShowDocInfo{
			DocInfo: d,
			Summary: summary,
			Imported: len(d.ImportedPkgs),
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
	results, err := search(c, q)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	data := struct {
		Q       string
		Results *ShowResults
	}{
		Q:       q,
		Results: showSearchResults(results),
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
		err := ddb.Get(id, &doc)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		err = templates.ExecuteTemplate(w, "view.html", struct {
			DocInfo
			Imported int
		}{
			DocInfo:    doc,
			Imported: len(doc.ImportedPkgs),
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
