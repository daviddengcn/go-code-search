package gocode

import (
	"appengine"
	"appengine/datastore"
	"crypto/tls"
	"github.com/daviddengcn/gddo/doc"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"fmt"
)

func init() {
	doc.SetGithubCredentials("94446b37edb575accd8b",
		"15f55815f0515a3f6ad057aaffa9ea83dceb220b")
	doc.SetUserAgent("Go-Code-Search-Agent")
}

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

var httpTransport = &transport{
	t: http.Transport{
//		Dial: timeoutDial,
		ResponseHeaderTimeout: requestTimeout / 2,
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
	},
}
var httpClient = &http.Client{Transport: httpTransport}

type CrawlingEntry struct {
	ScheduleTime time.Time
	Host         string
}

func urlOfPackage(pkg string) *url.URL {
	u, _ := url.Parse("http://" + pkg)
	return u
}

func scheduledPackage(c appengine.Context, pkg string, sTime time.Time) {
	var ent CrawlingEntry

	key := datastore.NewKey(c, "crawler", pkg, 0, nil)

	// fetch old value, if any
	err := datastore.Get(c, key, &ent)
	if !DocGetOk(err) {
		if err != datastore.ErrNoSuchEntity {
			log.Printf("[scheduledPackage] Get(crawler, %s) failed: %v", pkg, err)
		}
	}

	ent.ScheduleTime = sTime

	u := urlOfPackage(pkg)
	if u != nil {
		ent.Host = u.Host
	}

	_, err = datastore.Put(c, key, &ent)
	if err != nil {
		log.Printf("datastore.Put failed: %v", err)
		return
	}

	log.Printf("Schedule package %s to %v", pkg, sTime)
}

const (
	DefaultPackageAge = 72 * time.Hour
	DefaultPersonAge  = 72 * time.Hour
)

func appendPackage(c appengine.Context, pkg string) {
	if !doc.IsValidRemotePath(pkg) {
		// log.Printf("  [appendPackage] Not a valid remote path: %s", pkg)
		return
	}
	ddb := NewDocDB(c, "crawler")

	var ent CrawlingEntry
	err, exists := ddb.Get(pkg, &ent)
	if exists {
		// already scheduled
		log.Printf("  [appendPackage] Package %s was scheduled to %v", pkg, ent.ScheduleTime)
		return
	}

	if err != nil {
		log.Printf("  [appendPackage] Get(crawler, %s) failed: %v", pkg, err)
		return
	}

	scheduledPackage(c, pkg, time.Now())
}

func idOfPerson(site, username string) string {
	return fmt.Sprintf("%s:%s", site, username)
}

func parsePersonId(id string) (site, username string) {
	parts := strings.Split(id, ":")
	return parts[0], parts[1]
}

func scheduledPerson(c appengine.Context, site, username string, sTime time.Time) {
	var ent CrawlingEntry

	id := idOfPerson(site, username)

	key := datastore.NewKey(c, "crawler-person", id, 0, nil)

	// fetch old value, if any
	err := datastore.Get(c, key, &ent)
	if !DocGetOk(err) {
		log.Printf("  [scheduledPerson] crawler-person.Get(%s) failed: %v", id,
			err)
	}

	ent.ScheduleTime = sTime
	ent.Host = site

	_, err = datastore.Put(c, key, &ent)
	if err != nil {
		log.Printf("  [scheduledPerson] crawler-person.Put(%s) failed: %v", id,
			err)
		return
	}

	log.Printf("Schedule person %s to %v", id, sTime)
}

func appendPerson(c appengine.Context, site, username string) {
	ddb := NewDocDB(c, "crawler-person")

	id := idOfPerson(site, username)

	var ent CrawlingEntry
	err, exists := ddb.Get(id, &ent)
	if exists {
		// already scheduled
		log.Printf("  [appendPerson] Person %s was scheduled to %v", id, ent.ScheduleTime)
		return
	}

	if err != nil {
		log.Printf("  [appendPerson] crawler-person.Get(%s) failed: %v", id, err)
		return
	}

	scheduledPerson(c, site, username, time.Now())
}

func groupToFetch(c appengine.Context) (groups map[string][]string) {
	pkgCount := 0
	oldest := time.Now()
	var oldestPkg string

	groups = make(map[string][]string)
	q := datastore.NewQuery("crawler").Filter("ScheduleTime<", time.Now()).Order("ScheduleTime")

	t := q.Run(c)
	for {
		var ent CrawlingEntry
		key, err := t.Next(&ent)
		if err == datastore.Done {
			break
		}

		if err != nil {
			if !DocGetOk(err) {
				log.Printf("[groupToFetch] t.Next failed: %#v", err)
				break
			}
		}

		pkg := key.StringID()
		host := ent.Host
		if host == "" {
			u := urlOfPackage(pkg)

			if u != nil {
				host = u.Host
			}
		}

		groups[host] = append(groups[host], pkg)

		if ent.ScheduleTime.Before(oldest) {
			oldest = ent.ScheduleTime
			oldestPkg = pkg
		}
		pkgCount++
	}

	log.Printf("[groupToFetch] got %d groups, %d packages, oldest %v(%v): %v", len(groups), pkgCount, oldest, time.Now().Sub(oldest), oldestPkg)

	return groups
}

/* Crawl a package. */
func crawlPackage(c appengine.Context, pkg string) {
	pdoc, err := doc.Get(httpClient, pkg, "")
	if err != nil {
		log.Printf("[crawlPackage] doc.Get(%s) failed: %v", pkg, err)
	} else {
		log.Printf("[crawlPackage] doc.Get(%s) sucess", pkg)

		updateDocument(c, pdoc)

		for _, imp := range pdoc.Imports {
			appendPackage(c, imp)
		}
		log.Printf("[crawlPackage] References: %v", pdoc.References)
		for _, ref := range pdoc.References {
			appendPackage(c, ref)
		}
	}
	scheduledPackage(c, pkg, time.Now().Add(DefaultPackageAge).Add(
		time.Duration(rand.Int63n(int64(DefaultPackageAge)/10)-
			int64(DefaultPackageAge)/5)))
}

// debug function //
func tryCrawlPackage(c appengine.Context, w http.ResponseWriter, pkg string) {
	pdoc, err := doc.Get(httpClient, pkg, "")
	if err != nil {
		fmt.Fprintf(w, "[crawlPackage] doc.Get(%s) failed: %v<br>", pkg, err)
	} else {
		fmt.Fprintf(w, "[crawlPackage] doc.Get(%s) sucess<br>", pkg)

		fmt.Fprintf(w, "pdoc.Imports: [%d]%v<br>", len(pdoc.Imports), pdoc.Imports)
		fmt.Fprintf(w, "References: %v<br>", pdoc.References)
	}
}

type HostInfo struct {
	Host      string
	Total     int
	NeedCrawl int
}

type CrawlerInfo struct {
	Total     int
	NeedCrawl int

	Hosts []HostInfo
}

func fetchCrawlerInfo(c appengine.Context) (info CrawlerInfo) {
	now := time.Now()
	var err error
	q := datastore.NewQuery("crawler")
	info.Total, err = q.Count(c)
	if err != nil {
		log.Printf("  crawler.Count() failed: %v", err)
		info.Total = -1
	}

	q = datastore.NewQuery("crawler").Project("Host").Distinct()
	var ents []CrawlingEntry
	_, err = q.GetAll(c, &ents)
	if err != nil {
		log.Printf("  crawler.Host.GetAll() failed: %v", err)
	} else {
		info.Hosts = make([]HostInfo, len(ents))
		for i, ent := range ents {
			info.Hosts[i].Host = ent.Host
			q = datastore.NewQuery("crawler").Filter("Host=", ent.Host)
			info.Hosts[i].Total, _ = q.Count(c)
			q = datastore.NewQuery("crawler").Filter("Host=", ent.Host).Filter("ScheduleTime<", now)
			info.Hosts[i].NeedCrawl, _ = q.Count(c)
		}
	}

	q = datastore.NewQuery("crawler").Filter("ScheduleTime<", now)
	info.NeedCrawl, err = q.Count(c)
	if err != nil {
		log.Printf("  crawler.ScheduleTIme<time.Now().Count() failed: %v", err)
		info.NeedCrawl = -1
	}

	return
}

/* Crawl a person's projects. */
func crawlPerson(c appengine.Context, id string) {
	site, username := parsePersonId(id)
	//log.Printf("Crawling person %s/%s", site, username)
	switch site {
	case "github.com":
		p, err := doc.GetGithubPerson(httpClient, map[string]string{"owner": username})
		if err != nil {
			log.Printf("[crawlPerson] doc.GetGithubPerson(%s) failed: %v", username, err)
		} else {
			log.Printf("[crawlPerson] doc.GetGithubPerson(%s) sucess: %d projects", username, len(p.Projects))
	
			for _, proj := range p.Projects {
				appendPackage(c, proj)
			}
		}
	case "bitbucket.org":
		p, err := doc.GetBitbucketPerson(httpClient, map[string]string{"owner": username})
		if err != nil {
			log.Printf("[crawlPerson] doc.GetBitbucketPerson(%s) failed: %v", username, err)
		} else {
			log.Printf("[crawlPerson] doc.GetBitbucketPerson(%s) sucess: %d projects", username, len(p.Projects))
	
			for _, proj := range p.Projects {
				appendPackage(c, proj)
			}
		}
	}
	scheduledPerson(c, site, username, time.Now().Add(DefaultPersonAge).Add(
		time.Duration(rand.Int63n(int64(DefaultPersonAge)/10)-
			int64(DefaultPersonAge)/5)))
}

func groupToFetchPerson(c appengine.Context) (groups map[string][]string) {
	pkgCount := 0
	oldest := time.Now()
	var oldestPkg string

	groups = make(map[string][]string)
	q := datastore.NewQuery("crawler-person").Filter("ScheduleTime<", time.Now()).Order("ScheduleTime")

	t := q.Run(c)
	for {
		var ent CrawlingEntry
		key, err := t.Next(&ent)
		if err == datastore.Done {
			break
		}

		if err != nil {
			if !DocGetOk(err) {
				log.Printf("[groupToFetchPerson] t.Next failed: %#v", err)
				break
			}
		}

		pkg := key.StringID()
		host := ent.Host
		if host == "" {
			u := urlOfPackage(pkg)

			if u != nil {
				host = u.Host
			}
		}

		groups[host] = append(groups[host], pkg)

		if ent.ScheduleTime.Before(oldest) {
			oldest = ent.ScheduleTime
			oldestPkg = pkg
		}
		pkgCount++
	}

	log.Printf("[groupToFetchPerson] got %d groups, %d persons, oldest %v(%v): %v",
		len(groups), pkgCount, oldest, time.Now().Sub(oldest), oldestPkg)

	return groups
}

func findCrawlPackage(c appengine.Context, pkg string) (*CrawlingEntry, error) {
	ddb := NewDocDB(c, "crawler")
	
	var ent CrawlingEntry
	err, exists := ddb.Get(pkg, &ent)
	if exists {
		return &ent, nil
	}

	if err != nil {
		return nil, err
	}
	
	return nil, nil
}
