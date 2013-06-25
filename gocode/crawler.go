package gocode

import (
	"appengine"
	"appengine/datastore"
	"appengine/urlfetch"
	"github.com/daviddengcn/gddo/doc"
	"github.com/daviddengcn/go-code-crawl"
	"log"
	"math/rand"
	//	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
	
//	"crypto/tls"

	"fmt"
)

func init() {
	doc.SetGithubCredentials("94446b37edb575accd8b",
		"15f55815f0515a3f6ad057aaffa9ea83dceb220b")
	doc.SetUserAgent("Go-Code-Search-Engine")
}

const crawlerTimeout = 20 * time.Second

const (
	DefaultPackageAge = 10 * 24 * time.Hour
	DefaultPersonAge  = 10 * 24 * time.Hour
)

func genHttpClient(c appengine.Context) *http.Client {
	return &http.Client{
		Transport: &urlfetch.Transport{
			Context:                       c,
			Deadline:                      crawlerTimeout,
			AllowInvalidServerCertificate: true,
		},
	}
	/*
	return &http.Client {
		Transport: &http.Transport {
			TLSClientConfig: &tls.Config {
				InsecureSkipVerify: true,
			},
		},
	}
	*/
	/* return urlfetch.Client(c)*/
}

const (
	crawlerPackageKind = "crawler"
	crawlerPersonKind  = "crawler-person"
)

type CrawlingEntry struct {
	ScheduleTime time.Time
	Host         string
}

func urlOfPackage(pkg string) *url.URL {
	u, _ := url.Parse("http://" + pkg)
	return u
}

func schedulePackage(c appengine.Context, pkg string, sTime time.Time) error {
	var ent CrawlingEntry

	key := datastore.NewKey(c, crawlerPackageKind, pkg, 0, nil)

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
		return err
	}

	log.Printf("Schedule package %s to %v", pkg, sTime)
	return nil
}

// returns true if a new package is appended to the crawling list
func appendPackage(c appengine.Context, pkg string) bool {
	if !doc.IsValidRemotePath(pkg) {
		// log.Printf("  [appendPackage] Not a valid remote path: %s", pkg)
		return false
	}
	ddb := NewDocDB(c, crawlerPackageKind)

	var ent CrawlingEntry
	err, exists := ddb.Get(pkg, &ent)
	if exists {
		// already scheduled
		log.Printf("  [appendPackage] Package %s was scheduled to %v", pkg, ent.ScheduleTime)
		return false
	}

	if err != nil {
		log.Printf("  [appendPackage] Get(crawler, %s) failed: %v", pkg, err)
		return false
	}

	return schedulePackage(c, pkg, time.Now()) == nil
}

func idOfPerson(site, username string) string {
	return fmt.Sprintf("%s:%s", site, username)
}

func parsePersonId(id string) (site, username string) {
	parts := strings.Split(id, ":")
	return parts[0], parts[1]
}

func schedulePerson(c appengine.Context, site, username string, sTime time.Time) error {
	var ent CrawlingEntry

	id := idOfPerson(site, username)

	key := datastore.NewKey(c, crawlerPersonKind, id, 0, nil)

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
		return err
	}

	log.Printf("Schedule person %s to %v", id, sTime)
	return nil
}

func appendPerson(c appengine.Context, site, username string) bool {
	ddb := NewDocDB(c, crawlerPersonKind)

	id := idOfPerson(site, username)

	var ent CrawlingEntry
	err, exists := ddb.Get(id, &ent)
	if exists {
		// already scheduled
		log.Printf("  [appendPerson] Person %s was scheduled to %v", id, ent.ScheduleTime)
		return false
	}

	if err != nil {
		log.Printf("  [appendPerson] crawler-person.Get(%s) failed: %v", id, err)
		return false
	}

	return schedulePerson(c, site, username, time.Now()) == nil
}

func groupToFetch(c appengine.Context) (groups map[string][]string) {
	pkgCount := 0
	oldest := time.Now()
	var oldestPkg string

	groups = make(map[string][]string)
	q := datastore.NewQuery(crawlerPackageKind).Filter("ScheduleTime<", time.Now()).Order("ScheduleTime")

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

	c.Infof("[groupToFetch] got %d groups, %d packages, oldest %v(%v): %v", len(groups), pkgCount, oldest, time.Now().Sub(oldest), oldestPkg)

	return groups
}

/* Crawl a package. */
func crawlPackage(c appengine.Context, httpClient *http.Client, pkg string) error {
	pdoc, err := gcc.CrawlPackage(httpClient, pkg)
	//pdoc, err := doc.Get(httpClient, pkg, "")
	if err != nil {
		c.Errorf("[crawlPackage] doc.Get() failed: %v", err)
	} else {
		c.Infof("[crawlPackage] doc.Get(%s) sucess", pkg)

		updateDocument(c, pdoc)

		for _, imp := range pdoc.Imports {
			appendPackage(c, imp)
		}
		log.Printf("[crawlPackage] References: %v", pdoc.References)
		for _, ref := range pdoc.References {
			appendPackage(c, ref)
		}
	}
	schedulePackage(c, pkg, time.Now().Add(DefaultPackageAge).Add(
		time.Duration(rand.Int63n(int64(DefaultPackageAge)/10)-
			int64(DefaultPackageAge)/5)))
			
	return err
}

func pushPackage(c appengine.Context, doc *gcc.Package) { 
	updateDocument(c, doc)
	
	for _, imp := range doc.Imports {
		appendPackage(c, imp)
	}
	log.Printf("[crawlPackage] References: %v", doc.References)
	for _, ref := range doc.References {
		appendPackage(c, ref)
	}
	
	schedulePackage(c, doc.ImportPath, time.Now().Add(DefaultPackageAge).Add(
		time.Duration(rand.Int63n(int64(DefaultPackageAge)/10)-
			int64(DefaultPackageAge)/5)))
}

func pushPerson(c appengine.Context, p *gcc.Person) (hasNewPkg bool) {
	for _, proj := range p.Packages {
		if appendPackage(c, proj) {
			hasNewPkg = true
		}
	}
	
	site, username := gcc.ParsePersonId(p.Id)

	schedulePerson(c, site, username, time.Now().Add(DefaultPersonAge).Add(
		time.Duration(rand.Int63n(int64(DefaultPersonAge)/10)-
			int64(DefaultPersonAge)/5)))
			
	return
}

// debug function //
func tryCrawlPackage(c appengine.Context, w http.ResponseWriter, pkg string) {
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
	q := datastore.NewQuery(crawlerPackageKind)
	info.Total, err = q.Count(c)
	if err != nil {
		log.Printf("  crawler.Count() failed: %v", err)
		info.Total = -1
	}

	q = datastore.NewQuery(crawlerPackageKind).Project("Host").Distinct()
	var ents []CrawlingEntry
	_, err = q.GetAll(c, &ents)
	if err != nil {
		log.Printf("  crawler.Host.GetAll() failed: %v", err)
	} else {
		info.Hosts = make([]HostInfo, len(ents))
		for i, ent := range ents {
			info.Hosts[i].Host = ent.Host
			q = datastore.NewQuery(crawlerPackageKind).Filter("Host=", ent.Host)
			info.Hosts[i].Total, _ = q.Count(c)
			q = datastore.NewQuery(crawlerPackageKind).Filter("Host=", ent.Host).Filter("ScheduleTime<", now)
			info.Hosts[i].NeedCrawl, _ = q.Count(c)
		}
	}

	q = datastore.NewQuery(crawlerPackageKind).Filter("ScheduleTime<", now)
	info.NeedCrawl, err = q.Count(c)
	if err != nil {
		log.Printf("  crawler.ScheduleTIme<time.Now().Count() failed: %v", err)
		info.NeedCrawl = -1
	}

	return
}

/* Crawl a person's projects. */
func crawlPerson(c appengine.Context, httpClient *http.Client, id string) (hasNewPkg bool) {
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
				if appendPackage(c, proj) {
					hasNewPkg = true
				}
			}
		}
	case "bitbucket.org":
		p, err := doc.GetBitbucketPerson(httpClient, map[string]string{"owner": username})
		if err != nil {
			log.Printf("[crawlPerson] doc.GetBitbucketPerson(%s) failed: %v", username, err)
		} else {
			log.Printf("[crawlPerson] doc.GetBitbucketPerson(%s) sucess: %d projects", username, len(p.Projects))

			for _, proj := range p.Projects {
				if appendPackage(c, proj) {
					hasNewPkg = true
				}
			}
		}
	}
	schedulePerson(c, site, username, time.Now().Add(DefaultPersonAge).Add(
		time.Duration(rand.Int63n(int64(DefaultPersonAge)/10)-
			int64(DefaultPersonAge)/5)))
			
	return
}

func groupToFetchPerson(c appengine.Context) (groups map[string][]string) {
	pkgCount := 0
	oldest := time.Now()
	var oldestPkg string

	groups = make(map[string][]string)
	q := datastore.NewQuery(crawlerPersonKind).Filter("ScheduleTime<", time.Now()).Order("ScheduleTime")

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

	c.Infof("[groupToFetchPerson] got %d groups, %d persons, oldest %v(%v): %v",
		len(groups), pkgCount, oldest, time.Now().Sub(oldest), oldestPkg)

	return groups
}

func findCrawlingEntry(c appengine.Context, kind string, id string) (*CrawlingEntry, error) {
	ddb := NewDocDB(c, kind)

	var ent CrawlingEntry
	err, exists := ddb.Get(id, &ent)
	if exists {
		return &ent, nil
	}

	if err != nil {
		return nil, err
	}

	return nil, nil
}

// check the package, crawl if needed
func checkPackage(c appengine.Context, pkg string) {
	ent, err := findCrawlingEntry(c, crawlerPackageKind, pkg)
	if ent == nil {
		c.Errorf("Fail to find package %s: %v", pkg, err)
		return
	}
	
	if ent.ScheduleTime.After(time.Now()) {
		c.Infof("Package %s was scheduled to %v", pkg, ent.ScheduleTime)
		return
	}
	
	httpClient := genHttpClient(c)
	crawlPackage(c, httpClient, pkg)
}

// check the person, crawl if needed
func checkPerson(c appengine.Context, id string) {
	ent, err := findCrawlingEntry(c, crawlerPersonKind, id)
	if ent == nil {
		c.Errorf("Fail to find person %s: %v", id, err)
		return
	}
	
	if ent.ScheduleTime.After(time.Now()) {
		c.Infof("Person %s was scheduled to %v", id, ent.ScheduleTime)
		return
	}
	
	httpClient := genHttpClient(c)
	crawlPerson(c, httpClient, id)
}

func listCrawlEntries(c appengine.Context, kind string, l int) (pkgs []string) {
	if kind != crawlerPackageKind && kind != crawlerPersonKind {
		return nil
	}
	q := datastore.NewQuery(kind).Filter("ScheduleTime<", 
		time.Now()).Order("ScheduleTime")
		
	t := q.Run(c)
	for {
		var ent CrawlingEntry
		key, err := t.Next(&ent)
		if err != nil {
			break
		}
		
		pkgs = append(pkgs, key.StringID())
		if l > 0 && len(pkgs) >= l {
			break
		}
	}
	
	return pkgs
}

func reportBadPackage(c appengine.Context, pkg string) {
	err := datastore.Delete(c, datastore.NewKey(c, crawlerPackageKind, pkg, 0, nil))
	if err != nil {
		c.Errorf("Delete package %s in %s failed: %v", pkg, crawlerPackageKind, err)
		return
	}
	
	c.Infof("Package entry %s in %s removed", pkg, crawlerPackageKind)
}
