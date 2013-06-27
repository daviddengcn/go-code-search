package gocode

import (
	"appengine"
	"appengine/datastore"
	"appengine/memcache"
	"errors"
	"github.com/daviddengcn/gddo/doc"
	"github.com/daviddengcn/go-code-crawl"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"strings"
	"time"
)

func init() {
	doc.SetGithubCredentials("94446b37edb575accd8b",
		"15f55815f0515a3f6ad057aaffa9ea83dceb220b")
	doc.SetUserAgent("Go-Code-Search-Engine")
}

const (
	DefaultPackageAge = 10 * 24 * time.Hour
	DefaultPersonAge  = 10 * 24 * time.Hour
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
	ddb := NewCachedDocDB(c, kindCrawlerPackage)

	var ent CrawlingEntry
	err, _ := ddb.Get(pkg, &ent)
	if err != nil {
		log.Printf("[scheduledPackage] Get(crawler, %s) failed: %v", pkg, err)
	}

	mayAbsent := err != nil // ddb.Get may failed even if the doc exists

	ent.ScheduleTime = sTime

	u := urlOfPackage(pkg)
	if u != nil {
		ent.Host = u.Host
	}

	if mayAbsent {
		CachedComputingInvalidate(c, hostAllKind, kindCrawlerPackage+":"+ent.Host)
	}

	err = ddb.Put(pkg, &ent)
	if err != nil {
		log.Printf("datastore.Put failed: %v", err)
		return err
	}

	c.Infof("Schedule package %s to %v", pkg, sTime)
	return nil
}

// returns true if a new package is appended to the crawling list
func appendPackage(c appengine.Context, pkg string) bool {
	if !doc.IsValidRemotePath(pkg) {
		// log.Printf("  [appendPackage] Not a valid remote path: %s", pkg)
		return false
	}
	ddb := NewCachedDocDB(c, kindCrawlerPackage)

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

func schedulePerson(c appengine.Context, site, username string, sTime time.Time) error {
	ddb := NewCachedDocDB(c, kindCrawlerPerson)

	var ent CrawlingEntry

	id := gcc.IdOfPerson(site, username)
	/*
		err, _ := ddb.Get(id, &ent)
		if err != nil {
			log.Printf("  [scheduledPerson] crawler-person.Get(%s) failed: %v", id,
				err)
		}
	*/

	ent.ScheduleTime = sTime
	ent.Host = site

	CachedComputingInvalidate(c, hostAllKind, kindCrawlerPerson+":"+ent.Host)

	err := ddb.Put(id, &ent)
	if err != nil {
		c.Errorf("  [scheduledPerson] crawler-person.Put(%s) failed: %v", id,
			err)
		return err
	}

	c.Infof("Schedule person %s to %v", id, sTime)
	return nil
}

func appendPerson(c appengine.Context, site, username string) bool {
	ddb := NewCachedDocDB(c, kindCrawlerPerson)

	id := gcc.IdOfPerson(site, username)

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

func pushPackage(c appengine.Context, p *gcc.Package) (succ bool) {
	// copy Package as a DocInfo
	d := DocInfo {
		Name:        p.Name,
		Package:     p.ImportPath,
		Synopsis:    p.Synopsis,
		Description: p.Doc,
		LastUpdated: time.Now(),
		Author:      authorOfPackage(p.ImportPath),
		ProjectURL:  p.ProjectURL,
		StarCount:   p.StarCount,
		ReadmeFn:    p.ReadmeFn,
		ReadmeData:  p.ReadmeData,
	}

	d.Imports = nil
	for _, imp := range p.Imports {
		if doc.IsValidRemotePath(imp) {
			d.Imports = append(d.Imports, imp)
		}
	}
	
	// save DocInfo into fetchedDoc DB
	ddb := NewDocDB(c, kindFetchedDoc)
	err := ddb.Put(d.Package, &d)
	if err != nil {
		c.Errorf("ddb.Put(%s) failed: %v", err)
		return false
	}

	// append new authors	
	if strings.HasPrefix(d.Package, "github.com/") {
		appendPerson(c, "github.com", d.Author)
	} else if strings.HasPrefix(d.Package, "bitbucket.org/") {
		appendPerson(c, "bitbucket.org", d.Author)
	}
	
	for _, imp := range d.Imports {
		appendPackage(c, imp)
	}
	log.Printf("[crawlPackage] References: %v", p.References)
	for _, ref := range p.References {
		appendPackage(c, ref)
	}

	schedulePackage(c, d.Package, time.Now().Add(DefaultPackageAge).Add(
		time.Duration(rand.Int63n(int64(DefaultPackageAge)/10)-
			int64(DefaultPackageAge)/5)))
			
	return true
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

type CrawlerKindInfo struct {
	Total     int
	NeedCrawl int

	Hosts []HostInfo
}

type CrawlerInfo struct {
	Package, Person *CrawlerKindInfo
	CompTime        time.Duration
}

const hostAllKind = "host-all"

func compHostAll(c appengine.Context, dbKindHost string, v interface{}) (err error) {
	pv, ok := v.(*int)
	if !ok {
		return errors.New("Wrong type")
	}

	p := strings.Index(dbKindHost, ":")
	dbKind, host := dbKindHost[:p], dbKindHost[p+1:]

	q := datastore.NewQuery(dbKind)

	if host != "<all>" {
		q = q.Filter("Host=", host)
	}
	*pv, err = q.Count(c)
	return err
}

func fetchCrawlerKindInfo(c appengine.Context, kind string, now time.Time) (info *CrawlerKindInfo) {
	ccHostAll := NewCachedComputing(c, hostAllKind, compHostAll)

	info = &CrawlerKindInfo{}

	var err error
	_ = ccHostAll.Get(kind+":<all>", &(info.Total))

	q := datastore.NewQuery(kind).Filter("ScheduleTime<", now)
	info.NeedCrawl, err = q.Count(c)
	if err != nil {
		log.Printf("  crawler.ScheduleTime<time.Now().Count() failed: %v", err)
		info.NeedCrawl = -1
	}

	// get all possible sites
	q = datastore.NewQuery(kind).Project("Host").Distinct()
	var ents []CrawlingEntry
	_, err = q.GetAll(c, &ents)
	if err != nil {
		log.Printf("  crawler.Host.GetAll() failed: %v", err)
	} else {
		info.Hosts = make([]HostInfo, len(ents))
		for i, ent := range ents {
			info.Hosts[i].Host = ent.Host

			_ = ccHostAll.Get(kind+":"+ent.Host, &(info.Hosts[i].Total))
			//q = datastore.NewQuery(kind).Filter("Host=", ent.Host)
			//info.Hosts[i].Total, _ = q.Count(c)

			q = datastore.NewQuery(kind).Filter("Host=", ent.Host).Filter("ScheduleTime<", now)
			info.Hosts[i].NeedCrawl, _ = q.Count(c)
		}
	}

	return
}

func fetchCrawlerInfo(c appengine.Context) (info *CrawlerInfo) {
	now := time.Now()
	info = &CrawlerInfo{
		Package: fetchCrawlerKindInfo(c, kindCrawlerPackage, now),
		Person:  fetchCrawlerKindInfo(c, kindCrawlerPerson, now),
	}

	info.CompTime = time.Now().Sub(now)

	return info
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

const maxCrawlEntriesInCache = 1000

func queryCrawlEntries(c appengine.Context, kind string, l int) (pkgs []string) {
	q := datastore.NewQuery(kind).Filter("ScheduleTime<",
		time.Now()).Order("ScheduleTime").KeysOnly().Limit(l)
		
	keys, err := q.GetAll(c, nil)
	if err != nil {
		return nil
	}
	
	pkgs = make([]string, len(keys))
	for i, key := range keys {
		pkgs[i] = key.StringID()
	}
	
	return pkgs
}

// returns nil if not found or other error
func crawlEntriesInCache(c appengine.Context, kind string) (pkgs []string){
	mcID := prefixToCrawl + kind
	memcache.Gob.Get(c, mcID, &pkgs)
	return pkgs
}

func putCrawlEntriesInCache(c appengine.Context, kind string, pkgs []string){
	mcID := prefixToCrawl + kind
	memcache.Gob.Set(c, &memcache.Item{
		Key:    mcID,
		Object: pkgs,
	})
}

func clearCrawlEntriesInCache(c appengine.Context, kind string){
	mcID := prefixToCrawl + kind
	memcache.Delete(c, mcID)
}

func listCrawlEntries(c appengine.Context, kind string, l int) (pkgs []string) {
	if kind != kindCrawlerPackage && kind != kindCrawlerPerson {
		return nil
	}
	
	if l < 0 || l >= maxCrawlEntriesInCache {
		return queryCrawlEntries(c, kind, l)
	}
	
	cachedPkgs := crawlEntriesInCache(c, kind)
	c.Infof("%d %s entries in cache", len(cachedPkgs), kind)
	if len(cachedPkgs) < l {
		// not enough entries, refetch by query
		cachedPkgs = queryCrawlEntries(c, kind, maxCrawlEntriesInCache)
		c.Infof("query %d %s entries", len(cachedPkgs), kind)
	}
	
	// move from cache
	pkgs = make([]string, l)
	l = copy(pkgs, cachedPkgs)
	pkgs = pkgs[:l]
	cachedPkgs = cachedPkgs[l:]
	c.Infof("%d %s entries got, %d left", len(pkgs), kind, len(cachedPkgs))
	
	if len(cachedPkgs) > 0 {
		// write back if some left
		putCrawlEntriesInCache(c, kind, cachedPkgs)
	} else {
		// or clear it if nothing left
		clearCrawlEntriesInCache(c, kind)
	}
	return pkgs
}

func touchPackage(c appengine.Context, pkg string) (earlySchedule bool) {
	ddb := NewCachedDocDB(c, kindCrawlerPackage)

	var ent CrawlingEntry
	err, exists := ddb.Get(pkg, &ent)
	if err != nil {
		log.Printf("[scheduledPackage] Get(crawler, %s) failed: %v", pkg, err)
	}

	if exists {
		if ent.ScheduleTime.Before(time.Now()) {
			return true
		}
	}

	err = schedulePackage(c, pkg, time.Now())
	if err != nil {
		c.Errorf("[touchPackage] schedulePackage(%s) failed: %v", pkg, err)
	}

	return false
}

func indexFetchedDocs(c appengine.Context, ttl time.Duration) int {
	start := time.Now()
	q := datastore.NewQuery(kindFetchedDoc)
	t := q.Run(c)
	i := 0
	for ; i < 1000 ; i ++ {
		if time.Now().Sub(start) > ttl {
			c.Infof("%v elapsed, quit with %d entries processed", ttl, i)
			break
		}
		
		var d DocInfo
		k, err := t.Next(&d)
		if err == datastore.Done {
			c.Infof("t.Next Done: %d entries processed", i)
			break
		}
		
		if !DocGetOk(err) {
			c.Errorf("t.Next failed: %v", err)
			break
		}
		
		err = processDocument(c, &d)
		if err != nil {
			c.Errorf("Index doc %s failed: %v", d.Package, err)
			continue
		}
		
		err = datastore.Delete(c, k)
		if err != nil {
			c.Errorf("Deleting doc %s failed: %v", d.Package, err)
		}
		c.Infof("%d. Process entry %s success!", (i + 1), d.Package)
	}
	
	return i
}
