package gocode

import (
	"appengine"
	"appengine/datastore"
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
	ddb := NewCachedDocDB(c, crawlerPackageKind)

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
		CachedComputingInvalidate(c, hostAllKind, crawlerPackageKind+":"+ent.Host)
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
	ddb := NewCachedDocDB(c, crawlerPackageKind)

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
	ddb := NewCachedDocDB(c, crawlerPersonKind)

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

	CachedComputingInvalidate(c, hostAllKind, crawlerPersonKind+":"+ent.Host)

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
	ddb := NewCachedDocDB(c, crawlerPersonKind)

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
		Package: fetchCrawlerKindInfo(c, crawlerPackageKind, now),
		Person:  fetchCrawlerKindInfo(c, crawlerPersonKind, now),
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
	ddb := NewCachedDocDB(c, crawlerPackageKind)
	err := ddb.Delete(pkg)
	if err != nil {
		c.Errorf("Delete package %s in %s failed: %v", pkg, crawlerPackageKind, err)
		return
	}

	c.Infof("Package entry %s in %s removed", pkg, crawlerPackageKind)
}

func touchPackage(c appengine.Context, pkg string) (earlySchedule bool) {
	ddb := NewCachedDocDB(c, crawlerPackageKind)

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
