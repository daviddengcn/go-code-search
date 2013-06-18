package gocode

import (
	"appengine"
	//	"appengine/runtime"
	"appengine/datastore"
	"log"
	//	"net"
	"github.com/garyburd/gddo/doc"
//	"github.com/daviddengcn/go-villa"
	"math/rand"
	"net/http"
	"net/url"
	"time"
)

var gRunning bool = false

type CrawlingEntry struct {
	ScheduleTime time.Time
	Host string
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
	if err != nil && err != datastore.ErrNoSuchEntity {
		log.Printf("Get(crawler, %s) failed: %v", pkg, err)
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

	log.Printf("Schedule %s to %v", pkg, sTime)
}

const DefaultAge = 24 * time.Hour

func appendPackage(c appengine.Context, pkg string) {
	if !doc.IsValidRemotePath(pkg) {
		log.Printf("Not a valid remote path: %s", pkg)
		return
	}
	var ent CrawlingEntry
	err := datastore.Get(c, datastore.NewKey(c, "crawler", pkg, 0, nil), &ent)
	if err == nil {
		// already scheduled
		return
	}

	if err != datastore.ErrNoSuchEntity {
		log.Printf("Get(crawler, %s) failed: %v", pkg, err)
		return
	}

	scheduledPackage(c, pkg, time.Now())
}

func groupToFetch(c appengine.Context) (groups map[string][]string) {
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
			log.Printf("[groupToFetch] t.Next failed: %#v", err)
			break
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
	}
	
	log.Printf("[groupToFetch] got %d groups", len(groups))
	
	return groups
}

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
	scheduledPackage(c, pkg, time.Now().Add(DefaultAge).Add(
		time.Duration(rand.Int63n(int64(DefaultAge)/10)-int64(DefaultAge)/5)))
}

func crawlingLooop(c appengine.Context) {
	log.Printf("Entering background loop...")
	for {
		if !gRunning {
			break
		}

		grps := groupToFetch(c)
		
		for len(grps) > 0 {
			for host, pkgs := range grps {
				pkg := pkgs[len(pkgs) - 1]
				pkgs = pkgs[:len(pkgs) - 1]
				if len(pkgs) == 0 {
					delete(grps, host)
				} else {
					grps[host] = pkgs
				}
				
				log.Printf("Crawling package %s ...", pkg)
				crawlPackage(c, pkg)
			}
			time.Sleep(10 * time.Second)
		}

		time.Sleep(10 * time.Second)
	}

	log.Printf("Leaving background loop...")
}

func startBackend(w http.ResponseWriter, r *http.Request) {
	log.Println("Backend started!")
	gRunning = true
	c := appengine.NewContext(r)
	/*
		err := runtime.RunInBackground(c, crawlingLooop)
		if err != nil {
			log.Printf("runtime.RunInBackground failed: %v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	*/
	go crawlingLooop(c)
}

func stopBackend(w http.ResponseWriter, r *http.Request) {
	log.Println("Backend stopped!")
	gRunning = false
}
