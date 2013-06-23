package gocode

import (
	"appengine"
	"appengine/datastore"
	"log"
	"net/http"
//	"sync"
	"time"
)

var gRunning bool = false

func startBackend(w http.ResponseWriter, r *http.Request) {
	log.Println("Backend started!")
	gRunning = true
	//c := appengine.NewContext(r)
	/*
		err := runtime.RunInBackground(c, crawlingLooop)
		if err != nil {
			log.Printf("runtime.RunInBackground failed: %v", err)
			c.Errorf("runtime.RunInBackground failed: %v", err)
			go crawlingLooop(c)
		}
	*/
	//crawlingLooop(c, 0, true)
}

func stopBackend(w http.ResponseWriter, r *http.Request) {
	log.Println("Backend stopped!")
	gRunning = false
}
/*
func doCrawlingLoopParallel(c appengine.Context, ttl time.Duration) {
	c.Infof("Entering crawling loop...")
	httpClient := genHttpClient(c)
	stopTime := time.Now().Add(ttl)
	for {
		var wg sync.WaitGroup
		doNothing := true

		grps := groupToFetch(c)
		if len(grps) > 0 {
			doNothing = false

			wg.Add(len(grps))
			for _, pkgs := range grps {
				go func(pkgs []string) {
					for _, pkg := range pkgs {
						c.Infof("Crawling package %s ...", pkg)
						crawlPackage(c, httpClient, pkg)
						
						if ttl > 0 {
							if time.Now().After(stopTime) {
								break
							}
						}
						time.Sleep(5 * time.Second)
					}

					wg.Done()
				}(pkgs)
			}
		}

		grps = groupToFetchPerson(c)
		if len(grps) > 0 {
			doNothing = false

			wg.Add(len(grps))
			for site, persons := range grps {
				go func(site string, persons []string) {
					for _, p := range persons {
						c.Infof("Crawling person %s ...", p)
						crawlPerson(c, httpClient, p)
						
						if ttl > 0 {
							if time.Now().After(stopTime) {
								break
							}
						}
						time.Sleep(5 * time.Second)
					}

					wg.Done()
				}(site, persons)
			}
		}

		wg.Wait()

						
		if ttl > 0 {
			if time.Now().After(stopTime) {
				break
			}
		}
		
		if doNothing {
			break
		}
	}

	c.Infof("Leaving background loop...")
}
*/

func doCrawlingLoop(c appengine.Context, ttl time.Duration) {
	c.Infof("Entering crawling loop...")

	httpClient := genHttpClient(c)
	stopTime := time.Now().Add(ttl)
mainloop:
	for {
		c.Infof("Begin crawling packages...")
		
		q := datastore.NewQuery(crawlerPackageKind).Filter("ScheduleTime<", 
			time.Now()).Order("ScheduleTime")

		t := q.Run(c)
		for {
			var ent CrawlingEntry
			key, err := t.Next(&ent)
			if err == datastore.Done {
				break
			}
			
			
			if err != nil {
				if !DocGetOk(err) {
					log.Printf("  [doCrawlingLoop] t.Next failed: %#v", err)
					break
				}
			}
			
			pkg := key.StringID()
			
			c.Infof("Crawling package %s ...", pkg)
			crawlPackage(c, httpClient, pkg)
			if time.Now().After(stopTime) {
				break mainloop
			}
		}
		
		c.Infof("Begin crawling persons...")
		
		hasNewPkg := false
		q = datastore.NewQuery(crawlerPersonKind).Filter("ScheduleTime<", 
			time.Now()).Order("ScheduleTime")
			
		t = q.Run(c)
		for {
			var ent CrawlingEntry
			key, err := t.Next(&ent)
			if err == datastore.Done {
				break
			}
			
			
			if err != nil {
				if !DocGetOk(err) {
					log.Printf("  [doCrawlingLoop] t.Next failed: %#v", err)
					break
				}
			}
			
			person := key.StringID()
			
			c.Infof("Crawling person %s ...", person)
			if crawlPerson(c, httpClient, person) {
				hasNewPkg = true
			}
			if time.Now().After(stopTime) {
				break mainloop
			}
		}
		
		if !hasNewPkg {
			break
		}
	}

	c.Infof("Leaving background loop...")
}
