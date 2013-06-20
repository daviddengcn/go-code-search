package gocode

import (
	"appengine"
	"appengine/runtime"
	"log"
	"net/http"
	"time"
	"sync"
)

var gRunning bool = false

func crawlingLooop(c appengine.Context) {
	log.Printf("Entering background loop...")
	//lastHost := ""
	for {
		if !gRunning {
			break
		}

		var wg sync.WaitGroup
		doNothing := true
		
		grps := groupToFetch(c)
		if len(grps) > 0 {
			doNothing = true
			
			wg.Add(len(grps))
			for _, pkgs := range grps {
				go func(pkgs []string) {
					for _, pkg := range pkgs {
						log.Printf("Crawling package %s ...", pkg)
						crawlPackage(c, pkg)
						time.Sleep(10 * time.Second)
					}
					
					wg.Done()
				}(pkgs)
			}
			/*
			for len(grps) > 0 {
				for host, pkgs := range grps {
					pkg := pkgs[len(pkgs)-1]
					pkgs = pkgs[:len(pkgs)-1]
					if len(pkgs) == 0 {
						delete(grps, host)
					} else {
						grps[host] = pkgs
					}
					
					if lastHost == host {
						time.Sleep(10 * time.Second)
					}
	
					log.Printf("Crawling package %s ...", pkg)
					crawlPackage(c, pkg)
					lastHost = host
				}
			}
			*/
		}
		
		wg.Wait()
		
		if doNothing {
			// sleep to avoid looping without sleeping
			time.Sleep(10 * time.Second)
		}
	}

	log.Printf("Leaving background loop...")
}

func startBackend(w http.ResponseWriter, r *http.Request) {
	log.Println("Backend started!")
	gRunning = true
	c := appengine.NewContext(r)
	err := runtime.RunInBackground(c, crawlingLooop)
	if err != nil {
		log.Printf("runtime.RunInBackground failed: %v", err)
		go crawlingLooop(c)
	}
}

func stopBackend(w http.ResponseWriter, r *http.Request) {
	log.Println("Backend stopped!")
	gRunning = false
}
