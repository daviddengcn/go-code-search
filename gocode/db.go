package gocode

import (
	"appengine"
	"appengine/datastore"
	"time"
)

// constants for memcache
const (
	prefixCachedComputing = "cc:"  // cc:<kind>:<id>
	prefixCachedDocDB     = "doc:" // doc:<kind>:<id>
	prefixToCrawl         = "tc:"  // tc:<kind>
)

// constants for docs
const (
	kindCrawlerPackage = "crawler"
	kindCrawlerPerson  = "crawler-person"
	
	kindFetchedDoc = "fetched-docs"
	kindDocDB      = "doc"
	
	prefixIndex = "index:"
	fieldIndex  = "doc"
	kindIndex   = prefixIndex + fieldIndex
	
	prefixImports = "import:"
	fieldImports = "import"
	kindImports   = prefixImports + fieldImports
	
	kindToUpdate       = "to-update"
	kindPackageToCrawl = "to-crawl"
)


type DBInfo struct {
	Name  string
	Count int
}

func statDatabaseInfo(c appengine.Context) []DBInfo {
	kinds := []string {
		kindCrawlerPackage,
		kindCrawlerPerson,
		
		kindFetchedDoc,
		kindToUpdate,
		kindDocDB,
		kindIndex,
		
		kindImports,
	}
	
	dbs := make([]DBInfo, len(kinds))
	for i, kind := range kinds {
		cnt, err := datastore.NewQuery(kind).Count(c)
		if err != nil {
			c.Errorf("Count %s failed: %v", kind, err)
			cnt = -1
		}
		dbs[i].Name = kind
		dbs[i].Count = cnt
	}
	
	return dbs
}

func deletePackage(c appengine.Context, pkg string) {
	if err := NewCachedDocDB(c, kindCrawlerPackage).Delete(pkg); err != nil {
		c.Errorf("Delete package %s in %s failed: %v", pkg, kindCrawlerPackage, err)
	}
	if err := NewCachedDocDB(c, kindDocDB).Delete(pkg); err != nil {
		c.Errorf("Delete package %s in %s failed: %v", pkg, kindDocDB, err)
	}
	if err := NewCachedDocDB(c, kindIndex).Delete(pkg); err != nil {
		c.Errorf("Delete package %s in %s failed: %v", pkg, kindIndex, err)
	}
	if err := NewCachedDocDB(c, kindImports).Delete(pkg); err != nil {
		c.Errorf("Delete package %s in %s failed: %v", pkg, kindImports, err)
	}
}

func updateDocInfo(c appengine.Context, pkg string) {
	ddb := NewCachedDocDB(c, kindDocDB)
	var d DocInfo
	err, exists := ddb.Get(pkg, &d)
	if err != nil {
		c.Errorf("Get doc %s failed: %v", pkg, err)
		return
	}
	
	if !exists {
		c.Infof("Doc %s doesn't exist", pkg)
		return
	}
	
	err = processDocument(c, &d)
	if err != nil {
		c.Errorf("Process doc %s failed: %v", pkg, err)
	}
	
	c.Infof("Update doc %s success!", pkg)
}

func processToUpdate(c appengine.Context, ttl time.Duration) int {
	ddb := NewDocDB(c, kindToUpdate)
	start := time.Now()
	q := datastore.NewQuery(kindToUpdate).KeysOnly()
	t := q.Run(c)
	i := 0
	for ;i < 1000; i ++ {
		if time.Now().Sub(start) > ttl {
			c.Infof("%v elapsed, quit with %d entries processed", ttl, i)
			break
		}
		
		k, err := t.Next(nil)
		if err == datastore.Done {
			c.Infof("t.Next Done: %d entries processed", i)
			break
		}
		
		if !DocGetOk(err) {
			c.Errorf("t.Next failed: %v", err)
			break
		}
		
		pkg := k.StringID()
		updateDocInfo(c, pkg)
		
		err = ddb.Delete(pkg)
		if err != nil {
			c.Errorf("Delete(%s) in %s failed!", pkg, kindToUpdate)
		}
		
		c.Infof("%d. Process entry %s success!", (i + 1), pkg)
	}
	
	return i
}
