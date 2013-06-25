package gocode

import (
	"appengine"
	"appengine/datastore"
	"appengine/memcache"
	//"log"
)

type DocDB struct {
	c    appengine.Context
	kind string
}

func NewDocDB(c appengine.Context, kind string) *DocDB {
	return &DocDB{
		c:    c,
		kind: kind,
	}
}

func (db *DocDB) Put(id string, doc interface{}) error {
	_, err := datastore.Put(db.c, datastore.NewKey(db.c, db.kind, id, 0, nil), doc)

	return err
}

func (db *DocDB) Get(id string, doc interface{}) (err error, exists bool) {
	err = datastore.Get(db.c, datastore.NewKey(db.c, db.kind, id, 0, nil), doc)
	if err == datastore.ErrNoSuchEntity {
		return nil, false
	}

	if DocGetOk(err) {
		return nil, true
	}

	return err, false
}

func (db *DocDB) Delete(id string) error {
	return datastore.Delete(db.c, datastore.NewKey(db.c, db.kind, id, 0, nil))
}

func DocGetOk(err error) bool {
	if err == nil {
		return true
	}
	_, ok := err.(*datastore.ErrFieldMismatch)
	if ok {
		return true
	}
	return false
}

type CachedDocDB DocDB

func NewCachedDocDB(c appengine.Context, kind string) *CachedDocDB {
	return (*CachedDocDB)(NewDocDB(c, kind))
}

func (db *CachedDocDB) Get(id string, doc interface{}) (err error, exists bool) {
	mcID := "doc:" + db.kind + ":" + id
	if _, err := memcache.Gob.Get(db.c, mcID, doc); err == nil {
		// found in memcache
		return nil, true
	}
	
	err, exists = (*DocDB)(db).Get(id, doc)
	if err != nil || !exists {
		return err, exists
	}
	
	memcache.Gob.Set(db.c, &memcache.Item{
		Key: mcID,
		Object: doc,
	})
	
	return nil, true
}

func (db *CachedDocDB) Put(id string, doc interface{}) error {
	mcID := "doc:" + db.kind + ":" + id
	memcache.Gob.Set(db.c, &memcache.Item{
		Key: mcID,
		Object: doc,
	})
	
	return (*DocDB)(db).Put(id, doc)
}

func (db *CachedDocDB) Delete(id string) error {
	mcID := "doc:" + db.kind + ":" + id
	memcache.Delete(db.c, mcID)
	return (*DocDB)(db).Delete(id)
}
