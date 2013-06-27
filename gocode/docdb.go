package gocode

import (
	"appengine"
	"appengine/datastore"
	"appengine/memcache"
	"fmt"
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


type ErrorSlice []error

func (es ErrorSlice) ErrorCount() (cnt int) {
	for _, e := range es {
		if e != nil {
			cnt ++
		}
	}
	
	return cnt
}

func (es ErrorSlice) Error() string {
	return fmt.Sprint([]error(es))
}

func ErrorSliceFromError(err error, Len int) ErrorSlice {
	if err == nil {
		return make(ErrorSlice, Len)
	}
	
	if me, ok := err.(appengine.MultiError); ok {
		return ErrorSlice(me)
	}
	
	errs := make(ErrorSlice, Len)
	for i := range errs {
		errs[i] = err
	}
	return errs
}

func (db *DocDB) PutMulti(ids []string, docs interface{}) ErrorSlice {
	if len(ids) == 0 {
		return nil
	}
	
	keys := make([]*datastore.Key, len(ids))
	for i, id := range ids {
		keys[i] = datastore.NewKey(db.c, db.kind, id, 0, nil)
	}
	_, err := datastore.PutMulti(db.c, keys, docs)
	
	return ErrorSliceFromError(err, len(ids))
}

func (db *DocDB) GetMulti(ids []string, docs interface{}) ErrorSlice {
	if len(ids) == 0 {
		return nil
	}
	
	keys := make([]*datastore.Key, len(ids))
	for i, id := range ids {
		keys[i] = datastore.NewKey(db.c, db.kind, id, 0, nil)
	}
	
	err := datastore.GetMulti(db.c, keys, docs)
	return ErrorSliceFromError(err, len(ids))
}

// err is non-nil only if mistakes other than ErrNoSucheEntity and ErrFieldMismatch
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

// err is non-nil only if mistakes other than ErrNoSucheEntity and ErrFieldMismatch
func (db *CachedDocDB) Get(id string, doc interface{}) (err error, exists bool) {
	mcID := prefixCachedDocDB + db.kind + ":" + id
	if _, err := memcache.Gob.Get(db.c, mcID, doc); err == nil {
		// found in memcache
		return nil, true
	}

	err, exists = (*DocDB)(db).Get(id, doc)
	if err != nil || !exists {
		return err, exists
	}

	memcache.Gob.Set(db.c, &memcache.Item{
		Key:    mcID,
		Object: doc,
	})

	return nil, true
}

func (db *CachedDocDB) Put(id string, doc interface{}) error {
	mcID := prefixCachedDocDB + db.kind + ":" + id
	memcache.Gob.Set(db.c, &memcache.Item{
		Key:    mcID,
		Object: doc,
	})

	return (*DocDB)(db).Put(id, doc)
}

func (db *CachedDocDB) Delete(id string) error {
	mcID := prefixCachedDocDB + db.kind + ":" + id
	memcache.Delete(db.c, mcID)
	return (*DocDB)(db).Delete(id)
}

/* CachedComputing */
type CachedComputing struct {
	c     appengine.Context
	kind  string
	compF func(c appengine.Context, id string, v interface{}) error
}

func NewCachedComputing(c appengine.Context, kind string, compF func(appengine.Context,
	string, interface{}) error) *CachedComputing {
	return &CachedComputing{
		c:     c,
		compF: compF,
	}
}

func (cc *CachedComputing) Get(id string, v interface{}) error {
	mcID := prefixCachedComputing + cc.kind + ":" + id
	if _, err := memcache.Gob.Get(cc.c, mcID, v); err == nil {
		return nil
	}

	// not found in memcache, compute it
	err := cc.compF(cc.c, id, v)
	if err != nil {
		return err
	}

	// put back
	memcache.Gob.Set(cc.c, &memcache.Item{
		Key:    mcID,
		Object: v,
	})

	return nil
}

func (cc *CachedComputing) Invalidate(id string) error {
	return CachedComputingInvalidate(cc.c, cc.kind, id)
}

func CachedComputingInvalidate(c appengine.Context, kind, id string) error {
	mcID := prefixCachedComputing + kind + ":" + id
	return memcache.Delete(c, mcID)
}
