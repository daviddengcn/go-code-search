package gocode

import (
	"appengine"
	"appengine/datastore"
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
