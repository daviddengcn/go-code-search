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

func (db *DocDB) Get(id string, doc interface{}) error {
	err := datastore.Get(db.c, datastore.NewKey(db.c, db.kind, id, 0, nil), doc)
	_, ok := err.(*datastore.ErrFieldMismatch)
	if ok {
		return nil
	}
	return err
}
