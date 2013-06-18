package gocode

import (
	"appengine"
	"appengine/datastore"
	"github.com/daviddengcn/go-villa"
	"log"
)

type TokenSet struct {
	c          appengine.Context
	typePrefix string
}

func NewTokenSet(c appengine.Context, typePrefix string) *TokenSet {
	return &TokenSet{
		c:          c,
		typePrefix: typePrefix,
	}
}

type IndexEntry struct {
	Token string
	Id    string
}

func (ts *TokenSet) Delete(field, id string) error {
	log.Printf("    deleting field:%s id:%s", field, id)
	q := datastore.NewQuery(ts.typePrefix+field).Filter("Id=", id).KeysOnly()
	keys, err := q.GetAll(ts.c, nil)
	if err != nil {
		return err
	}
	log.Printf("    got %d keys", len(keys))

	if len(keys) > 0 {
		err = datastore.DeleteMulti(ts.c, keys)
		if err != nil {
			return err
		}
	}

	return nil
}

func (ts *TokenSet) Index(field, id string, tokens villa.StrSet) error {
	err := ts.Delete(field, id)
	if err != nil {
		return err
	}

	log.Printf("    adding %d tokens for field:%s id:%s", len(tokens), field, id)
	for token := range tokens {
		_, err := datastore.Put(ts.c, datastore.NewIncompleteKey(ts.c,
			ts.typePrefix+field, nil), &IndexEntry{
			Token: token,
			Id:    id,
		})
		if err != nil {
			return err
		}
	}

	return nil
}

func (ts *TokenSet) Search(field string, tokens villa.StrSet) ([]string, error) {
	var idSet villa.StrSet
	first := true
	for token := range tokens {
		q := datastore.NewQuery(ts.typePrefix+field).Filter("Token=", token)
		var entries []IndexEntry
		_, err := q.GetAll(ts.c, &entries)
		if err != nil {
			return nil, err
		}

		log.Printf("    %d entries for token %s", len(entries), token)

		if first {
			for _, entry := range entries {
				idSet.Put(entry.Id)
			}
			first = false
		} else {
			var newIdSet villa.StrSet
			for _, entry := range entries {
				if idSet.In(entry.Id) {
					newIdSet.Put(entry.Id)
				}
			}

			idSet = newIdSet
		}
	}

	return idSet.Elements(), nil
}

func (ts *TokenSet) SingleCount(field string, token string) (int, error) {
	q := datastore.NewQuery(ts.typePrefix+field).Filter("Token=", token)
	return q.Count(ts.c)
}
