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
	Tokens []string
}

func (ts *TokenSet) Clear(field string) error {
	for {
		q := datastore.NewQuery(ts.typePrefix+field)
		cnt, err := q.Count(ts.c)
		if err != nil {
			return err
		}
		if cnt == 0 {
			return nil
		}
		log.Printf("    [ts.Clear] %d items left", cnt)
		
		q = datastore.NewQuery(ts.typePrefix+field).KeysOnly().Limit(1000)
		keys, err := q.GetAll(ts.c, nil)
		if err != nil {
			return err
		}
		log.Printf("    [ts.Clear] Deleting %d entries...", len(keys))
		err = datastore.DeleteMulti(ts.c, keys)
		if err != nil {
			return err
		}
	}
}

func (ts *TokenSet) Index(field, id string, tokens villa.StrSet) error {
	log.Printf("    [ts.Index] Adding %d tokens for field:%s id:%s", 
		len(tokens), field, id)
	_, err := datastore.Put(ts.c, datastore.NewKey(ts.c, ts.typePrefix+field,
		id, 0, nil), &IndexEntry{
			Tokens: tokens.Elements(),
		})
	if err != nil {
		return err
	}

	return nil
}

func (ts *TokenSet) Search(field string, tokens villa.StrSet) ([]string, error) {
	q := datastore.NewQuery(ts.typePrefix+field)
	for token := range tokens {
		q = q.Filter("Tokens=", token)
	}
	q = q.KeysOnly()
	
	keys, err := q.GetAll(ts.c, nil)
	if err != nil {
		return nil, err
	}

	res := make([]string, len(keys))
	for i, key := range keys {
		res[i] = key.StringID()
	}
	log.Printf("    [ts.Search] %d entries for tokens %v", len(res), tokens)
	
	return res, nil
}

func (ts *TokenSet) Count(field string, tokens villa.StrSet) (int, error) {
	q := datastore.NewQuery(ts.typePrefix+field)
	for token := range tokens {
		q = q.Filter("Tokens=", token)
	}
	
	return q.Count(ts.c)
}
