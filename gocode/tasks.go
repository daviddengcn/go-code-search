package gocode

import (
	"appengine"
	"appengine/taskqueue"
)

func addCheckTask(c appengine.Context, tp, id string) {
	t := taskqueue.NewPOSTTask("/check", map[string][]string {
		"tp": {tp}, "id": {id}})
	_, err := taskqueue.Add(c, t, "")
	if err != nil {
		c.Errorf("addCheckTask(%s, %s) failed: %v", tp, id, err)
	}
}
