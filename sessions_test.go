package emit

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func collect(t *testing.T) (*Client, func() []map[string]any) {
	var mu sync.Mutex
	var got []map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		var body struct{ Events []map[string]any }
		_ = json.Unmarshal(b, &body)
		mu.Lock()
		got = append(got, body.Events...)
		mu.Unlock()
	}))
	t.Cleanup(srv.Close)
	c := New(Config{URL: srv.URL, Key: "k", Source: "unit", Flush: 10 * time.Millisecond})
	return c, func() []map[string]any {
		c.Close(context.Background())
		mu.Lock()
		defer mu.Unlock()
		return got
	}
}

func TestSessionsNeedTwoPlayersAndTime(t *testing.T) {
	c, events := collect(t)
	s := NewSessions(c, "game_sessions.x", 3*time.Minute)
	a, b := Identity{"switch", "a"}, Identity{"switch", "b"}
	t0 := time.Now()
	// alone for ages: nothing
	s.Tick(t0, []Room{{ID: "r1", Players: []Identity{a}}})
	s.Tick(t0.Add(10*time.Minute), []Room{{ID: "r1", Players: []Identity{a}}})
	// b joins: clock starts now, not before
	if n := s.Tick(t0.Add(11*time.Minute), []Room{{ID: "r1", Players: []Identity{a, b}}}); n != 0 {
		t.Fatalf("early: %d", n)
	}
	if n := s.Tick(t0.Add(13*time.Minute), []Room{{ID: "r1", Players: []Identity{a, b}}}); n != 0 {
		t.Fatalf("2 min: %d", n)
	}
	if n := s.Tick(t0.Add(14*time.Minute), []Room{{ID: "r1", Players: []Identity{a, b, b}}}); n != 2 {
		t.Fatalf("3 min: %d", n)
	}
	// once only
	if n := s.Tick(t0.Add(20*time.Minute), []Room{{ID: "r1", Players: []Identity{a, b}}}); n != 0 {
		t.Fatalf("again: %d", n)
	}
	if ev := events(); len(ev) != 2 || ev[0]["metric"] != "game_sessions.x" {
		t.Fatalf("events %v", ev)
	}
}

func TestSessionsLeaveResets(t *testing.T) {
	c, _ := collect(t)
	s := NewSessions(c, "m", time.Minute)
	a, b := Identity{"switch", "a"}, Identity{"switch", "b"}
	t0 := time.Now()
	s.Tick(t0, []Room{{ID: "r", Players: []Identity{a, b}}})
	s.Tick(t0.Add(50*time.Second), []Room{{ID: "r", Players: []Identity{a}}}) // b left
	if n := s.Tick(t0.Add(70*time.Second), []Room{{ID: "r", Players: []Identity{a, b}}}); n != 0 {
		t.Fatalf("clock must restart: %d", n)
	}
	if n := s.Tick(t0.Add(131*time.Second), []Room{{ID: "r", Players: []Identity{a, b}}}); n != 2 {
		t.Fatalf("after a full minute: %d", n)
	}
}

func TestRegistrySnapshot(t *testing.T) {
	var g Registry
	a := Identity{"switch", "a"}
	g.Join("r", a)
	g.Join("r", Identity{"switch", "b"})
	if r := g.Snapshot(); len(r) != 1 || len(r[0].Players) != 2 {
		t.Fatalf("%v", r)
	}
	g.Leave("r", a)
	g.Leave("r", Identity{"switch", "b"})
	if len(g.Snapshot()) != 0 {
		t.Fatal("empty room kept")
	}
}
