package emit

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type sink struct {
	mu   sync.Mutex
	got  []map[string]any
	fail atomic.Int32
	key  string
}

func (s *sink) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if s.fail.Add(-1) >= 0 {
		w.WriteHeader(503)
		return
	}
	s.key = r.Header.Get("X-Internal-Key")
	b, _ := io.ReadAll(r.Body)
	var in struct{ Events []map[string]any }
	json.Unmarshal(b, &in)
	s.mu.Lock()
	s.got = append(s.got, in.Events...)
	s.mu.Unlock()
	w.Write([]byte(`{"results":[]}`))
}

func (s *sink) n() int { s.mu.Lock(); defer s.mu.Unlock(); return len(s.got) }

func TestDisabledAndNilAreNoOps(t *testing.T) {
	var c *Client
	c.Event(Event{ID: "x", Metric: "m"})
	c.Close(context.Background())
	d := New(Config{})
	d.Event(Event{ID: "x", Metric: "m"})
	d.Close(context.Background())
	if d.Enabled() {
		t.Fatal("unconfigured client must be disabled")
	}
}

func TestDeliversBatchedWithKeyAndDefaults(t *testing.T) {
	s := &sink{}
	srv := httptest.NewServer(s)
	defer srv.Close()
	c := New(Config{URL: srv.URL, Key: "k", Source: "unit", Flush: 20 * time.Millisecond})
	for i := 0; i < 120; i++ {
		c.Event(Event{ID: string(rune('a'+i%26)) + string(rune('A'+i/26)), Metric: "trades_completed", Account: "acct"})
	}
	c.Close(context.Background())
	if s.n() != 120 || s.key != "k" {
		t.Fatalf("delivered %d key %q", s.n(), s.key)
	}
	if s.got[0]["source"] != "unit" || s.got[0]["delta"] != 1.0 {
		t.Fatalf("%v", s.got[0])
	}
}

func TestRetriesAfterFailure(t *testing.T) {
	s := &sink{}
	s.fail.Store(2)
	srv := httptest.NewServer(s)
	defer srv.Close()
	c := New(Config{URL: srv.URL, Key: "k", Source: "unit", Flush: 10 * time.Millisecond})
	c.Event(Event{ID: "1", Metric: "m"})
	deadline := time.Now().Add(8 * time.Second)
	for s.n() == 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	c.Close(context.Background())
	if s.n() != 1 {
		t.Fatalf("delivered %d after retries", s.n())
	}
}

func TestEventNeverBlocksWhenServiceIsDown(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { time.Sleep(time.Second); w.WriteHeader(503) }))
	defer srv.Close()
	c := New(Config{URL: srv.URL, Key: "k", Source: "unit", QueueSize: 4, Flush: time.Hour})
	start := time.Now()
	for i := 0; i < 10000; i++ {
		c.Event(Event{ID: "e", Metric: "m"})
	}
	if time.Since(start) > 500*time.Millisecond {
		t.Fatal("Event blocked")
	}
	if c.Dropped() == 0 {
		t.Fatal("a full queue must drop, not block")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	c.Close(ctx)
}

func TestClientErrorDropsBatch(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits.Add(1); w.WriteHeader(401) }))
	defer srv.Close()
	c := New(Config{URL: srv.URL, Key: "bad", Source: "unit", Flush: 10 * time.Millisecond})
	c.Event(Event{ID: "1", Metric: "m"})
	c.Close(context.Background())
	if hits.Load() != 1 || c.Dropped() != 1 {
		t.Fatalf("hits %d dropped %d", hits.Load(), c.Dropped())
	}
}
