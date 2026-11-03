package emit

import (
	"context"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

type fakeRow struct {
	id      string
	payload []byte
	created time.Time
}

type fakeStore struct {
	mu    sync.Mutex
	rows  map[string]fakeRow // source\x00id
	state map[string][]byte
}

func newFake() *fakeStore { return &fakeStore{rows: map[string]fakeRow{}, state: map[string][]byte{}} }

func (f *fakeStore) insert(_ context.Context, src, id string, p []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.rows[src+"\x00"+id]; !ok {
		f.rows[src+"\x00"+id] = fakeRow{id, p, time.Now()}
	}
	return nil
}

func (f *fakeStore) pending(_ context.Context, src string, limit int) ([]outboxRow, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var rs []fakeRow
	for k, r := range f.rows {
		if strings.HasPrefix(k, src+"\x00") {
			rs = append(rs, r)
		}
	}
	sort.Slice(rs, func(i, j int) bool {
		if !rs[i].created.Equal(rs[j].created) {
			return rs[i].created.Before(rs[j].created)
		}
		return rs[i].id < rs[j].id
	})
	var out []outboxRow
	for _, r := range rs[:min(len(rs), limit)] {
		out = append(out, outboxRow{r.id, r.payload})
	}
	return out, nil
}

func (f *fakeStore) remove(_ context.Context, src string, ids []string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, id := range ids {
		delete(f.rows, src+"\x00"+id)
	}
	return nil
}

func (f *fakeStore) purge(_ context.Context, src string, before time.Time) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var n int64
	for k, r := range f.rows {
		if strings.HasPrefix(k, src+"\x00") && r.created.Before(before) {
			delete(f.rows, k)
			n++
		}
	}
	return n, nil
}

func (f *fakeStore) loadState(_ context.Context, src, name string) ([]byte, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	v, ok := f.state[src+"\x00"+name]
	return v, ok, nil
}

func (f *fakeStore) saveState(_ context.Context, src, name string, v []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.state[src+"\x00"+name] = v
	return nil
}

func (f *fakeStore) close() {}

func (f *fakeStore) count() int { f.mu.Lock(); defer f.mu.Unlock(); return len(f.rows) }

func TestRestartDeliversWhatClientOneStored(t *testing.T) {
	fs := newFake()
	s := &sink{}
	s.fail.Store(1 << 20) // rewards is down for client 1
	srv := httptest.NewServer(s)
	defer srv.Close()
	c1 := New(Config{URL: srv.URL, Key: "k", Source: "unit", Flush: 20 * time.Millisecond, store: fs})
	for _, id := range []string{"a", "b", "c", "a"} { // "a" twice: deduped by the primary key
		c1.Event(Event{ID: id, Metric: "m"})
	}
	c1.Close(context.Background())
	if fs.count() != 3 || s.n() != 0 {
		t.Fatalf("rows=%d delivered=%d, want 3 and 0", fs.count(), s.n())
	}
	// rewards is back and the process restarted
	s.fail.Store(0)
	c2 := New(Config{URL: srv.URL, Key: "k", Source: "unit", Flush: 20 * time.Millisecond, store: fs})
	deadline := time.Now().Add(3 * time.Second)
	for s.n() < 3 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	c2.Close(context.Background())
	if s.n() != 3 || fs.count() != 0 {
		t.Fatalf("delivered=%d rows=%d, want 3 and 0", s.n(), fs.count())
	}
}

func TestFailedPostKeepsRowsThenRetries(t *testing.T) {
	fs := newFake()
	s := &sink{}
	s.fail.Store(2)
	srv := httptest.NewServer(s)
	defer srv.Close()
	c := New(Config{URL: srv.URL, Key: "k", Source: "unit", Flush: 20 * time.Millisecond, store: fs})
	c.Event(Event{ID: "x", Metric: "m"})
	deadline := time.Now().Add(5 * time.Second)
	for s.n() < 1 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	c.Close(context.Background())
	if s.n() != 1 || fs.count() != 0 {
		t.Fatalf("delivered=%d rows=%d", s.n(), fs.count())
	}
}

func TestOtherSourcesAreLeftAlone(t *testing.T) {
	fs := newFake()
	_ = fs.insert(context.Background(), "other", "z", []byte(`{"event_id":"z","metric":"m"}`))
	s := &sink{}
	srv := httptest.NewServer(s)
	defer srv.Close()
	c := New(Config{URL: srv.URL, Key: "k", Source: "unit", store: fs})
	c.Event(Event{ID: "x", Metric: "m"})
	c.Close(context.Background())
	if s.n() != 1 || fs.count() != 1 {
		t.Fatalf("delivered=%d rows=%d, want 1 and 1", s.n(), fs.count())
	}
}

func TestOldRowsPurgedAndDefaultMaxAge(t *testing.T) {
	fs := newFake()
	_ = fs.insert(context.Background(), "unit", "old", []byte(`{}`))
	fs.mu.Lock()
	r := fs.rows["unit\x00old"]
	r.created = time.Now().Add(-8 * 24 * time.Hour)
	fs.rows["unit\x00old"] = r
	fs.mu.Unlock()
	s := &sink{}
	s.fail.Store(1 << 20)
	srv := httptest.NewServer(s)
	defer srv.Close()
	c := New(Config{URL: srv.URL, Key: "k", Source: "unit", store: fs})
	if c.cfg.MaxAge != 7*24*time.Hour {
		t.Fatalf("MaxAge %v", c.cfg.MaxAge)
	}
	c.Close(context.Background())
	if fs.count() != 0 || c.Dropped() != 1 {
		t.Fatalf("rows=%d dropped=%d", fs.count(), c.Dropped())
	}
}

func TestPersistSessionsRestoresClock(t *testing.T) {
	fs := newFake()
	s := &sink{}
	srv := httptest.NewServer(s)
	defer srv.Close()
	mk := func() *Client {
		return New(Config{URL: srv.URL, Key: "k", Source: "unit", Flush: 20 * time.Millisecond, store: fs})
	}
	room := []Room{{ID: "r1", Players: []Identity{{"switch", "1"}, {"switch", "2"}}}}
	c1 := mk()
	s1 := NewSessions(c1, "game_sessions.t", time.Minute)
	c1.PersistSessions("clock", s1)
	t0 := time.Now()
	s1.Tick(t0, room)
	s1.onTick(s1.State())
	c1.Close(context.Background())
	// restart: the clock must carry on from t0, so a tick a minute later emits
	c2 := mk()
	s2 := NewSessions(c2, "game_sessions.t", time.Minute)
	c2.PersistSessions("clock", s2)
	if n := s2.Tick(t0.Add(61*time.Second), room); n != 2 {
		t.Fatalf("emitted %d after restore, want 2", n)
	}
	c2.Close(context.Background())
}

func TestStateWithoutDatabaseIsNoOp(t *testing.T) {
	c := New(Config{URL: "http://x", Key: "k", Source: "unit"})
	c.SaveState("n", []byte("v"))
	if _, ok := c.LoadState("n"); ok {
		t.Fatal("no database must report false")
	}
	c.Close(context.Background())
}
