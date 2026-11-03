package emit

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"sort"
	"sync"
	"time"
)

// SessionMin is how long two players must share a room before it counts: REWARDS_SESSION_MIN or 3 minutes.
func SessionMin() time.Duration {
	if d, err := time.ParseDuration(os.Getenv("REWARDS_SESSION_MIN")); err == nil && d > 0 {
		return d
	}
	return 3 * time.Minute
}

// Room is one room (lobby, session, match) as a host sees it right now: a stable id, and the players
// in it as identities rewards can resolve (for a Switch title: {"switch", <BAAS user id>}).
// A player the host cannot map to an account is simply left out; leaving them out also means they do
// not count as the "other player" that makes a session.
type Room struct {
	ID      string
	Players []Identity
}

// Sessions turns room snapshots into `game_sessions.<game>` events: a player counts once per room after
// MinTime of unbroken time in it together with at least one other player. Not safe for concurrent
// Tick calls; Watch runs it from one goroutine.
type Sessions struct {
	rw      *Client
	metric  string
	min     time.Duration
	since   map[string]time.Time
	emitted map[string]bool
	onTick  func(State)
}

// NewSessions reports through rw under metric (for example "game_sessions.outbound"). min <= 0 uses SessionMin().
func NewSessions(rw *Client, metric string, min time.Duration) *Sessions {
	if min <= 0 {
		min = SessionMin()
	}
	return &Sessions{rw: rw, metric: metric, min: min, since: map[string]time.Time{}, emitted: map[string]bool{}}
}

// Tick takes the rooms as of now and emits the players that just crossed the minimum. A room with
// fewer than two mapped players resets its players. It returns how many events it sent.
func (s *Sessions) Tick(now time.Time, rooms []Room) int {
	seen := map[string]bool{}
	n := 0
	sort.Slice(rooms, func(i, j int) bool { return rooms[i].ID < rooms[j].ID })
	for _, r := range rooms {
		uniq := map[Identity]bool{}
		for _, p := range r.Players {
			if p.Namespace != "" && p.Subject != "" {
				uniq[p] = true
			}
		}
		if len(uniq) < 2 {
			continue
		}
		for p := range uniq {
			k := r.ID + "\x00" + p.Namespace + "\x00" + p.Subject
			seen[k] = true
			if _, ok := s.since[k]; !ok {
				s.since[k] = now
			}
			if !s.emitted[k] && now.Sub(s.since[k]) >= s.min {
				s.emitted[k] = true
				p := p
				s.rw.Event(Event{ID: s.metric + ":" + r.ID + ":" + p.Namespace + ":" + p.Subject, Metric: s.metric,
					Player: &p, Keys: []string{r.ID}})
				n++
			}
		}
	}
	for k := range s.since {
		if !seen[k] {
			delete(s.since, k)
		}
	}
	for k := range s.emitted {
		if !seen[k] {
			delete(s.emitted, k)
		}
	}
	return n
}

// State is the session clock as plain data (room/player key -> since, and which keys already emitted),
// for a host to keep in its durable store (Postgres) so a restart loses nothing.
type State struct {
	Since   map[string]time.Time `json:"since"`
	Emitted []string             `json:"emitted"`
}

// State copies the clock. Call it after each Tick and save it when it changed.
func (s *Sessions) State() State {
	st := State{Since: make(map[string]time.Time, len(s.since))}
	for k, v := range s.since {
		st.Since[k] = v
	}
	for k := range s.emitted {
		st.Emitted = append(st.Emitted, k)
	}
	sort.Strings(st.Emitted)
	return st
}

// Restore loads a saved clock before the first Tick. Keys for rooms that are gone are dropped by the next Tick.
func (s *Sessions) Restore(st State) {
	for k, v := range st.Since {
		s.since[k] = v
	}
	for _, k := range st.Emitted {
		s.emitted[k] = true
	}
}

// OnTick, when set, is called by Watch after every tick with the clock, so the host can save it.
// (Set it before Watch.)
func (s *Sessions) SetOnTick(f func(State)) { s.onTick = f }

// Watch calls snapshot every `every` (default 20 s) and ticks until ctx ends. With rewards disabled
// it returns at once and snapshot is never called. The room id must be unique over time (add the
// room's creation time or a counter if the host reuses names), because it is the dedupe key.
func (s *Sessions) Watch(ctx context.Context, every time.Duration, snapshot func() []Room) {
	if !s.rw.Enabled() {
		return
	}
	if every <= 0 {
		every = 20 * time.Second
	}
	go func() {
		t := time.NewTicker(every)
		defer t.Stop()
		for {
			s.Tick(time.Now(), snapshot())
			if s.onTick != nil {
				s.onTick(s.State())
			}
			select {
			case <-t.C:
			case <-ctx.Done():
				return
			}
		}
	}()
}

// Registry lets a host that learns room membership from events (join, leave) instead of holding a
// room table keep one for Watch: Join and Leave are safe to call from any goroutine.
type Registry struct {
	mu    sync.Mutex
	rooms map[string]map[Identity]bool
}

func (g *Registry) Join(room string, p Identity) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.rooms == nil {
		g.rooms = map[string]map[Identity]bool{}
	}
	if g.rooms[room] == nil {
		g.rooms[room] = map[Identity]bool{}
	}
	g.rooms[room][p] = true
}

func (g *Registry) Leave(room string, p Identity) {
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.rooms[room], p)
	if len(g.rooms[room]) == 0 {
		delete(g.rooms, room)
	}
}

// Snapshot is a snapshot func for Sessions.Watch.
func (g *Registry) Snapshot() []Room {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make([]Room, 0, len(g.rooms))
	for id, ps := range g.rooms {
		r := Room{ID: id}
		for p := range ps {
			r.Players = append(r.Players, p)
		}
		out = append(out, r)
	}
	return out
}

// PersistSessions keeps the session clock in the emit database under name: it restores a saved clock now
// and saves the clock after every tick when it changed. It does nothing without REWARDS_EMIT_DATABASE_URL.
// Call it before Watch.
func (c *Client) PersistSessions(name string, s *Sessions) {
	if c == nil || s == nil || c.store() == nil {
		return
	}
	if b, ok := c.LoadState(name); ok {
		var st State
		if json.Unmarshal(b, &st) == nil {
			s.Restore(st)
		}
	}
	var last []byte
	s.SetOnTick(func(st State) {
		b, err := json.Marshal(st)
		if err != nil || bytes.Equal(b, last) {
			return
		}
		c.SaveState(name, b)
		last = b
	})
}
