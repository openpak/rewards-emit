// Package emit is the client every OpenPak game server uses to tell the rewards service that
// something happened (a race finished, a course was uploaded, a trade completed).
//
// It is built so that rewards can never hurt gameplay: Event never blocks and never returns an
// error, a full queue drops the new event, a slow or dead rewards service only delays delivery
// (batched, retried with backoff), and a client built without configuration does nothing at
// all. A nil *Client is also valid and does nothing.
//
// Counts are server-observed facts. Only call Event from the code path that proved the action
// (a result row written, a session ended with results), never from anything a console says.
//
// Event IDs are the dedupe key and must be stable for the same real-world occurrence, so a
// retry or a restart never counts twice, for example "mk8:race:<raceID>:<pid>".
package emit

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Identity is a player as the game server knows them: the adapter namespace ("switch", "wiiu",
// "3ds", "wfc", "np", "xbox360", "xbox") and the subject id registered for that link.
type Identity struct {
	Namespace string `json:"namespace"`
	Subject   string `json:"subject"`
}

// Event is one counted observation.
type Event struct {
	ID         string     // required, stable per occurrence
	Metric     string     // e.g. "kart_races_completed.mk8dx"
	Delta      int        // sum metrics: how many (default 1)
	Account    string     // the core account id, if the server has it
	Player     *Identity  // otherwise the network identity; rewards resolves it
	Keys       []string   // distinct metrics: course id, tournament id, ...
	KeyPlayers []Identity // distinct metrics about other people: co-players as identities
	At         time.Time  // when it happened (default now)
}

type wireEvent struct {
	EventID     string     `json:"event_id"`
	Source      string     `json:"source"`
	Metric      string     `json:"metric"`
	Delta       int        `json:"delta,omitempty"`
	AccountID   string     `json:"account_id,omitempty"`
	Identity    *Identity  `json:"identity,omitempty"`
	Keys        []string   `json:"keys,omitempty"`
	KeyIdentity []Identity `json:"key_identity,omitempty"`
	OccurredAt  time.Time  `json:"occurred_at"`
}

// Config tunes a Client. URL and Key empty means disabled.
type Config struct {
	URL       string // base URL of rewards, e.g. http://openpak-rewards:20180
	Key       string // the events key
	Source    string // who is sending, e.g. "nex-mario-kart-8"
	QueueSize int    // default 2048
	Batch     int    // default 50 (the service takes at most 100)
	Flush     time.Duration
	// MaxAge is how long an undelivered event is kept before it is dropped. Default 6 h.
	MaxAge time.Duration
	HTTP   *http.Client
}

// Client queues events and delivers them in the background.
type Client struct {
	cfg     Config
	q       chan wireEvent
	done    chan struct{}
	wg      sync.WaitGroup
	closed  atomic.Bool
	dropped atomic.Int64
	sent    atomic.Int64
}

// FromEnv builds a Client from REWARDS_EVENTS_URL and REWARDS_EVENTS_KEY (or
// REWARDS_EVENTS_KEY_FILE). Unset means disabled. Never returns nil.
func FromEnv(source string) *Client {
	key := os.Getenv("REWARDS_EVENTS_KEY")
	if f := os.Getenv("REWARDS_EVENTS_KEY_FILE"); f != "" {
		if b, err := os.ReadFile(f); err == nil {
			key = strings.TrimSpace(string(b))
		}
	}
	return New(Config{URL: os.Getenv("REWARDS_EVENTS_URL"), Key: key, Source: source})
}

// New starts the background sender. With no URL or Key the client is disabled.
func New(cfg Config) *Client {
	c := &Client{cfg: cfg, done: make(chan struct{})}
	if cfg.URL == "" || cfg.Key == "" {
		c.closed.Store(true)
		return c
	}
	if c.cfg.QueueSize <= 0 {
		c.cfg.QueueSize = 2048
	}
	if c.cfg.Batch <= 0 || c.cfg.Batch > 100 {
		c.cfg.Batch = 50
	}
	if c.cfg.Flush <= 0 {
		c.cfg.Flush = 2 * time.Second
	}
	if c.cfg.MaxAge <= 0 {
		c.cfg.MaxAge = 6 * time.Hour
	}
	if c.cfg.HTTP == nil {
		c.cfg.HTTP = &http.Client{Timeout: 10 * time.Second}
	}
	c.cfg.URL = strings.TrimRight(c.cfg.URL, "/")
	c.q = make(chan wireEvent, c.cfg.QueueSize)
	c.wg.Add(1)
	go c.run()
	return c
}

// Enabled reports whether events are being delivered anywhere.
func (c *Client) Enabled() bool { return c != nil && !c.closed.Load() }

// Dropped is how many events were lost to a full queue or old age.
func (c *Client) Dropped() int64 {
	if c == nil {
		return 0
	}
	return c.dropped.Load()
}

// Event queues one event. It never blocks, never panics and never reports failure.
func (c *Client) Event(e Event) {
	if !c.Enabled() || e.ID == "" || e.Metric == "" {
		return
	}
	if e.At.IsZero() {
		e.At = time.Now()
	}
	w := wireEvent{EventID: e.ID, Source: c.cfg.Source, Metric: e.Metric, Delta: e.Delta, AccountID: e.Account,
		Identity: e.Player, Keys: e.Keys, KeyIdentity: e.KeyPlayers, OccurredAt: e.At.UTC()}
	if w.Delta == 0 && len(e.Keys) == 0 && len(e.KeyPlayers) == 0 {
		w.Delta = 1
	}
	defer func() { _ = recover() }() // a send on a channel closed by a racing Close
	select {
	case c.q <- w:
	default:
		c.dropped.Add(1)
	}
}

// Close stops accepting events and tries to deliver what is queued until ctx ends.
func (c *Client) Close(ctx context.Context) {
	if c == nil || c.q == nil || !c.closed.CompareAndSwap(false, true) {
		return
	}
	close(c.done)
	fin := make(chan struct{})
	go func() { c.wg.Wait(); close(fin) }()
	select {
	case <-fin:
	case <-ctx.Done():
	}
}

func (c *Client) run() {
	defer c.wg.Done()
	t := time.NewTicker(c.cfg.Flush)
	defer t.Stop()
	var buf []wireEvent
	backoff := time.Duration(0)
	var next time.Time
	flush := func(final bool) {
		for len(buf) > 0 && (final || !time.Now().Before(next)) {
			n := min(len(buf), c.cfg.Batch)
			if c.post(buf[:n]) {
				c.sent.Add(int64(n))
				buf = buf[n:]
				backoff = 0
				continue
			}
			if final {
				c.dropped.Add(int64(len(buf)))
				buf = nil
				return
			}
			backoff = min(max(backoff*2, time.Second), time.Minute)
			next = time.Now().Add(backoff)
			cut := time.Now().Add(-c.cfg.MaxAge)
			kept := buf[:0]
			for _, w := range buf {
				if w.OccurredAt.After(cut) {
					kept = append(kept, w)
				} else {
					c.dropped.Add(1)
				}
			}
			buf = kept
			return
		}
	}
	for {
		select {
		case w := <-c.q:
			buf = append(buf, w)
			if len(buf) >= c.cfg.Batch {
				flush(false)
			}
		case <-t.C:
			flush(false)
		case <-c.done:
			for {
				select {
				case w := <-c.q:
					buf = append(buf, w)
					continue
				default:
				}
				break
			}
			flush(true)
			return
		}
	}
}

// post delivers one batch. true means the batch is finished with (delivered, or refused for a
// reason a retry cannot fix); false means try again later.
func (c *Client) post(batch []wireEvent) bool {
	body, err := json.Marshal(map[string]any{"events": batch})
	if err != nil {
		return true
	}
	req, err := http.NewRequest(http.MethodPost, c.cfg.URL+"/internal/v1/events", bytes.NewReader(body))
	if err != nil {
		return true
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Internal-Key", c.cfg.Key)
	resp, err := c.cfg.HTTP.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
	switch {
	case resp.StatusCode == 200:
		return true
	case resp.StatusCode == 429 || resp.StatusCode >= 500:
		return false
	default:
		log.Printf("rewards-emit: batch of %d refused with %d; dropped", len(batch), resp.StatusCode)
		c.dropped.Add(int64(len(batch)))
		return true
	}
}
