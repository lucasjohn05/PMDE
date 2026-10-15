package polymarket

import (
	"context"
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// fakeVenue is a websocket server that waits for each connection's
// subscribe message, then plays that connection's script. A connection
// past the last script gets nothing and is held open.
type fakeVenue struct {
	scripts []func(conn *websocket.Conn)

	conns atomic.Int32
	mu    sync.Mutex
	subs  []string
}

func (v *fakeVenue) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()
	n := int(v.conns.Add(1)) - 1

	_, sub, err := conn.ReadMessage()
	if err != nil {
		return
	}
	v.mu.Lock()
	v.subs = append(v.subs, string(sub))
	v.mu.Unlock()

	// Keep reading (and ignoring the client's PINGs) so a client-side
	// close is noticed.
	gone := make(chan struct{})
	go func() {
		defer close(gone)
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}()
	if n < len(v.scripts) && v.scripts[n] != nil {
		v.scripts[n](conn)
	}
	<-gone
}

func (v *fakeVenue) subscriptions() []string {
	v.mu.Lock()
	defer v.mu.Unlock()
	return append([]string(nil), v.subs...)
}

// send writes lines until one fails (the client hung up).
func send(conn *websocket.Conn, lines [][]byte) {
	for _, line := range lines {
		if conn.WriteMessage(websocket.TextMessage, line) != nil {
			return
		}
	}
}

type bookState struct {
	stale      bool
	bids, asks map[int]int64
}

// recorder captures what a Client reports, for assertions.
type recorder struct {
	mu    sync.Mutex
	logs  []string
	bests []bestReport
	state bookState
}

type bestReport struct {
	bid, ask int
	ok       bool
}

func (r *recorder) logf(format string, args ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.logs = append(r.logs, fmt.Sprintf(format, args...))
}

func (r *recorder) onBest(bid, ask int, ok bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.bests = append(r.bests, bestReport{bid, ask, ok})
}

// onFeed copies the session's book state; it runs on the book goroutine.
func (r *recorder) onFeed(f *Feed) {
	s := bookState{stale: f.Stale(), bids: maps.Clone(f.book.bids), asks: maps.Clone(f.book.asks)}
	r.mu.Lock()
	r.state = s
	r.mu.Unlock()
}

func (r *recorder) snapshot() (logs []string, bests []bestReport, state bookState) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.logs...), append([]bestReport(nil), r.bests...), r.state
}

// runClient starts c.Run against v and returns a stop func that cancels it
// and checks that Run returns promptly.
func runClient(t *testing.T, v *fakeVenue, c *Client) (stop func()) {
	t.Helper()
	srv := httptest.NewServer(v)
	c.URL = "ws" + strings.TrimPrefix(srv.URL, "http")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		c.Run(ctx)
	}()
	return func() {
		t.Helper()
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatalf("Run did not return after cancel")
		}
		srv.Close()
	}
}

func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v waiting for %s", timeout, what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// cleanState replays the whole capture into a fresh Feed: the state a
// recovered client must end up in.
func cleanState(t *testing.T, lines [][]byte) (bookState, bestReport) {
	t.Helper()
	f := NewFeed(captureAssetID)
	for i, line := range lines {
		if _, err := f.Handle(line); err != nil {
			t.Fatalf("clean replay, line %d: %v", i+1, err)
		}
	}
	bid, ask, ok := f.Best()
	return bookState{bids: f.book.bids, asks: f.book.asks}, bestReport{bid, ask, ok}
}

func matches(got, want bookState) bool {
	return !got.stale && maps.Equal(got.bids, want.bids) && maps.Equal(got.asks, want.asks)
}

func resyncReasons(logs []string) []string {
	var reasons []string
	for _, line := range logs {
		if strings.Contains(line, "resync (") {
			reasons = append(reasons, line[strings.Index(line, "(")+1:strings.Index(line, ")")])
		}
	}
	return reasons
}

// TestClientRecoversFromReconnect plays two connections: the first sends
// the capture up to a little past its first book event, then the server
// hangs up mid-stream; the second sends the whole capture. The second
// session starts with ~90 price_change events before any book event, so
// it has to ignore those rather than trust them, and end up with exactly
// the book a clean replay produces.
func TestClientRecoversFromReconnect(t *testing.T) {
	lines := captureLines(t)
	want, wantBest := cleanState(t, lines)

	firstBook := 0
	for eventType(t, lines[firstBook]) != "book" {
		firstBook++
	}

	v := &fakeVenue{scripts: []func(*websocket.Conn){
		func(conn *websocket.Conn) {
			send(conn, lines[:firstBook+5])
			conn.Close()
		},
		func(conn *websocket.Conn) { send(conn, lines) },
	}}
	rec := &recorder{}
	c := &Client{AssetID: captureAssetID, OnBest: rec.onBest, Logf: rec.logf, onFeed: rec.onFeed}
	stop := runClient(t, v, c)

	waitFor(t, 10*time.Second, "the second connection's book to match a clean replay", func() bool {
		_, _, state := rec.snapshot()
		return v.conns.Load() == 2 && matches(state, want)
	})
	stop()

	logs, bests, _ := rec.snapshot()
	t.Logf("log:\n  %s", strings.Join(logs, "\n  "))
	if reasons := resyncReasons(logs); len(reasons) < 1 || reasons[0] != ReasonReconnect {
		t.Fatalf("resync reasons = %v, want [%s ...]", reasons, ReasonReconnect)
	}
	for i, sub := range v.subscriptions() {
		if !strings.Contains(sub, captureAssetID) {
			t.Fatalf("connection %d subscribe = %s, want asset %s", i+1, sub, captureAssetID)
		}
	}

	// Trusted, untrusted across the reconnect, then trusted again. The
	// capture's top of book never moves after the first book event, so
	// every trusted report must be the clean replay's.
	var sawTrusted, sawUntrustedAfter bool
	for _, b := range bests {
		switch {
		case b.ok:
			if b != wantBest {
				t.Fatalf("trusted best %+v, want clean replay's %+v", b, wantBest)
			}
			sawTrusted = true
		case sawTrusted:
			sawUntrustedAfter = true
		}
	}
	if !sawTrusted || !sawUntrustedAfter {
		t.Fatalf("OnBest reports %v: want the book reported untrusted across the reconnect", bests)
	}
}

// TestClientResyncsAfterDrop forces a drop through the real reader and
// book goroutines: on the first connection the book goroutine is held up
// right after the book event syncs it, while the server sends a burst
// bigger than the queue. The drop must mark the book stale, and the client
// must drop the connection and rebuild from the second connection's book
// event.
func TestClientResyncsAfterDrop(t *testing.T) {
	lines := captureLines(t)
	want, _ := cleanState(t, lines)
	book := firstBookEvent(t)

	var queue atomic.Pointer[Queue]
	synced := make(chan struct{})
	release := make(chan struct{})
	var holdOnce sync.Once

	rec := &recorder{}
	c := &Client{
		AssetID: captureAssetID,
		Logf:    rec.logf,
		onQueue: func(q *Queue) { queue.Store(q) },
		onFeed: func(f *Feed) {
			rec.onFeed(f)
			if !f.Stale() {
				holdOnce.Do(func() {
					close(synced)
					select {
					case <-release:
					case <-time.After(5 * time.Second):
					}
				})
			}
		},
	}

	const burst = QueueSize + 50
	v := &fakeVenue{scripts: []func(*websocket.Conn){
		func(conn *websocket.Conn) {
			send(conn, [][]byte{book})
			select {
			case <-synced:
			case <-time.After(5 * time.Second):
				return
			}
			for i := 0; i < burst; i++ {
				send(conn, [][]byte{priceChange(fmt.Sprintf("0.%02d", 10+i%50), "1")})
			}
			// Wait until the reader has dropped everything that didn't
			// fit, then let the book goroutine go.
			deadline := time.Now().Add(5 * time.Second)
			for queue.Load().Drops() < burst-QueueSize && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			close(release)
		},
		func(conn *websocket.Conn) { send(conn, lines) },
	}}
	stop := runClient(t, v, c)

	waitFor(t, 10*time.Second, "the second connection's book to match a clean replay", func() bool {
		_, _, state := rec.snapshot()
		return v.conns.Load() == 2 && matches(state, want)
	})
	stop()

	logs, _, _ := rec.snapshot()
	t.Logf("log:\n  %s", strings.Join(logs, "\n  "))
	if reasons := resyncReasons(logs); len(reasons) < 1 || reasons[0] != ReasonDropped {
		t.Fatalf("resync reasons = %v, want [%s ...]", reasons, ReasonDropped)
	}
}

func TestBackoff(t *testing.T) {
	low := func(int64) int64 { return 0 }
	high := func(n int64) int64 { return n - 1 }

	tests := []struct {
		attempt  int
		min, max time.Duration // max is exclusive
	}{
		{0, 250 * time.Millisecond, 500 * time.Millisecond},
		{1, 500 * time.Millisecond, time.Second},
		{3, 2 * time.Second, 4 * time.Second},
		{6, 15 * time.Second, 30 * time.Second}, // 32s window, capped
		{100, 15 * time.Second, 30 * time.Second},
	}
	for _, tt := range tests {
		if got := backoff(tt.attempt, low); got != tt.min {
			t.Errorf("backoff(%d) at lowest jitter = %v, want %v", tt.attempt, got, tt.min)
		}
		if got := backoff(tt.attempt, high); got != tt.max-time.Nanosecond {
			t.Errorf("backoff(%d) at highest jitter = %v, want just under %v", tt.attempt, got, tt.max)
		}
	}
}
