package kalshi

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

const captureTicker = "KXFEDDECISION-26SEP-H25"

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

	// Keep reading so a client-side close is noticed.
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
	stale   bool
	lastSeq int64
	yes, no map[int]int64
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
	s := bookState{stale: f.Stale(), lastSeq: f.book.lastSeq, yes: maps.Clone(f.book.yes), no: maps.Clone(f.book.no)}
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
	var f Feed
	for i, line := range lines {
		if _, err := f.Handle(line); err != nil {
			t.Fatalf("clean replay, line %d: %v", i+1, err)
		}
	}
	bid, ask, ok := f.Best()
	return bookState{lastSeq: f.book.lastSeq, yes: f.book.yes, no: f.book.no}, bestReport{bid, ask, ok}
}

func matches(got, want bookState) bool {
	return !got.stale && got.lastSeq == want.lastSeq && maps.Equal(got.yes, want.yes) && maps.Equal(got.no, want.no)
}

// TestClientRecoversFromSeqGapAndReconnect plays three connections:
//
//  1. the capture with one delta cut out, so the book hits a seq gap and
//     the client must drop the connection and resync;
//  2. the first half of the capture, then the server hangs up mid-stream;
//  3. the whole capture.
//
// The client must end up with exactly the book a clean replay produces,
// having resubscribed on every connection and reported the book as
// untrusted in between.
func TestClientRecoversFromSeqGapAndReconnect(t *testing.T) {
	lines := captureLines(t)
	want, wantBest := cleanState(t, lines)

	gap := len(lines) / 2
	for messageType(t, lines[gap]) != "orderbook_delta" {
		gap++
	}
	withGap := append(append([][]byte(nil), lines[:gap]...), lines[gap+1:]...)

	v := &fakeVenue{scripts: []func(*websocket.Conn){
		func(conn *websocket.Conn) { send(conn, withGap) },
		func(conn *websocket.Conn) {
			send(conn, lines[:len(lines)/2])
			conn.Close()
		},
		func(conn *websocket.Conn) { send(conn, lines) },
	}}
	rec := &recorder{}
	c := &Client{Ticker: captureTicker, OnBest: rec.onBest, Logf: rec.logf, onFeed: rec.onFeed}
	stop := runClient(t, v, c)

	waitFor(t, 10*time.Second, "the third connection's book to match a clean replay", func() bool {
		_, _, state := rec.snapshot()
		return v.conns.Load() == 3 && matches(state, want)
	})
	stop()

	logs, bests, _ := rec.snapshot()
	t.Logf("log:\n  %s", strings.Join(logs, "\n  "))

	// Session 1 ended on the seq gap, session 2 on the hang-up.
	var reasons []string
	for _, line := range logs {
		if strings.Contains(line, "resync (") {
			reasons = append(reasons, line[strings.Index(line, "(")+1:strings.Index(line, ")")])
		}
	}
	if len(reasons) < 2 || reasons[0] != ReasonSeqGap || reasons[1] != ReasonReconnect {
		t.Fatalf("resync reasons = %v, want [%s %s ...]", reasons, ReasonSeqGap, ReasonReconnect)
	}

	for i, sub := range v.subscriptions() {
		if !strings.Contains(sub, `"orderbook_delta"`) || !strings.Contains(sub, captureTicker) {
			t.Fatalf("connection %d subscribe = %s, want orderbook_delta for %s", i+1, sub, captureTicker)
		}
	}

	// Trusted, then untrusted at least once (the gap, the hang-up), then
	// trusted again at the clean replay's top of book. stop() also
	// reports untrusted on shutdown, so look at the last trusted report.
	var sawTrusted, sawUntrustedAfter bool
	var lastTrusted bestReport
	for _, b := range bests {
		switch {
		case b.ok:
			sawTrusted = true
			lastTrusted = b
		case sawTrusted:
			sawUntrustedAfter = true
		}
	}
	if !sawTrusted || !sawUntrustedAfter {
		t.Fatalf("OnBest reports %v: want the book reported untrusted between sessions", bests)
	}
	if lastTrusted != wantBest {
		t.Fatalf("last trusted best = %+v, want clean replay's %+v", lastTrusted, wantBest)
	}
}

// TestClientSnapshotTimeout covers a subscription that never produces a
// snapshot: the client must give up on the connection and try again
// rather than sit on an untrusted book forever.
func TestClientSnapshotTimeout(t *testing.T) {
	lines := captureLines(t)
	want, _ := cleanState(t, lines)

	v := &fakeVenue{scripts: []func(*websocket.Conn){
		nil, // subscribes, then silence
		func(conn *websocket.Conn) { send(conn, lines) },
	}}
	rec := &recorder{}
	c := &Client{Ticker: captureTicker, Logf: rec.logf, onFeed: rec.onFeed, snapshotTimeout: 200 * time.Millisecond}
	stop := runClient(t, v, c)

	waitFor(t, 10*time.Second, "the second connection's book to match a clean replay", func() bool {
		_, _, state := rec.snapshot()
		return v.conns.Load() == 2 && matches(state, want)
	})
	stop()

	logs, _, _ := rec.snapshot()
	if !strings.Contains(strings.Join(logs, "\n"), "resync ("+ReasonSnapshotTimeout+")") {
		t.Fatalf("no %s resync in log:\n  %s", ReasonSnapshotTimeout, strings.Join(logs, "\n  "))
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
		if got := backoff(tt.attempt, high); got >= tt.max || got < tt.max-time.Nanosecond {
			t.Errorf("backoff(%d) at highest jitter = %v, want just under %v", tt.attempt, got, tt.max)
		}
	}
}
