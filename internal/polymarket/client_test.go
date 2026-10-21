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
	"github.com/prometheus/client_golang/prometheus"

	"github.com/lucasjohn05/pmde/internal/metrics"
	"github.com/lucasjohn05/pmde/internal/storage"
	"github.com/lucasjohn05/pmde/internal/tick"
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
	ticks []tick.Tick
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

func (r *recorder) onTick(t tick.Tick) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.bests = append(r.bests, bestReport{t.BidBps, t.AskBps, t.OK})
	r.ticks = append(r.ticks, t)
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

// metricValue reads one series from g: a counter's or gauge's value, or a
// histogram's sample count. labels are name/value pairs.
func metricValue(t *testing.T, g prometheus.Gatherer, name string, labels ...string) float64 {
	t.Helper()
	families, err := g.Gather()
	if err != nil {
		t.Fatalf("gathering metrics: %v", err)
	}
	for _, mf := range families {
		if mf.GetName() != name {
			continue
		}
	series:
		for _, m := range mf.GetMetric() {
			have := map[string]string{}
			for _, l := range m.GetLabel() {
				have[l.GetName()] = l.GetValue()
			}
			for i := 0; i+1 < len(labels); i += 2 {
				if have[labels[i]] != labels[i+1] {
					continue series
				}
			}
			switch {
			case m.GetCounter() != nil:
				return m.GetCounter().GetValue()
			case m.GetGauge() != nil:
				return m.GetGauge().GetValue()
			case m.GetHistogram() != nil:
				return float64(m.GetHistogram().GetSampleCount())
			}
		}
	}
	t.Fatalf("no series %s%v", name, labels)
	return 0
}

// checkMetrics compares series of the given venue against want, keyed by
// "name" or "name/reason".
func checkMetrics(t *testing.T, g prometheus.Gatherer, venue string, want map[string]float64) {
	t.Helper()
	for key, w := range want {
		name, reason, _ := strings.Cut(key, "/")
		labels := []string{"venue", venue}
		if reason != "" {
			labels = append(labels, "reason", reason)
		}
		if got := metricValue(t, g, name, labels...); got != w {
			t.Errorf("%s = %v, want %v", key, got, w)
		}
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
	reg := prometheus.NewRegistry()
	c := &Client{AssetID: captureAssetID, OnTick: rec.onTick, Logf: rec.logf, onFeed: rec.onFeed,
		Metrics: metrics.New(reg).Market("polymarket", captureAssetID)}
	stop := runClient(t, v, c)

	waitFor(t, 10*time.Second, "the second connection's book to match a clean replay", func() bool {
		_, _, state := rec.snapshot()
		return v.conns.Load() == 2 && matches(state, want)
	})
	checkMetrics(t, reg, "polymarket", map[string]float64{"book_stale": 0})
	stop()

	checkMetrics(t, reg, "polymarket", map[string]float64{
		"book_stale":                       1,
		"reconnects_total":                 1,
		"resyncs_total/" + ReasonReconnect: 1,
		"resyncs_total/" + ReasonDropped:   0,
		"resyncs_total/" + ReasonInvalid:   0,
		"dropped_messages_total":           0,
	})
	if n := metricValue(t, reg, "messages_total", "venue", "polymarket"); n < float64(len(lines)) {
		t.Errorf("messages_total = %v, want at least the second connection's %d", n, len(lines))
	}
	if n := metricValue(t, reg, "ingest_lag_seconds", "venue", "polymarket"); n == 0 {
		t.Errorf("ingest_lag_seconds has no samples")
	}

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
		t.Fatalf("OnTick reports %v: want the book reported untrusted across the reconnect", bests)
	}

	// Every report is a Tick for this market, stamped with a time and,
	// since every Polymarket event here carries a timestamp, a venue
	// time. A trusted one carries the sizes resting at its prices; the
	// last one matches the clean replay's book.
	rec.mu.Lock()
	defer rec.mu.Unlock()
	var lastOK tick.Tick
	for _, tk := range rec.ticks {
		if tk.Venue != tick.Polymarket || tk.Market != captureAssetID || tk.Time.IsZero() {
			t.Fatalf("Tick %+v: want venue %s, market %s, a non-zero time", tk, tick.Polymarket, captureAssetID)
		}
		if tk.OK && (tk.BidSize <= 0 || tk.AskSize <= 0 || tk.VenueTime.IsZero()) {
			t.Fatalf("trusted Tick %+v without sizes or a venue time", tk)
		}
		if tk.OK {
			lastOK = tk
		}
	}
	if lastOK.BidSize != want.bids[lastOK.BidBps] || lastOK.AskSize != want.asks[lastOK.AskBps] {
		t.Fatalf("last trusted Tick sizes %d/%d, want the clean replay's %d/%d",
			lastOK.BidSize, lastOK.AskSize, want.bids[lastOK.BidBps], want.asks[lastOK.AskBps])
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
	reg := prometheus.NewRegistry()
	c := &Client{
		AssetID: captureAssetID,
		Logf:    rec.logf,
		Metrics: metrics.New(reg).Market("polymarket", captureAssetID),
		onQueue: func(q *Queue) { queue.CompareAndSwap(nil, q) }, // the first session's
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

	// Every drop the reader made is counted, and the book's loss is a
	// resync for "dropped", not for the reconnect that followed.
	drops := queue.Load().Drops()
	if drops < burst-QueueSize {
		t.Fatalf("first session dropped %d, want at least %d", drops, burst-QueueSize)
	}
	checkMetrics(t, reg, "polymarket", map[string]float64{
		"dropped_messages_total":           float64(drops),
		"resyncs_total/" + ReasonDropped:   1,
		"resyncs_total/" + ReasonReconnect: 0,
		"reconnects_total":                 1,
	})
	if n := metricValue(t, reg, "messages_total", "venue", "polymarket"); n < float64(1+burst+len(lines)) {
		t.Errorf("messages_total = %v, want at least %d", n, 1+burst+len(lines))
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

// TestSlowStorageChain runs the slow-disk failure chain end to end: the
// storage writer stops draining (a stuck disk), so its queue fills and
// the client's OnTick blocks in Send; the book goroutine is stuck behind
// it, so the reader->book queue fills and drops; when the disk recovers,
// the book goroutine sees the drop, the book goes stale, and the client
// resyncs from a fresh book event. Every link shows up in the metrics.
func TestSlowStorageChain(t *testing.T) {
	lines := captureLines(t)
	want, _ := cleanState(t, lines)
	book := firstBookEvent(t)

	reg := prometheus.NewRegistry()
	m := metrics.New(reg)
	w, err := storage.NewWriter(t.TempDir(), m, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for i := 0; i < storage.QueueSize; i++ { // the disk is stuck: the writer's queue is full
		w.Send(ctx, tick.Tick{Venue: tick.Polymarket, Market: captureAssetID, Time: time.Now()})
	}

	var queue atomic.Pointer[Queue]
	rec := &recorder{}
	c := &Client{
		AssetID: captureAssetID,
		Logf:    rec.logf,
		Metrics: m.Market(tick.Polymarket, captureAssetID),
		OnTick:  func(tk tick.Tick) { w.Send(ctx, tk) },
		onQueue: func(q *Queue) { queue.CompareAndSwap(nil, q) },
		onFeed:  rec.onFeed,
	}

	const burst = QueueSize + 50
	writerDone := make(chan struct{})
	v := &fakeVenue{scripts: []func(*websocket.Conn){
		func(conn *websocket.Conn) {
			// The book event syncs the book; reporting it blocks in Send.
			send(conn, [][]byte{book})
			for i := 0; i < burst; i++ {
				send(conn, [][]byte{priceChange(fmt.Sprintf("0.%02d", 10+i%50), "1")})
			}
			deadline := time.Now().Add(5 * time.Second)
			for (queue.Load() == nil || queue.Load().Drops() < burst-QueueSize) && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			// The disk recovers.
			go func() {
				defer close(writerDone)
				w.Run(ctx)
			}()
		},
		func(conn *websocket.Conn) { send(conn, lines) },
	}}
	stop := runClient(t, v, c)

	waitFor(t, 10*time.Second, "the second connection's book to match a clean replay", func() bool {
		_, _, state := rec.snapshot()
		return v.conns.Load() == 2 && matches(state, want)
	})
	stop()
	cancel()
	<-writerDone // the writer closes its files

	logs, _, _ := rec.snapshot()
	if reasons := resyncReasons(logs); len(reasons) < 1 || reasons[0] != ReasonDropped {
		t.Fatalf("resync reasons = %v, want [%s ...]\n  %s", reasons, ReasonDropped, strings.Join(logs, "\n  "))
	}
	checkMetrics(t, reg, "polymarket", map[string]float64{"resyncs_total/" + ReasonDropped: 1})
	if n := metricValue(t, reg, "dropped_messages_total", "venue", "polymarket"); n < burst-QueueSize {
		t.Errorf("dropped_messages_total = %v, want at least %d", n, burst-QueueSize)
	}
	if s := metricValue(t, reg, "storage_blocked_seconds_total"); s <= 0 {
		t.Errorf("storage_blocked_seconds_total = %v, want the time the book goroutine waited", s)
	}
}
