package opens

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/lucasjohn05/pmde/internal/kalshi"
	"github.com/lucasjohn05/pmde/internal/metrics"
	"github.com/lucasjohn05/pmde/internal/storage"
)

const openTicker = "KXNHL2PTOTAL-26NOV12MTLBOS-4"

// openCaptureLines is testdata/kalshi_open_capture.jsonl: a real
// session that subscribed before its market opened.
func openCaptureLines(t *testing.T) [][]byte {
	t.Helper()
	data, err := os.ReadFile("../../testdata/kalshi_open_capture.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	var lines [][]byte
	sc := bufio.NewScanner(bytes.NewReader(data))
	for sc.Scan() {
		if line := bytes.TrimSpace(sc.Bytes()); len(line) > 0 {
			lines = append(lines, append([]byte(nil), line...))
		}
	}
	return lines
}

// wsVenue is a websocket server that, per connection, waits for the
// subscribe message and then runs script with the subscribed tickers.
type wsVenue struct {
	script func(conn *websocket.Conn, tickers []string)
	conns  atomic.Int32
	closed atomic.Int32 // connections the client hung up on

	mu   sync.Mutex
	subs [][]string // the tickers of each subscribe, in order
}

func (v *wsVenue) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()
	v.conns.Add(1)
	_, raw, err := conn.ReadMessage()
	if err != nil {
		return
	}
	var sub struct {
		Params struct {
			MarketTickers []string `json:"market_tickers"`
		} `json:"params"`
	}
	json.Unmarshal(raw, &sub)
	v.mu.Lock()
	v.subs = append(v.subs, sub.Params.MarketTickers)
	v.mu.Unlock()
	gone := make(chan struct{})
	go func() {
		defer close(gone)
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}()
	if v.script != nil {
		v.script(conn, sub.Params.MarketTickers)
	}
	<-gone
	v.closed.Add(1)
}

func (v *wsVenue) subscriptions() [][]string {
	v.mu.Lock()
	defer v.mu.Unlock()
	return append([][]string(nil), v.subs...)
}

func wsSnap(seq int, ticker string) []byte {
	return []byte(fmt.Sprintf(`{"type":"orderbook_snapshot","sid":1,"seq":%d,"msg":{"market_ticker":%q}}`, seq, ticker))
}

func wsDelta(seq int, ticker, side, price, delta string) []byte {
	return []byte(fmt.Sprintf(`{"type":"orderbook_delta","sid":1,"seq":%d,"msg":{"market_ticker":%q,"price_dollars":%q,"delta_fp":%q,"side":%q,"ts_ms":1794528712733}}`,
		seq, ticker, price, delta, side))
}

func wsSend(conn *websocket.Conn, lines ...[]byte) {
	for _, l := range lines {
		conn.WriteMessage(websocket.TextMessage, l)
	}
}

type captureRig struct {
	c    *Capturer
	reg  *prometheus.Registry
	dir  string
	stop func()
	mu   sync.Mutex
	logs []string
}

// startCapturer runs a Recorder and a Capturer against v.
func startCapturer(t *testing.T, v *wsVenue, cfg Config) *captureRig {
	t.Helper()
	srv := httptest.NewServer(v)
	rig := &captureRig{reg: prometheus.NewRegistry(), dir: t.TempDir()}
	m := metrics.NewOpens(rig.reg)
	logf := func(f string, a ...any) {
		rig.mu.Lock()
		rig.logs = append(rig.logs, fmt.Sprintf(f, a...))
		rig.mu.Unlock()
	}
	recorder := NewRecorder(rig.dir, m, logf)
	rig.c = &Capturer{
		URL: "ws" + strings.TrimPrefix(srv.URL, "http"), Cfg: cfg, Schedule: NewSchedule(),
		Recorder: recorder, Dir: rig.dir, Metrics: m, Logf: logf,
	}
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	recDone := make(chan struct{})
	go func() { defer close(recDone); recorder.Run(ctx) }()
	wg.Go(func() { rig.c.Run(ctx) })
	stopped := false
	rig.stop = func() {
		if stopped {
			return
		}
		stopped = true
		cancel()
		wg.Wait()
		<-recDone
		srv.Close()
	}
	t.Cleanup(rig.stop)
	return rig
}

func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func captureConfig(lead, window time.Duration, max int) Config {
	c := DefaultConfig()
	c.Lead, c.Window, c.MaxConcurrent = lead, window, max
	return c
}

func (rig *captureRig) captures(t *testing.T) []CaptureRecord {
	t.Helper()
	recs, err := storage.ReadJSONL[CaptureRecord](filepath.Join(rig.dir, "captures.jsonl"))
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	return recs
}

// A market that comes due is subscribed before its open, every book
// event is recorded with the identity of the market, and the
// subscription ends when the window does.
func TestCaptureRecordsTheOpen(t *testing.T) {
	lines := openCaptureLines(t)
	open := time.Now().Add(400 * time.Millisecond)
	var subscribedAt atomic.Int64
	v := &wsVenue{script: func(conn *websocket.Conn, tickers []string) {
		subscribedAt.Store(time.Now().UnixMilli())
		// The ack and the empty snapshot at once; the deltas at the open.
		wsSend(conn, lines[:2]...)
		time.Sleep(time.Until(open.Add(20 * time.Millisecond)))
		wsSend(conn, lines[2:]...)
		wsSend(conn, wsDelta(9, tickers[0], "yes", "0.9200", "15.00"))
	}}
	rig := startCapturer(t, v, captureConfig(300*time.Millisecond, 400*time.Millisecond, 4))
	rig.c.Schedule.Update("KXNHL2PTOTAL", []kalshi.Market{{
		Ticker: openTicker, Event: "KXNHL2PTOTAL-26NOV12MTLBOS", Rung: "Over 3.5 goals",
		StrikeType: "greater", FloorStrike: "3.5", Open: open,
	}})

	waitFor(t, 10*time.Second, "the capture to finish", func() bool { return len(rig.captures(t)) == 1 })
	waitFor(t, 5*time.Second, "the connection to be closed", func() bool { return v.closed.Load() == 1 })
	rig.stop()

	if at := subscribedAt.Load(); at > open.UnixMilli() || at < open.Add(-350*time.Millisecond).UnixMilli() {
		t.Errorf("subscribed %dms before the open, want within the 300ms lead", open.UnixMilli()-at)
	}
	cap := rig.captures(t)[0]
	if cap.Market != openTicker || cap.Series != "KXNHL2PTOTAL" || cap.Event != "KXNHL2PTOTAL-26NOV12MTLBOS" ||
		cap.OpenMs != open.UnixMilli() || cap.Events != 9 || cap.Resyncs != 0 || cap.Rungs != 1 || !cap.Complete {
		t.Errorf("capture record %+v", cap)
	}
	// EndMs is truncated to the millisecond, so allow one either side.
	if end := time.UnixMilli(cap.EndMs); end.Before(open.Add(399*time.Millisecond)) || end.After(open.Add(2*time.Second)) {
		t.Errorf("capture ended %v after the open, want just past the 400ms window", end.Sub(open))
	}

	day := open.UTC().Format("2006-01-02")
	recs, err := storage.ReadJSONL[EventRecord](filepath.Join(rig.dir, "events", "KXNHL2PTOTAL", day+".jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 9 {
		t.Fatalf("%d event records, want 9", len(recs))
	}
	for i, r := range recs {
		if r.Market != openTicker || r.Series != "KXNHL2PTOTAL" || r.Event != "KXNHL2PTOTAL-26NOV12MTLBOS" ||
			r.Rung != "Over 3.5 goals" || r.StrikeType != "greater" || r.Floor != "3.5" || r.OpenMs != open.UnixMilli() ||
			r.Seq != int64(i+1) || r.RecvMs == 0 || r.Stale != "" {
			t.Errorf("record %d: %+v", i, r)
		}
	}
	if s := recs[0]; s.Type != "snapshot" || s.TsMs != 0 || s.Bid != nil || s.Ask != nil || s.RecvMs > open.UnixMilli() {
		t.Errorf("snapshot record %+v", s)
	}
	if d := recs[1]; d.Type != "delta" || d.TsMs != 1794528712733 || d.Side != "no" || *d.Price != 100 || *d.Delta != 1500 ||
		d.Bid != nil || d.Ask == nil || *d.Ask != 9900 || *d.AskSz != 1500 || d.RecvMs < open.UnixMilli() {
		t.Errorf("first delta record %+v", d)
	}
	if d := recs[8]; d.Bid == nil || *d.Bid != 9200 || *d.BidSz != 1500 || *d.Ask != 9400 || *d.AskSz != 15000 {
		t.Errorf("last record %+v", d)
	}
	if n := metricValue(t, rig.reg, "opens_events_total"); n != 9 {
		t.Errorf("opens_events_total = %v", n)
	}
	if n := metricValue(t, rig.reg, "opens_captures_total"); n != 1 {
		t.Errorf("opens_captures_total = %v", n)
	}
	if n := metricValue(t, rig.reg, "opens_captures_active"); n != 0 {
		t.Errorf("opens_captures_active = %v after the capture", n)
	}
}

// The rungs of an event share one connection and one subscription, with
// one seq across them; each rung still gets its own records.
func TestCaptureEventOnOneConnection(t *testing.T) {
	open := time.Now().Add(400 * time.Millisecond)
	v := &wsVenue{script: func(conn *websocket.Conn, tickers []string) {
		for i, tk := range tickers {
			wsSend(conn, wsSnap(i+1, tk))
		}
		time.Sleep(time.Until(open.Add(20 * time.Millisecond)))
		// The maker walks the ladder: an ask and a bid on each rung,
		// interleaved on the shared seq.
		seq := len(tickers)
		for _, tk := range tickers {
			seq++
			wsSend(conn, wsDelta(seq, tk, "no", "0.5600", "250.00"))
		}
		for _, tk := range tickers {
			seq++
			wsSend(conn, wsDelta(seq, tk, "yes", "0.3600", "250.00"))
		}
	}}
	rig := startCapturer(t, v, captureConfig(300*time.Millisecond, 300*time.Millisecond, 1))
	// The third rung opens 100ms after the others: still the same
	// capture, which runs until the window after the last open.
	rig.c.Schedule.Update("S", []kalshi.Market{
		{Ticker: "S-E1-T1", Event: "S-E1", Open: open},
		{Ticker: "S-E1-T2", Event: "S-E1", Open: open},
		{Ticker: "S-E1-T3", Event: "S-E1", Open: open.Add(100 * time.Millisecond)},
	})
	waitFor(t, 10*time.Second, "the capture to finish", func() bool { return len(rig.captures(t)) == 3 })
	rig.stop()

	if n := v.conns.Load(); n != 1 {
		t.Fatalf("%d connections for a three-rung event, want 1", n)
	}
	if subs := v.subscriptions(); fmt.Sprint(subs) != "[[S-E1-T1 S-E1-T2 S-E1-T3]]" {
		t.Errorf("subscriptions %v", subs)
	}
	for _, c := range rig.captures(t) {
		if c.Event != "S-E1" || c.Rungs != 3 || c.Events != 3 || c.Resyncs != 0 || !c.Complete {
			t.Errorf("capture record %+v", c)
		}
		if end := time.UnixMilli(c.EndMs); end.Before(open.Add(399 * time.Millisecond)) {
			t.Errorf("%s: capture ended %v after the first open, want the window after the last (400ms)", c.Market, end.Sub(open))
		}
	}
	recs, err := storage.ReadJSONL[EventRecord](filepath.Join(rig.dir, "events", "S", open.UTC().Format("2006-01-02")+".jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	seqs := map[string][]int64{}
	for _, r := range recs {
		seqs[r.Market] = append(seqs[r.Market], r.Seq)
		if r.Type == "delta" && r.Side == "yes" && (r.Bid == nil || *r.Bid != 3600 || *r.Ask != 4400 || *r.BidSz != 25000) {
			t.Errorf("%s two-sided record: %+v", r.Market, r)
		}
	}
	if fmt.Sprint(seqs) != "map[S-E1-T1:[1 4 7] S-E1-T2:[2 5 8] S-E1-T3:[3 6 9]]" {
		t.Errorf("seqs by market %v", seqs)
	}
	if n := metricValue(t, rig.reg, "opens_captures_total"); n != 1 {
		t.Errorf("opens_captures_total = %v, want 1 (connections)", n)
	}
	if n := metricValue(t, rig.reg, "opens_captured_markets_total"); n != 3 {
		t.Errorf("opens_captured_markets_total = %v, want 3", n)
	}
}

// max_concurrent counts connections. An event past the cap is skipped
// whole, never in part, and so is one that comes due after it opened.
func TestCaptureSkipsWholeEvents(t *testing.T) {
	v := &wsVenue{script: func(conn *websocket.Conn, tickers []string) {
		for i, tk := range tickers {
			wsSend(conn, wsSnap(i+1, tk))
		}
	}}
	rig := startCapturer(t, v, captureConfig(300*time.Millisecond, 200*time.Millisecond, 2))
	open := time.Now().Add(350 * time.Millisecond)
	var ms []kalshi.Market
	// Three events of three rungs each opening together, and one that
	// opened a second ago: nine markets due, two connections allowed.
	for _, e := range []string{"S-E1", "S-E2", "S-E3"} {
		for _, r := range []string{"-A", "-B", "-C"} {
			ms = append(ms, kalshi.Market{Ticker: e + r, Event: e, Open: open})
		}
	}
	ms = append(ms, kalshi.Market{Ticker: "S-OLD-A", Event: "S-OLD", Open: time.Now().Add(-time.Second)},
		kalshi.Market{Ticker: "S-OLD-B", Event: "S-OLD", Open: time.Now().Add(-time.Second)})
	rig.c.Schedule.Update("S", ms)
	waitFor(t, 10*time.Second, "two events to finish", func() bool { return len(rig.captures(t)) == 6 })
	rig.stop()

	if n := metricValue(t, rig.reg, "opens_skipped_total", "reason", "concurrency"); n != 3 {
		t.Errorf("skipped for concurrency = %v, want 3 (one event of three rungs)", n)
	}
	if n := metricValue(t, rig.reg, "opens_skipped_total", "reason", "late"); n != 2 {
		t.Errorf("skipped late = %v, want 2", n)
	}
	if n := v.conns.Load(); n != 2 {
		t.Errorf("%d connections, want 2 (the cap)", n)
	}
	// Soonest first, ties by event: E1 and E2 got the slots, whole.
	byEvent := map[string]int{}
	for _, c := range rig.captures(t) {
		byEvent[c.Event]++
		if !c.Complete {
			t.Errorf("%s incomplete", c.Market)
		}
	}
	if fmt.Sprint(byEvent) != "map[S-E1:3 S-E2:3]" {
		t.Errorf("captured rungs by event %v, want E1 and E2 whole and nothing of E3", byEvent)
	}
}

// A gap in the shared seq loses every rung at once: each record shows
// the hole, each rung is flagged incomplete, and the resync brings a
// fresh snapshot for each.
func TestCaptureResyncMarksEveryRung(t *testing.T) {
	var n atomic.Int32
	v := &wsVenue{script: func(conn *websocket.Conn, tickers []string) {
		a, b := tickers[0], tickers[1]
		wsSend(conn, wsSnap(1, a), wsSnap(2, b))
		if n.Add(1) == 1 {
			wsSend(conn, wsDelta(3, a, "yes", "0.4000", "10.00"), wsDelta(5, a, "yes", "0.4100", "10.00")) // seq 4 is missing
			return
		}
		wsSend(conn, wsDelta(3, b, "yes", "0.2000", "10.00"))
	}}
	rig := startCapturer(t, v, captureConfig(300*time.Millisecond, 2*time.Second, 2))
	open := time.Now().Add(350 * time.Millisecond)
	rig.c.Schedule.Update("S", []kalshi.Market{{Ticker: "S-E-A", Event: "S-E", Open: open}, {Ticker: "S-E-B", Event: "S-E", Open: open}})
	waitFor(t, 15*time.Second, "the capture to finish", func() bool { return len(rig.captures(t)) == 2 })
	rig.stop()

	for _, c := range rig.captures(t) {
		if c.Resyncs != 1 || c.Complete {
			t.Errorf("capture record %+v, want 1 resync and incomplete", c)
		}
	}
	recs, err := storage.ReadJSONL[EventRecord](filepath.Join(rig.dir, "events", "S", open.UTC().Format("2006-01-02")+".jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	shape := map[string][]string{}
	for _, r := range recs {
		shape[r.Market] = append(shape[r.Market], strings.TrimSuffix(fmt.Sprintf("%s/%d/%s", r.Type, r.Seq, r.Stale), "/"))
	}
	want := "map[S-E-A:[snapshot/1 delta/3 delta/5/seq_gap snapshot/1] S-E-B:[snapshot/2 stale/0/seq_gap snapshot/2 delta/3]]"
	if fmt.Sprint(shape) != want {
		t.Errorf("records\n got %v\nwant %s", shape, want)
	}
	if n := metricValue(t, rig.reg, "opens_capture_resyncs_total", "reason", "seq_gap"); n != 2 {
		t.Errorf("seq_gap resyncs = %v, want 2 (one per rung)", n)
	}
	if n := v.conns.Load(); n != 2 {
		t.Errorf("%d connections, want 2 (the capture and its resync)", n)
	}
}

// The Recorder never drops: with its queue full, Send blocks until Run
// makes room, and the wait is counted.
func TestRecorderSendBlocksWhenFull(t *testing.T) {
	reg := prometheus.NewRegistry()
	dir := t.TempDir()
	r := NewRecorder(dir, metrics.NewOpens(reg), nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rec := EventRecord{Market: "M", Series: "S", OpenMs: time.Date(2026, 11, 12, 15, 0, 0, 0, time.UTC).UnixMilli(), Type: "delta"}
	for range RecorderQueueSize {
		r.Send(ctx, rec)
	}
	sent := make(chan struct{})
	go func() { r.Send(ctx, rec); close(sent) }()
	select {
	case <-sent:
		t.Fatal("Send returned with the queue full and nothing reading it")
	case <-time.After(50 * time.Millisecond):
	}
	done := make(chan struct{})
	go func() { defer close(done); r.Run(ctx) }()
	select {
	case <-sent:
	case <-time.After(5 * time.Second):
		t.Fatal("Send still blocked with Run reading")
	}
	cancel()
	<-done
	recs, err := storage.ReadJSONL[EventRecord](filepath.Join(dir, "events", "S", "2026-11-12.jsonl"))
	if err != nil || len(recs) != RecorderQueueSize+1 {
		t.Fatalf("%d records on disk, want %d (%v)", len(recs), RecorderQueueSize+1, err)
	}
	if n := metricValue(t, reg, "opens_writer_blocked_seconds_total"); n < 0.04 {
		t.Errorf("opens_writer_blocked_seconds_total = %v, want at least the 50ms wait", n)
	}
}
