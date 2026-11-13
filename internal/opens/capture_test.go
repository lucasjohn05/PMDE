package opens

import (
	"bufio"
	"bytes"
	"context"
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
// subscribe message and then runs script with the subscribed ticker.
type wsVenue struct {
	script func(conn *websocket.Conn, ticker string)
	conns  atomic.Int32
	closed atomic.Int32 // connections the client hung up on
}

func (v *wsVenue) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()
	v.conns.Add(1)
	_, sub, err := conn.ReadMessage()
	if err != nil {
		return
	}
	ticker := ""
	if i := strings.Index(string(sub), `"market_ticker":"`); i >= 0 {
		rest := string(sub)[i+len(`"market_ticker":"`):]
		ticker = rest[:strings.IndexByte(rest, '"')]
	}
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
		v.script(conn, ticker)
	}
	<-gone
	v.closed.Add(1)
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
// event is recorded with the market's identity, and the subscription
// ends when the window does.
func TestCaptureRecordsTheOpen(t *testing.T) {
	lines := openCaptureLines(t)
	open := time.Now().Add(400 * time.Millisecond)
	var subscribedAt atomic.Int64
	v := &wsVenue{script: func(conn *websocket.Conn, ticker string) {
		subscribedAt.Store(time.Now().UnixMilli())
		// The ack and the empty snapshot at once; the deltas at the open.
		for _, l := range lines[:2] {
			conn.WriteMessage(websocket.TextMessage, l)
		}
		time.Sleep(time.Until(open.Add(20 * time.Millisecond)))
		for _, l := range lines[2:] {
			conn.WriteMessage(websocket.TextMessage, l)
		}
		conn.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf(
			`{"type":"orderbook_delta","sid":1,"seq":9,"msg":{"market_ticker":%q,"price_dollars":"0.9200","delta_fp":"15.00","side":"yes","ts_ms":1794528732000}}`, ticker)))
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
		cap.OpenMs != open.UnixMilli() || cap.Events != 9 || cap.Resyncs != 0 || !cap.Complete {
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

// Opens past the concurrency cap are skipped and counted, and so are
// markets that come due after they opened.
func TestCaptureSkipsPastTheCapAndLateMarkets(t *testing.T) {
	v := &wsVenue{script: func(conn *websocket.Conn, ticker string) {
		conn.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf(
			`{"type":"orderbook_snapshot","sid":1,"seq":1,"msg":{"market_ticker":%q}}`, ticker)))
	}}
	rig := startCapturer(t, v, captureConfig(300*time.Millisecond, 200*time.Millisecond, 2))
	open := time.Now().Add(350 * time.Millisecond)
	rig.c.Schedule.Update("S", []kalshi.Market{
		{Ticker: "S-A", Open: open}, {Ticker: "S-B", Open: open}, {Ticker: "S-C", Open: open}, {Ticker: "S-D", Open: open},
		{Ticker: "S-LATE", Open: time.Now().Add(-time.Second)},
	})
	waitFor(t, 10*time.Second, "two captures to finish", func() bool { return len(rig.captures(t)) == 2 })
	rig.stop()

	if n := metricValue(t, rig.reg, "opens_skipped_total", "reason", "concurrency"); n != 2 {
		t.Errorf("skipped for concurrency = %v, want 2", n)
	}
	if n := metricValue(t, rig.reg, "opens_skipped_total", "reason", "late"); n != 1 {
		t.Errorf("skipped late = %v, want 1", n)
	}
	if n := v.conns.Load(); n != 2 {
		t.Errorf("%d connections, want 2 (the cap)", n)
	}
	// Soonest first, ties by ticker: A and B got the slots.
	got := map[string]bool{}
	for _, c := range rig.captures(t) {
		got[c.Market] = c.Complete
	}
	if !got["S-A"] || !got["S-B"] || len(got) != 2 {
		t.Errorf("captured %v, want S-A and S-B, both complete", got)
	}
}

// A capture that loses its book mid-window records the hole and is
// flagged incomplete; the Client resyncs it from a fresh snapshot.
func TestCaptureRecordsAResync(t *testing.T) {
	var n atomic.Int32
	v := &wsVenue{script: func(conn *websocket.Conn, ticker string) {
		snap := fmt.Sprintf(`{"type":"orderbook_snapshot","sid":1,"seq":1,"msg":{"market_ticker":%q}}`, ticker)
		delta := func(seq int) string {
			return fmt.Sprintf(`{"type":"orderbook_delta","sid":1,"seq":%d,"msg":{"market_ticker":%q,"price_dollars":"0.4000","delta_fp":"10.00","side":"yes","ts_ms":1794528712733}}`, seq, ticker)
		}
		conn.WriteMessage(websocket.TextMessage, []byte(snap))
		if n.Add(1) == 1 {
			conn.WriteMessage(websocket.TextMessage, []byte(delta(2)))
			conn.WriteMessage(websocket.TextMessage, []byte(delta(4))) // seq 3 is missing
			return
		}
		conn.WriteMessage(websocket.TextMessage, []byte(delta(2)))
	}}
	rig := startCapturer(t, v, captureConfig(300*time.Millisecond, 2*time.Second, 2))
	open := time.Now().Add(350 * time.Millisecond)
	rig.c.Schedule.Update("S", []kalshi.Market{{Ticker: "S-A", Open: open}})
	waitFor(t, 15*time.Second, "the capture to finish", func() bool { return len(rig.captures(t)) == 1 })
	rig.stop()

	cap := rig.captures(t)[0]
	if cap.Resyncs != 1 || cap.Complete {
		t.Errorf("capture record %+v, want 1 resync and incomplete", cap)
	}
	recs, err := storage.ReadJSONL[EventRecord](filepath.Join(rig.dir, "events", "S", open.UTC().Format("2006-01-02")+".jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	var shape []string
	for _, r := range recs {
		shape = append(shape, strings.TrimSuffix(fmt.Sprintf("%s/%d/%s", r.Type, r.Seq, r.Stale), "/"))
	}
	want := "[snapshot/1 delta/2 delta/4/seq_gap snapshot/1 delta/2]"
	if fmt.Sprint(shape) != want {
		t.Errorf("records %v, want %s", shape, want)
	}
	if n := metricValue(t, rig.reg, "opens_capture_resyncs_total", "reason", "seq_gap"); n != 1 {
		t.Errorf("seq_gap resyncs = %v, want 1", n)
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
