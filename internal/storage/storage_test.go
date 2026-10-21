package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/lucasjohn05/pmde/internal/metrics"
	"github.com/lucasjohn05/pmde/internal/tick"
)

// t0 is midnight UTC, so it's aligned to every window and to the day.
var t0 = time.Date(2026, 10, 21, 0, 0, 0, 0, time.UTC)

func at(ms int64) time.Time { return t0.Add(time.Duration(ms) * time.Millisecond) }

func ok(ms int64, bid, ask int) tick.Tick {
	return tick.Tick{Venue: tick.Kalshi, Market: "KX-TEST", BidBps: bid, AskBps: ask,
		BidSize: int64(bid) * 10, AskSize: int64(ask) * 10, OK: true, Time: at(ms)}
}

func notOK(ms int64) tick.Tick {
	return tick.Tick{Venue: tick.Kalshi, Market: "KX-TEST", Time: at(ms)}
}

type logRec struct {
	mu    sync.Mutex
	lines []string
}

func (l *logRec) logf(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, fmt.Sprintf(format, args...))
}

func (l *logRec) joined() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return strings.Join(l.lines, "\n")
}

func newTestWriter(t *testing.T) (*Writer, string, *logRec) {
	t.Helper()
	dir := t.TempDir()
	logs := &logRec{}
	w, err := NewWriter(dir, nil, logs.logf)
	if err != nil {
		t.Fatal(err)
	}
	return w, dir, logs
}

func file(dir, venue, market, day string, k kind) string {
	return filepath.Join(dir, venue, market, day+"."+kindNames[k]+".jsonl")
}

func readBars(t *testing.T, path string) []BarRecord {
	t.Helper()
	bars, err := ReadBars(path)
	if err != nil {
		t.Fatal(err)
	}
	return bars
}

func p4(o, h, l, c int) *[4]int { return &[4]int{o, h, l, c} }

func checkBars(t *testing.T, name string, got, want []BarRecord) {
	t.Helper()
	enc := func(b []BarRecord) string {
		out, _ := json.Marshal(b)
		return string(out)
	}
	if enc(got) != enc(want) {
		t.Fatalf("%s bars:\n got  %s\n want %s", name, enc(got), enc(want))
	}
}

// TestTickRecords checks the tick file's lines exactly: t is the local
// receive time, vt the venue time when there is one, and prices and
// sizes appear only on a trusted top of book.
func TestTickRecords(t *testing.T) {
	w, dir, _ := newTestWriter(t)

	a := ok(100, 8700, 8800)
	a.VenueTime = at(40)
	w.write(a)
	w.write(notOK(250)) // no venue time: e.g. a session ending
	w.close(at(300))

	data, err := os.ReadFile(file(dir, "kalshi", "KX-TEST", "2026-10-21", kindTicks))
	if err != nil {
		t.Fatal(err)
	}
	want := fmt.Sprintf(`{"t":%d,"vt":%d,"bid":8700,"ask":8800,"bid_sz":87000,"ask_sz":88000,"ok":true}
{"t":%d,"ok":false}
`, at(100).UnixMilli(), at(40).UnixMilli(), at(250).UnixMilli())
	if string(data) != want {
		t.Fatalf("tick file:\n%s\nwant:\n%s", data, want)
	}
}

// TestBars runs one sequence through all three window sizes:
//
//	   100  trusted 8700/8800   (first Tick after startup: windows partial)
//	   400  trusted 8800/8900
//	  1500  untrusted
//	  2300  trusted 8600/8700
//	120500  trusted 8500/8600  (a new minute: opens on the carried state)
//
// with flushes closing finished windows and a shutdown at 125000.
func TestBars(t *testing.T) {
	w, dir, _ := newTestWriter(t)
	for _, tk := range []tick.Tick{ok(100, 8700, 8800), ok(400, 8800, 8900), notOK(1500), ok(2300, 8600, 8700)} {
		w.write(tk)
	}
	w.flush(at(3999)) // [2000,3000) ends at 3000, closes at 3000+closeGrace
	if got := readBarsIfExists(dir, kind1s); len(got) != 2 {
		t.Fatalf("1s bars after a flush inside the grace period: %d, want 2", len(got))
	}
	w.flush(at(4000))
	w.flush(at(61000))
	w.write(ok(120500, 8500, 8600))
	w.close(at(125000))

	day := "2026-10-21"
	ms := func(off int64) int64 { return at(off).UnixMilli() }

	checkBars(t, "1s", readBars(t, file(dir, "kalshi", "KX-TEST", day, kind1s)), []BarRecord{
		// Opened before any state was known, so partial; ok at the end
		// because the second Tick left the book trusted.
		{T: ms(0), N: 2, Bid: p4(8700, 8800, 8700, 8800), Ask: p4(8800, 8900, 8800, 8900), OK: true, Partial: true},
		// Opens on the carried 8800/8900; untrusted from 1500 to the end.
		{T: ms(1000), N: 1, Bid: p4(8800, 8800, 8800, 8800), Ask: p4(8900, 8900, 8900, 8900), StaleMs: 500, OK: false},
		// Untrusted from the start until the Tick at 2300.
		{T: ms(2000), N: 1, Bid: p4(8600, 8600, 8600, 8600), Ask: p4(8700, 8700, 8700, 8700), StaleMs: 300, OK: true},
		// Nothing for 3000..120000: those windows aren't written.
		// Opens on the carried 8600/8700, then 8500/8600; cut short by
		// the shutdown, so partial.
		{T: ms(120000), N: 1, Bid: p4(8600, 8600, 8500, 8500), Ask: p4(8700, 8700, 8600, 8600), OK: true, Partial: true},
	})
	checkBars(t, "1m", readBars(t, file(dir, "kalshi", "KX-TEST", day, kind1m)), []BarRecord{
		{T: ms(0), N: 4, Bid: p4(8700, 8800, 8600, 8600), Ask: p4(8800, 8900, 8700, 8700), StaleMs: 800, OK: true, Partial: true},
		{T: ms(120000), N: 1, Bid: p4(8600, 8600, 8500, 8500), Ask: p4(8700, 8700, 8600, 8600), OK: true, Partial: true},
	})
	checkBars(t, "1h", readBars(t, file(dir, "kalshi", "KX-TEST", day, kind1h)), []BarRecord{
		{T: ms(0), N: 5, Bid: p4(8700, 8800, 8500, 8500), Ask: p4(8800, 8900, 8600, 8600), StaleMs: 800, OK: true, Partial: true},
	})
}

func readBarsIfExists(dir string, k kind) []BarRecord {
	bars, _ := ReadBars(file(dir, "kalshi", "KX-TEST", "2026-10-21", k))
	return bars
}

// TestBarAllUntrusted: a window that's untrusted throughout has null
// prices and is stale for its whole length after the state is known.
func TestBarAllUntrusted(t *testing.T) {
	w, dir, _ := newTestWriter(t)
	w.write(notOK(5200))
	w.write(notOK(6100)) // same state again (a client wouldn't, but storage mustn't care)
	w.flush(at(8000))
	checkBars(t, "1s", readBars(t, file(dir, "kalshi", "KX-TEST", "2026-10-21", kind1s)), []BarRecord{
		{T: at(5000).UnixMilli(), N: 1, Partial: true, StaleMs: 800},
		{T: at(6000).UnixMilli(), N: 1, StaleMs: 1000},
	})
	w.close(at(8000))
}

// TestClockStepsBack: a Tick stamped earlier than one already written
// keeps its raw time in the tick file, but folds into the open window;
// and a closed window is never reopened.
func TestClockStepsBack(t *testing.T) {
	w, dir, _ := newTestWriter(t)
	w.write(ok(5000, 5000, 5100))
	w.write(ok(4000, 5050, 5100)) // clock stepped back 1s
	w.flush(at(7000))             // closes [5000,6000)
	w.write(ok(5500, 4900, 5100)) // late: its window is already closed
	w.close(at(6500))

	ticks, err := ReadTicks(file(dir, "kalshi", "KX-TEST", "2026-10-21", kindTicks))
	if err != nil {
		t.Fatal(err)
	}
	if len(ticks) != 3 || ticks[1].T != at(4000).UnixMilli() || ticks[2].T != at(5500).UnixMilli() {
		t.Fatalf("tick times not kept raw: %+v", ticks)
	}
	bars := readBars(t, file(dir, "kalshi", "KX-TEST", "2026-10-21", kind1s))
	if len(bars) != 2 || bars[0].T != at(5000).UnixMilli() || bars[0].N != 2 || bars[1].T != at(6000).UnixMilli() || bars[1].N != 1 {
		t.Fatalf("1s bars = %+v; want [5000] with 2 Ticks, then [6000] with the late one", bars)
	}
}

// TestDayRollover: files are per UTC day, chosen by window start, so a
// window never spans two files.
func TestDayRollover(t *testing.T) {
	w, dir, _ := newTestWriter(t)
	w.write(ok(-500, 5000, 5100)) // 2026-10-20 23:59:59.500
	w.write(ok(200, 5100, 5200))  // 2026-10-21 00:00:00.200
	w.write(ok(-100, 5200, 5300)) // the clock stepped back over midnight
	w.close(at(300))

	for _, c := range []struct {
		day   string
		k     kind
		times []int64
	}{
		{"2026-10-20", kindTicks, []int64{-500}},
		// The stepped-back Tick keeps its raw time but goes in today's
		// file, with the bars that count it.
		{"2026-10-21", kindTicks, []int64{200, -100}},
		{"2026-10-20", kind1s, []int64{-1000}},
		{"2026-10-21", kind1s, []int64{0}},
		{"2026-10-20", kind1h, []int64{-3600 * 1000}},
		{"2026-10-21", kind1h, []int64{0}},
	} {
		path := file(dir, "kalshi", "KX-TEST", c.day, c.k)
		var got []int64
		if c.k == kindTicks {
			ticks, err := ReadTicks(path)
			if err != nil {
				t.Fatal(err)
			}
			for _, r := range ticks {
				got = append(got, r.T)
			}
		} else {
			for _, r := range readBars(t, path) {
				got = append(got, r.T)
			}
		}
		var want []int64
		for _, ms := range c.times {
			want = append(want, at(ms).UnixMilli())
		}
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Errorf("%s: times %v, want %v", filepath.Base(path), got, want)
		}
	}
}

// TestTornTail: a crash mid-write leaves a partial last line. Reading
// skips it; reopening for append cuts it off first, so the next record
// starts on its own line.
func TestTornTail(t *testing.T) {
	w, dir, logs := newTestWriter(t)
	w.write(ok(100, 5000, 5100))
	w.close(at(200))
	path := file(dir, "kalshi", "KX-TEST", "2026-10-21", kindTicks)

	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	f.WriteString(`{"t":17925`) // the crash
	f.Close()

	if ticks, err := ReadTicks(path); err != nil || len(ticks) != 1 {
		t.Fatalf("ReadTicks with a torn tail = %d records, %v; want 1, nil", len(ticks), err)
	}

	w2, err := NewWriter(dir, nil, logs.logf)
	if err != nil {
		t.Fatal(err)
	}
	w2.write(ok(300, 5100, 5200))
	w2.close(at(400))

	ticks, err := ReadTicks(path)
	if err != nil || len(ticks) != 2 || ticks[1].T != at(300).UnixMilli() {
		t.Fatalf("after reopening: %+v, %v; want the first record then the new one", ticks, err)
	}
	if !strings.Contains(logs.joined(), "partial line (10 bytes") {
		t.Fatalf("cut not logged:\n%s", logs.joined())
	}

	// A bad line that isn't the last one is corruption, not a torn tail.
	os.WriteFile(path, []byte("{\"t\":1,\"ok\":false}\nnot json\n{\"t\":2,\"ok\":false}\n"), 0o644)
	if _, err := ReadTicks(path); err == nil {
		t.Fatal("ReadTicks accepted a corrupt line mid-file")
	}
}

// TestUnsafeMarket: a market ID that isn't a safe directory name is
// refused and logged, and nothing is written outside the data directory.
func TestUnsafeMarket(t *testing.T) {
	w, dir, logs := newTestWriter(t)
	bad := ok(100, 5000, 5100)
	bad.Market = "../escape"
	w.write(bad)
	w.write(bad)
	w.close(at(200))
	if _, err := os.Stat(filepath.Join(dir, "escape")); err == nil {
		t.Fatal("wrote outside the venue directory")
	}
	if n := strings.Count(logs.joined(), "not a safe directory name"); n != 1 {
		t.Fatalf("logged %d times, want once:\n%s", n, logs.joined())
	}
}

// TestSendBlocking: a full queue blocks Send (the Tick isn't dropped),
// the time blocked is counted, a long block is logged, and cancelling
// ctx releases it.
func TestSendBlocking(t *testing.T) {
	reg := prometheus.NewRegistry()
	logs := &logRec{}
	w, err := NewWriter(t.TempDir(), metrics.New(reg), logs.logf)
	if err != nil {
		t.Fatal(err)
	}
	w.in = make(chan tick.Tick, 1)
	w.in <- notOK(0) // full

	sent := make(chan struct{})
	go func() {
		w.Send(context.Background(), ok(1, 5000, 5100))
		close(sent)
	}()
	select {
	case <-sent:
		t.Fatal("Send returned on a full queue")
	case <-time.After(blockedLogAfter + 100*time.Millisecond):
	}
	<-w.in // the writer catches up
	<-sent
	if got := <-w.in; got.Time != at(1) {
		t.Fatalf("queued Tick = %+v, want the blocked one", got)
	}
	blocked := counter(t, reg, "storage_blocked_seconds_total")
	if blocked < blockedLogAfter.Seconds() {
		t.Fatalf("storage_blocked_seconds_total = %v, want at least %v", blocked, blockedLogAfter.Seconds())
	}
	if !strings.Contains(logs.joined(), "waited") {
		t.Fatalf("long block not logged:\n%s", logs.joined())
	}

	// Shutdown releases a blocked Send.
	w.in <- notOK(0)
	ctx, cancel := context.WithCancel(context.Background())
	released := make(chan struct{})
	go func() {
		w.Send(ctx, ok(2, 5000, 5100))
		close(released)
	}()
	cancel()
	select {
	case <-released:
	case <-time.After(5 * time.Second):
		t.Fatal("Send still blocked after ctx was cancelled")
	}
}

func counter(t *testing.T, reg *prometheus.Registry, name string) float64 {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range families {
		if mf.GetName() == name {
			return mf.GetMetric()[0].GetCounter().GetValue()
		}
	}
	t.Fatalf("no %s", name)
	return 0
}

// TestRun: Ticks through Send and Run land on disk, and cancelling
// writes out what's queued and closes the open windows.
func TestRun(t *testing.T) {
	w, dir, _ := newTestWriter(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		w.Run(ctx)
	}()
	now := time.Now()
	for i := 0; i < 50; i++ {
		tk := ok(0, 5000+i, 5100+i)
		tk.Time = now.Add(time.Duration(i) * time.Millisecond)
		w.Send(ctx, tk)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return")
	}

	day := dayOf(now.UnixMilli())
	ticks, err := ReadTicks(file(dir, "kalshi", "KX-TEST", day, kindTicks))
	if err != nil || len(ticks) != 50 {
		t.Fatalf("%d ticks on disk (%v), want 50", len(ticks), err)
	}
	hour := readBars(t, file(dir, "kalshi", "KX-TEST", day, kind1h))
	if len(hour) != 1 || hour[0].N != 50 || !hour[0].Partial {
		t.Fatalf("1h bars = %+v, want one partial bar of 50", hour)
	}
}

func TestIndex(t *testing.T) {
	dir := t.TempDir()
	hold := "111061902544814266207267295505639408607400625795891618462682726460921782993748"
	err := UpdateIndex(dir, map[string]IndexEntry{
		MarketDir("kalshi", "KX-H0"):  {Venue: "kalshi", Market: "KX-H0", Pairs: []string{"hold"}},
		MarketDir("polymarket", hold): {Venue: "polymarket", Market: hold, Pairs: []string{"hold"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	// A later config: a new pair shares the Kalshi market; the Polymarket
	// one isn't in it any more but its directory must stay explained.
	err = UpdateIndex(dir, map[string]IndexEntry{
		MarketDir("kalshi", "KX-H0"): {Venue: "kalshi", Market: "KX-H0", Pairs: []string{"hold-vs-hike", "hold"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	data, err := os.ReadFile(filepath.Join(dir, IndexFile))
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]IndexEntry
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("index.json isn't valid JSON: %v\n%s", err, data)
	}
	if p := got["kalshi/KX-H0"].Pairs; fmt.Sprint(p) != "[hold hold-vs-hike]" {
		t.Errorf("kalshi pairs = %v, want [hold hold-vs-hike]", p)
	}
	if e := got["polymarket/"+hold]; e.Market != hold || fmt.Sprint(e.Pairs) != "[hold]" {
		t.Errorf("polymarket entry = %+v, want kept from the first config", e)
	}
	if _, err := os.Stat(filepath.Join(dir, IndexFile+".tmp")); err == nil {
		t.Error("temporary file left behind")
	}

	if err := UpdateIndex(dir, map[string]IndexEntry{"kalshi/../x": {Venue: "kalshi", Market: "../x"}}); err == nil {
		t.Error("accepted an unsafe market")
	}
}
