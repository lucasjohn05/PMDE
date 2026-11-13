package opens

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/lucasjohn05/pmde/internal/kalshi"
	"github.com/lucasjohn05/pmde/internal/metrics"
	"github.com/lucasjohn05/pmde/internal/storage"
)

// fakeKalshi serves the REST endpoints discovery uses from in-memory
// state the test changes between polls.
type fakeKalshi struct {
	mu       sync.Mutex
	series   []string            // tickers in the series list
	unopened map[string][]string // series -> market JSON objects
	fail     map[string]bool     // series whose market list returns 500
	requests []string
}

func (f *fakeKalshi) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.requests = append(f.requests, r.URL.Path+"?"+r.URL.RawQuery)
	q := r.URL.Query()
	switch r.URL.Path {
	case "/series":
		var objs []string
		for _, s := range f.series {
			objs = append(objs, fmt.Sprintf(`{"ticker":%q,"title":"Title of %s","category":"Climate and Weather","frequency":"daily"}`, s, s))
		}
		fmt.Fprintf(w, `{"series":[%s]}`, strings.Join(objs, ","))
	case "/markets":
		series := q.Get("series_ticker")
		if q.Get("status") != "unopened" {
			http.Error(w, "unexpected status filter", http.StatusBadRequest)
			return
		}
		if f.fail[series] {
			http.Error(w, "boom", http.StatusInternalServerError)
			return
		}
		fmt.Fprintf(w, `{"cursor":"","markets":[%s]}`, strings.Join(f.unopened[series], ","))
	case "/events":
		var objs []string
		for _, t := range strings.Split(q.Get("tickers"), ",") {
			series, _, _ := strings.Cut(t, "-")
			objs = append(objs, fmt.Sprintf(`{"event_ticker":%q,"series_ticker":%q,"title":"Event %s","sub_title":"","mutually_exclusive":true}`, t, series, t))
		}
		fmt.Fprintf(w, `{"cursor":"","events":[%s],"milestones":[]}`, strings.Join(objs, ","))
	default:
		http.NotFound(w, r)
	}
}

func (f *fakeKalshi) set(fn func()) {
	f.mu.Lock()
	defer f.mu.Unlock()
	fn()
}

func marketJSON(ticker, event string, open time.Time) string {
	return fmt.Sprintf(`{"ticker":%q,"event_ticker":%q,"status":"initialized","result":"","yes_sub_title":"rung of %s",`+
		`"strike_type":"greater","floor_strike":66,"open_time":%q,"close_time":%q,"volume_fp":"0.00"}`,
		ticker, event, ticker, open.Format(time.RFC3339), open.Add(24*time.Hour).Format(time.RFC3339))
}

type discoveryRig struct {
	d    *Discovery
	fake *fakeKalshi
	reg  *prometheus.Registry
	now  time.Time
	logs []string
	dir  string
}

func newDiscoveryRig(t *testing.T, cfg Config) *discoveryRig {
	t.Helper()
	rig := &discoveryRig{
		fake: &fakeKalshi{unopened: map[string][]string{}, fail: map[string]bool{}},
		reg:  prometheus.NewRegistry(),
		now:  time.Date(2026, 11, 12, 10, 0, 0, 0, time.UTC),
		dir:  t.TempDir(),
	}
	srv := httptest.NewServer(rig.fake)
	t.Cleanup(srv.Close)
	rig.d = rig.discovery(srv.URL, cfg, metrics.NewOpens(rig.reg))
	if err := rig.d.Load(); err != nil {
		t.Fatal(err)
	}
	return rig
}

func (rig *discoveryRig) discovery(base string, cfg Config, m *metrics.Opens) *Discovery {
	return &Discovery{
		REST:     &kalshi.REST{Base: base, Interval: time.Nanosecond, MaxRetries: 1},
		Dir:      rig.dir,
		Cfg:      cfg,
		Schedule: NewSchedule(),
		Metrics:  m,
		Logf:     func(f string, a ...any) { rig.logs = append(rig.logs, fmt.Sprintf(f, a...)) },
		Now:      func() time.Time { return rig.now },
	}
}

func (rig *discoveryRig) counter(t *testing.T, name string, labels ...string) float64 {
	t.Helper()
	return metricValue(t, rig.reg, name, labels...)
}

// metricValue reads one counter or gauge series from g.
func metricValue(t *testing.T, g prometheus.Gatherer, name string, labels ...string) float64 {
	t.Helper()
	mfs, err := g.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
	metric:
		for _, m := range mf.GetMetric() {
			for i := 0; i < len(labels); i += 2 {
				found := false
				for _, lp := range m.GetLabel() {
					found = found || lp.GetName() == labels[i] && lp.GetValue() == labels[i+1]
				}
				if !found {
					continue metric
				}
			}
			if m.Counter != nil {
				return m.Counter.GetValue()
			}
			return m.Gauge.GetValue()
		}
	}
	t.Fatalf("no metric %s %v", name, labels)
	return 0
}

func testConfig(series ...string) Config {
	c := DefaultConfig()
	c.Series = series
	return c
}

func TestSeriesSnapshotAndNewSeries(t *testing.T) {
	rig := newDiscoveryRig(t, testConfig("KXHIGHNY"))
	ctx := context.Background()
	rig.fake.series = []string{"KXHIGHNY", "KXFED"}

	// First poll ever: the baseline. Snapshot written, nothing is "new".
	if err := rig.d.PollSeries(ctx); err != nil {
		t.Fatal(err)
	}
	snap, err := storage.ReadJSONL[kalshi.Series](filepath.Join(rig.dir, "series", "2026-11-12.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if len(snap) != 2 || snap[0].Ticker != "KXFED" || snap[1].Ticker != "KXHIGHNY" || snap[0].Category != "Climate and Weather" {
		t.Errorf("snapshot %+v", snap)
	}
	if n := rig.counter(t, "opens_new_series_total"); n != 0 {
		t.Errorf("baseline counted %v new series", n)
	}
	if got := rig.d.Watched(); fmt.Sprint(got) != "[KXHIGHNY]" {
		t.Errorf("watched %v", got)
	}

	// Later the same day a series appears: incremental poll, logged,
	// recorded and watched.
	rig.now = rig.now.Add(15 * time.Minute)
	rig.fake.set(func() { rig.fake.series = append(rig.fake.series, "KXNEWTHING") })
	if err := rig.d.PollSeries(ctx); err != nil {
		t.Fatal(err)
	}
	last := rig.fake.requests[len(rig.fake.requests)-1]
	wantSince := fmt.Sprintf("min_updated_ts=%d", rig.now.Add(-15*time.Minute-seriesOverlap).Unix())
	if !strings.Contains(last, wantSince) {
		t.Errorf("incremental poll was %q, want %s", last, wantSince)
	}
	recs, err := storage.ReadJSONL[NewSeriesRecord](filepath.Join(rig.dir, "new_series.jsonl"))
	if err != nil || len(recs) != 1 || recs[0].Ticker != "KXNEWTHING" || recs[0].T != rig.now.UnixMilli() || recs[0].Title != "Title of KXNEWTHING" {
		t.Fatalf("new_series.jsonl: %+v, %v", recs, err)
	}
	if n := rig.counter(t, "opens_new_series_total"); n != 1 {
		t.Errorf("opens_new_series_total = %v, want 1", n)
	}
	if !strings.Contains(strings.Join(rig.logs, "\n"), "new series KXNEWTHING") {
		t.Errorf("no log line for the new series: %q", rig.logs)
	}
	if got := rig.d.Watched(); fmt.Sprint(got) != "[KXHIGHNY KXNEWTHING]" {
		t.Errorf("watched %v", got)
	}
	// Seen again: not new twice.
	if err := rig.d.PollSeries(ctx); err != nil {
		t.Fatal(err)
	}
	if n := rig.counter(t, "opens_new_series_total"); n != 1 {
		t.Errorf("a known series was counted new again: %v", n)
	}

	// Next UTC day: a full fetch and a second snapshot.
	rig.now = rig.now.Add(24 * time.Hour)
	if err := rig.d.PollSeries(ctx); err != nil {
		t.Fatal(err)
	}
	if last := rig.fake.requests[len(rig.fake.requests)-1]; strings.Contains(last, "min_updated_ts") {
		t.Errorf("the first poll of a new day was incremental: %q", last)
	}
	if snap, err := storage.ReadJSONL[kalshi.Series](filepath.Join(rig.dir, "series", "2026-11-13.jsonl")); err != nil || len(snap) != 3 {
		t.Errorf("second snapshot: %d series, %v", len(snap), err)
	}

	// A restart restores the known set and the new series, so nothing
	// is re-announced and the new series is still watched...
	srv := httptest.NewServer(rig.fake)
	defer srv.Close()
	reg2 := prometheus.NewRegistry()
	d2 := rig.discovery(srv.URL, rig.d.Cfg, metrics.NewOpens(reg2))
	if err := d2.Load(); err != nil {
		t.Fatal(err)
	}
	rig.fake.set(func() { rig.fake.series = append(rig.fake.series, "KXAFTERRESTART") })
	if err := d2.PollSeries(ctx); err != nil {
		t.Fatal(err)
	}
	// The first poll after a restart has no last poll to be incremental
	// from: it is a full fetch.
	if last := rig.fake.requests[len(rig.fake.requests)-1]; last != "/series?" {
		t.Errorf("first series poll after a restart was %q, want a full fetch", last)
	}
	if n := metricValue(t, reg2, "opens_new_series_total"); n != 1 {
		t.Errorf("after restart %v new series, want only KXAFTERRESTART", n)
	}
	if got := d2.Watched(); fmt.Sprint(got) != "[KXAFTERRESTART KXHIGHNY KXNEWTHING]" {
		t.Errorf("watched after restart %v", got)
	}
	// ...until it ages out of the new-series window.
	rig.now = rig.now.Add(15 * 24 * time.Hour)
	if got := d2.Watched(); fmt.Sprint(got) != "[KXHIGHNY]" {
		t.Errorf("watched after the window %v", got)
	}
}

func TestMaxNewSeriesKeepsTheMostRecent(t *testing.T) {
	cfg := testConfig()
	cfg.MaxNewSeries = 2
	rig := newDiscoveryRig(t, cfg)
	ctx := context.Background()
	rig.fake.series = []string{"OLD"}
	if err := rig.d.PollSeries(ctx); err != nil {
		t.Fatal(err)
	}
	for _, s := range []string{"NEW1", "NEW2", "NEW3", "not/safe"} {
		rig.now = rig.now.Add(time.Minute)
		rig.fake.set(func() { rig.fake.series = append(rig.fake.series, s) })
		if err := rig.d.PollSeries(ctx); err != nil {
			t.Fatal(err)
		}
	}
	// NEW3 and NEW2 are the two most recent with usable names.
	if got := rig.d.Watched(); fmt.Sprint(got) != "[NEW2 NEW3]" {
		t.Errorf("watched %v, want [NEW2 NEW3]", got)
	}
}

func TestPollMarketsBuildsSchedule(t *testing.T) {
	rig := newDiscoveryRig(t, testConfig("KXHIGHNY", "KXHIGHCHI"))
	ctx := context.Background()
	open := rig.now.Add(30 * time.Minute)
	rig.fake.unopened["KXHIGHNY"] = []string{
		marketJSON("KXHIGHNY-26NOV13-T66", "KXHIGHNY-26NOV13", open),
		marketJSON("KXHIGHNY-26NOV13-T68", "KXHIGHNY-26NOV13", open),
	}
	rig.fake.unopened["KXHIGHCHI"] = []string{marketJSON("KXHIGHCHI-26NOV13-T50", "KXHIGHCHI-26NOV13", open.Add(time.Hour))}

	if err := rig.d.PollMarkets(ctx); err != nil {
		t.Fatal(err)
	}
	if n := rig.d.Schedule.Len(); n != 3 {
		t.Fatalf("%d scheduled, want 3", n)
	}
	if next, ok := rig.d.Schedule.Next(); !ok || !next.Equal(open) {
		t.Errorf("next open %v %v, want %v", next, ok, open)
	}
	if n := rig.counter(t, "opens_scheduled_markets"); n != 3 {
		t.Errorf("opens_scheduled_markets = %v", n)
	}
	if n := rig.counter(t, "opens_watched_series"); n != 2 {
		t.Errorf("opens_watched_series = %v", n)
	}
	// The two events were looked up once and recorded.
	evs, err := storage.ReadJSONL[kalshi.Event](filepath.Join(rig.dir, "events.jsonl"))
	if err != nil || len(evs) != 2 || evs[0].Series != "KXHIGHCHI" || !evs[0].MutuallyExclusive {
		t.Fatalf("events.jsonl: %+v, %v", evs, err)
	}

	// The open_time moves, one market leaves the list before opening,
	// and the other series fails: its schedule must be left alone.
	moved := open.Add(10 * time.Minute)
	rig.fake.set(func() {
		rig.fake.unopened["KXHIGHNY"] = []string{marketJSON("KXHIGHNY-26NOV13-T66", "KXHIGHNY-26NOV13", moved)}
		rig.fake.fail["KXHIGHCHI"] = true
	})
	before := len(rig.fake.requests)
	err = rig.d.PollMarkets(ctx)
	if err == nil || !strings.Contains(err.Error(), "KXHIGHCHI") {
		t.Fatalf("err = %v, want the KXHIGHCHI failure", err)
	}
	if n := rig.d.Schedule.Len(); n != 2 {
		t.Errorf("%d scheduled, want 2 (T66 moved, T68 vanished, CHI kept)", n)
	}
	if next, _ := rig.d.Schedule.Next(); !next.Equal(moved) {
		t.Errorf("next open %v, want the moved time %v", next, moved)
	}
	if n := rig.counter(t, "opens_skipped_total", "reason", "vanished"); n != 1 {
		t.Errorf("vanished = %v, want 1", n)
	}
	if n := rig.counter(t, "opens_poll_errors_total", "kind", "markets"); n != 1 {
		t.Errorf("market poll errors = %v, want 1", n)
	}
	for _, r := range rig.fake.requests[before:] {
		if strings.HasPrefix(r, "/events") {
			t.Errorf("known events were fetched again: %s", r)
		}
	}
}

func TestScheduleDue(t *testing.T) {
	s := NewSchedule()
	t0 := time.Date(2026, 11, 12, 15, 0, 0, 0, time.UTC)
	mk := func(ticker string, open time.Time) kalshi.Market { return kalshi.Market{Ticker: ticker, Open: open} }
	s.Update("S", []kalshi.Market{mk("soon", t0.Add(4*time.Second)), mk("sooner", t0.Add(2*time.Second)),
		mk("later", t0.Add(time.Minute)), mk("past", t0.Add(-time.Second))})

	due, late := s.Due(t0, 5*time.Second)
	if len(due) != 2 || due[0].Market.Ticker != "sooner" || due[1].Market.Ticker != "soon" || due[0].Series != "S" {
		t.Errorf("due %+v", due)
	}
	if len(late) != 1 || late[0].Market.Ticker != "past" {
		t.Errorf("late %+v", late)
	}
	if s.Len() != 1 {
		t.Errorf("%d left, want 1", s.Len())
	}
	// A poll that still lists a taken market does not reschedule it,
	// and its absence later is not counted as vanished.
	added, vanished := s.Update("S", []kalshi.Market{mk("soon", t0.Add(4*time.Second)), mk("later", t0.Add(time.Minute))})
	if added != 0 || vanished != 0 || s.Len() != 1 {
		t.Errorf("added %d vanished %d len %d after re-listing a taken market", added, vanished, s.Len())
	}
	if due, late := s.Due(t0.Add(time.Second), 5*time.Second); len(due)+len(late) != 0 {
		t.Errorf("a market came due twice: %+v %+v", due, late)
	}
	s.Drop("S")
	if s.Len() != 0 {
		t.Errorf("Drop left %d", s.Len())
	}
}

func TestParseConfig(t *testing.T) {
	c, err := ParseConfig([]byte("series: [KXHIGHNY, KXHIGHCHI]\nlead: 3s\nwindow: 90s\nmax_concurrent: 8\nnew_series_days: 7\ninclude_new_series: false\n"))
	if err != nil {
		t.Fatal(err)
	}
	if len(c.Series) != 2 || c.Lead != 3*time.Second || c.Window != 90*time.Second || c.MaxConcurrent != 8 ||
		c.NewSeriesFor != 7*24*time.Hour || c.IncludeNewSeries || c.MarketPoll != time.Minute {
		t.Errorf("config %+v", c)
	}
	if c, err := ParseConfig(nil); err != nil || !c.IncludeNewSeries || c.MaxConcurrent != 50 {
		t.Errorf("empty config: %+v, %v", c, err)
	}
	for name, bad := range map[string]string{
		"unknown key":     "serie: [A]\n",
		"unsafe series":   "series: [\"../x\"]\n",
		"duplicate":       "series: [A, A]\n",
		"bad duration":    "series: [A]\nlead: soon\n",
		"short window":    "series: [A]\nwindow: 10ms\n",
		"zero concurrent": "series: [A]\nmax_concurrent: 0\n",
		"nothing to scan": "include_new_series: false\n",
	} {
		if _, err := ParseConfig([]byte(bad)); err == nil {
			t.Errorf("%s: no error for %q", name, bad)
		}
	}
}
