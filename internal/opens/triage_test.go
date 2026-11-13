package opens

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lucasjohn05/pmde/internal/kalshi"
	"github.com/lucasjohn05/pmde/internal/storage"
)

// triageFake serves the three endpoints triage reads: the cutoff, a
// series' settled markets, and batch candlesticks.
type triageFake struct {
	mu      sync.Mutex
	markets map[string][]string // series -> market JSON objects
	candles map[string]string   // ticker -> JSON array of candles
	batches [][]string          // the tickers of each batch request
	queries []string
}

func (f *triageFake) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	q := r.URL.Query()
	f.queries = append(f.queries, r.URL.Path+"?"+r.URL.RawQuery)
	switch r.URL.Path {
	case "/historical/cutoff":
		fmt.Fprint(w, `{"market_settled_ts":"2026-09-13T00:00:00Z"}`)
	case "/markets":
		fmt.Fprintf(w, `{"cursor":"","markets":[%s]}`, strings.Join(f.markets[q.Get("series_ticker")], ","))
	case "/markets/candlesticks":
		tickers := strings.Split(q.Get("market_tickers"), ",")
		f.batches = append(f.batches, tickers)
		var objs []string
		for _, t := range tickers {
			if c, ok := f.candles[t]; ok {
				objs = append(objs, fmt.Sprintf(`{"market_ticker":%q,"candlesticks":%s}`, t, c))
			}
		}
		fmt.Fprintf(w, `{"markets":[%s]}`, strings.Join(objs, ","))
	default:
		http.NotFound(w, r)
	}
}

// triageCandles is a market's candles: two-sided at bid/ask from the
// candle ending fromMinute minutes after open, empty before.
func triageCandles(open time.Time, fromMinute, bidBps, askBps int) string {
	var cs []string
	for m := 1; m <= 10; m++ {
		bid, ask := "0.0000", "1.0000"
		if m >= fromMinute {
			bid, ask = fmt.Sprintf("%.4f", float64(bidBps)/10000), fmt.Sprintf("%.4f", float64(askBps)/10000)
		}
		cs = append(cs, fmt.Sprintf(`{"end_period_ts":%d,"volume_fp":"0.00","yes_bid":{"open_dollars":%q,"close_dollars":%q},"yes_ask":{"open_dollars":%q,"close_dollars":%q}}`,
			open.Unix()+int64(60*m), bid, bid, ask, ask))
	}
	return "[" + strings.Join(cs, ",") + "]"
}

// addLadder gives series an event of n "greater" rungs opening at open,
// quoted off N(mean, sd) with a 4c spread from fromMinute on.
func (f *triageFake) addLadder(series, event string, open time.Time, n int, mean, sd float64, fromMinute int) {
	for i := range n {
		k := 50 + 2*i
		ticker := fmt.Sprintf("%s-T%d", event, k)
		f.markets[series] = append(f.markets[series], settledJSON(ticker, event, "no", "0.0000", k, open, open.Add(30*time.Hour)))
		p := 1 - 0.5*math.Erfc(-(float64(k)-mean)/sd/math.Sqrt2)
		midBps := min(max(int(math.Round(p*100))*100, 300), 9700)
		f.candles[ticker] = triageCandles(open, fromMinute, midBps-200, midBps+200)
	}
}

func newTriage(t *testing.T, fake *triageFake, series ...string) *Triage {
	t.Helper()
	srv := httptest.NewServer(fake)
	t.Cleanup(srv.Close)
	return &Triage{
		REST:   &kalshi.REST{Base: srv.URL, Interval: time.Nanosecond, MaxRetries: 1},
		Dir:    t.TempDir(),
		Series: series,
	}
}

func TestTriage(t *testing.T) {
	fake := &triageFake{markets: map[string][]string{}, candles: map[string]string{}}
	day := time.Date(2026, 11, 2, 15, 0, 0, 0, time.UTC)
	// MADE: a 5-rung ladder a day for 4 days, quoted in the first minute.
	for d := range 4 {
		fake.addLadder("MADE", fmt.Sprintf("MADE-D%d", d), day.Add(time.Duration(d)*24*time.Hour), 5, 55, 3, 1)
	}
	// SLOW: 2 single-rung events a day for 2 days, quoted from minute 5.
	for d := range 2 {
		for e := range 2 {
			open := day.Add(time.Duration(d)*24*time.Hour + time.Duration(e)*time.Hour)
			fake.addLadder("SLOW", fmt.Sprintf("SLOW-D%dE%d", d, e), open, 1, 50, 3, 5)
		}
	}
	// DEAD: listed, never quoted.
	fake.markets["DEAD"] = []string{settledJSON("DEAD-A", "DEAD-E", "no", "0.0000", 1, day, day.Add(30*time.Hour))}

	tr := newTriage(t, fake, "MADE", "SLOW", "DEAD", "EMPTY")
	rows, err := tr.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	by := map[string]TriageRow{}
	for _, r := range rows {
		by[r.Series] = r
	}

	m := by["MADE"]
	if m.Opens != 20 || m.Events != 4 || m.Days != 4 || m.OpensPerDay != 5 || m.EventsPerDay != 1 || m.Sampled != 20 || m.SampledEvents != 4 {
		t.Errorf("MADE counts: %+v", m)
	}
	if m.QuotedFirstMinute != 1 || m.Quoted != 1 || m.SpreadBps != 400 || m.FitEvents != 4 || m.FitRMSEBps > 60 {
		t.Errorf("MADE quotes: %+v", m)
	}
	s := by["SLOW"]
	if s.Opens != 4 || s.Events != 4 || s.Days != 2 || s.EventsPerDay != 2 || s.QuotedFirstMinute != 0 || s.Quoted != 1 || s.FitEvents != 0 {
		t.Errorf("SLOW: %+v", s)
	}
	if d := by["DEAD"]; d.Err != "" || d.Opens != 1 || d.Quoted != 0 || d.QuotedFirstMinute != 0 || d.Sampled != 1 {
		t.Errorf("DEAD: %+v", d)
	}
	if e := by["EMPTY"]; !strings.Contains(e.Err, "no markets settled since 2026-09-13") {
		t.Errorf("EMPTY: %+v", e)
	}

	// Since defaulted to the historical cutoff, and the listing asked
	// for settled markets from then.
	if !strings.Contains(strings.Join(fake.queries, "\n"), "min_settled_ts=1789257600") {
		t.Errorf("listing did not use the cutoff: %q", fake.queries[:3])
	}
	// One batch request per event: an event's rungs share a window.
	if len(fake.batches) != 4+4+1 {
		t.Errorf("%d batch requests, want 9 (one per event)", len(fake.batches))
	}
	for _, b := range fake.batches {
		if strings.HasPrefix(b[0], "MADE") && len(b) != 5 {
			t.Errorf("a MADE batch had %d tickers, want the event's 5", len(b))
		}
	}

	// The rows are on file, candle-only; a rerun fetches nothing and
	// reports the same.
	saved, err := storage.ReadJSONL[OpenRow](filepath.Join(tr.Dir, "triage.jsonl"))
	if err != nil || len(saved) != 25 {
		t.Fatalf("%d rows on file, want 25 (%v)", len(saved), err)
	}
	for _, r := range saved {
		if r.Source == SourceCapture || r.TradePrice != nil || r.BidSz != nil {
			t.Errorf("triage row is not candle-only: %+v", r)
		}
	}
	before := len(fake.batches)
	again, err := tr.Run(context.Background())
	if err != nil || len(fake.batches) != before {
		t.Fatalf("rerun: %d new batch requests, err %v", len(fake.batches)-before, err)
	}
	if again[0] != rows[0] || again[1] != rows[1] {
		t.Errorf("rerun differs: %+v vs %+v", again[0], rows[0])
	}

	// Ranked by first-minute share, failures last; the table prints.
	SortTriage(rows)
	if rows[0].Series != "MADE" || rows[len(rows)-1].Series != "EMPTY" {
		t.Errorf("order: %s ... %s", rows[0].Series, rows[len(rows)-1].Series)
	}
	var buf bytes.Buffer
	if err := WriteTriage(&buf, rows); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"opens/day", "events/day", "quoted 1m", "spread", "fit rmse", "MADE", "100%", "EMPTY: no markets settled"} {
		if !strings.Contains(buf.String(), want) {
			t.Errorf("table lacks %q:\n%s", want, buf.String())
		}
	}
	if got := Passing(rows, 0.5); fmt.Sprint(got) != "[MADE]" {
		t.Errorf("passing at 50%%: %v", got)
	}
	if got := Passing(rows, 0); fmt.Sprint(got) != "[MADE SLOW DEAD]" {
		t.Errorf("passing at 0: %v", got)
	}
}

// Counts come from the whole listing; candles from an evenly spaced
// sample of whole events.
func TestTriageSamplesWholeEvents(t *testing.T) {
	fake := &triageFake{markets: map[string][]string{}, candles: map[string]string{}}
	day := time.Date(2026, 11, 2, 15, 0, 0, 0, time.UTC)
	for d := range 9 {
		fake.addLadder("BIG", fmt.Sprintf("BIG-D%d", d), day.Add(time.Duration(d)*24*time.Hour), 4, 55, 3, 1)
	}
	tr := newTriage(t, fake, "BIG")
	tr.SampleEvents = 3
	tr.Since = day.Add(-24 * time.Hour)
	rows, err := tr.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	r := rows[0]
	if r.Opens != 36 || r.Events != 9 || r.OpensPerDay != 4 || r.SampledEvents != 3 || r.Sampled != 12 || r.FitEvents != 3 {
		t.Errorf("row %+v", r)
	}
	var events []string
	for _, b := range fake.batches {
		if len(b) != 4 {
			t.Errorf("a sampled event was fetched in part: %v", b)
		}
		events = append(events, strings.TrimSuffix(b[0], "-T50"))
	}
	if fmt.Sprint(events) != "[BIG-D0 BIG-D4 BIG-D8]" {
		t.Errorf("sampled events %v, want the first, middle and last", events)
	}
	if strings.Contains(strings.Join(fake.queries, "\n"), "/historical/cutoff") {
		t.Errorf("the cutoff was fetched although -since was given")
	}
}

func TestAllowlistAndSeriesList(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteAllowlist(&buf, []string{"KXHIGHNY", "KXWTI"}, "2 of 5 series passed.\nSecond line.", DefaultQuoteRule()); err != nil {
		t.Fatal(err)
	}
	cfg, err := ParseConfig(buf.Bytes())
	if err != nil {
		t.Fatalf("the written allowlist does not parse: %v\n%s", err, buf.String())
	}
	want := DefaultConfig()
	want.Series = []string{"KXHIGHNY", "KXWTI"}
	if fmt.Sprintf("%+v", cfg) != fmt.Sprintf("%+v", want) {
		t.Errorf("parsed %+v, want the defaults with the two series", cfg)
	}
	if !strings.Contains(buf.String(), "# 2 of 5 series passed.\n# Second line.\n") {
		t.Errorf("note missing:\n%s", buf.String())
	}
	// An empty allowlist is still a valid config (new series only).
	buf.Reset()
	WriteAllowlist(&buf, nil, "none passed", DefaultQuoteRule())
	if cfg, err := ParseConfig(buf.Bytes()); err != nil || len(cfg.Series) != 0 {
		t.Errorf("empty allowlist: %+v, %v", cfg, err)
	}

	got, err := ReadSeriesList(strings.NewReader("# hourly and daily\nKXHIGHNY  daily  Highest temperature in NYC\n\nKXWTI\nKXHIGHNY\n  KXBTC # crypto\n"))
	if err != nil || fmt.Sprint(got) != "[KXHIGHNY KXWTI KXBTC]" {
		t.Errorf("series list %v, %v", got, err)
	}
	if _, err := ReadSeriesList(strings.NewReader("../etc\n")); err == nil {
		t.Errorf("no error for an unsafe ticker")
	}
}

func TestCandidates(t *testing.T) {
	dir := t.TempDir()
	if s, day, err := Candidates(dir, nil); err != nil || s != nil || day != "" {
		t.Errorf("no snapshot: %v %q %v", s, day, err)
	}
	snap := []kalshi.Series{
		{Ticker: "KXWTI", Frequency: "hourly", Title: "WTI"},
		{Ticker: "KXHIGHNY", Frequency: "daily", Title: "NYC high", FeeType: "quadratic", FeeMultiplier: json.Number("1")},
		{Ticker: "KXFED", Frequency: "custom"},
		{Ticker: "bad/ticker", Frequency: "daily"},
	}
	if err := storage.WriteJSONL(filepath.Join(dir, "series", "2026-11-12.jsonl"), []kalshi.Series{{Ticker: "OLD", Frequency: "daily"}}); err != nil {
		t.Fatal(err)
	}
	if err := storage.WriteJSONL(filepath.Join(dir, "series", "2026-11-13.jsonl"), snap); err != nil {
		t.Fatal(err)
	}
	got, day, err := Candidates(dir, []string{"hourly", "daily"})
	if err != nil || day != "2026-11-13" || len(got) != 2 || got[0].Ticker != "KXHIGHNY" || got[1].Ticker != "KXWTI" {
		t.Errorf("candidates %+v from %q, %v", got, day, err)
	}
	if all, _, _ := Candidates(dir, nil); len(all) != 3 {
		t.Errorf("%d candidates with no filter, want 3 (the unsafe ticker left out)", len(all))
	}
}
