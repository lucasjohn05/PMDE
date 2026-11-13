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

	"github.com/lucasjohn05/pmde/internal/kalshi"
	"github.com/lucasjohn05/pmde/internal/storage"
)

// backfillFake serves the endpoints the backfill reads. Markets are
// JSON objects keyed by ticker; settled lists which a series' settled
// query returns, archived which only the historical endpoints have.
type backfillFake struct {
	mu       sync.Mutex
	markets  map[string]string
	settled  map[string][]string // series -> tickers
	archived map[string]bool
	candles  map[string]string // ticker -> response body (live shape)
	trades   map[string]string // ticker -> response body
	requests []string
}

func (f *backfillFake) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.requests = append(f.requests, r.URL.Path)
	q := r.URL.Query()
	list := func(tickers []string, historical bool) {
		var objs []string
		for _, t := range tickers {
			if m, ok := f.markets[t]; ok && f.archived[t] == historical {
				objs = append(objs, m)
			}
		}
		fmt.Fprintf(w, `{"cursor":"","markets":[%s]}`, strings.Join(objs, ","))
	}
	path := r.URL.Path
	switch {
	case path == "/historical/cutoff":
		fmt.Fprint(w, `{"market_settled_ts":"2026-09-13T00:00:00Z"}`)
	case path == "/markets" || path == "/historical/markets":
		if t := q.Get("tickers"); t != "" {
			list(strings.Split(t, ","), path != "/markets")
		} else {
			list(f.settled[q.Get("series_ticker")], path != "/markets")
		}
	case strings.HasSuffix(path, "/candlesticks"):
		parts := strings.Split(path, "/")
		body, ok := f.candles[parts[len(parts)-2]]
		if !ok {
			body = `{"candlesticks":[]}`
		}
		fmt.Fprint(w, body)
	case path == "/markets/trades" || path == "/historical/trades":
		body, ok := f.trades[q.Get("ticker")]
		if !ok {
			body = `{"cursor":"","trades":[]}`
		}
		fmt.Fprint(w, body)
	default:
		http.NotFound(w, r)
	}
}

func (f *backfillFake) count(prefix, suffix string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, r := range f.requests {
		if strings.HasPrefix(r, prefix) && strings.HasSuffix(r, suffix) {
			n++
		}
	}
	return n
}

var bfOpen = time.Date(2026, 11, 10, 15, 0, 0, 0, time.UTC)

func settledJSON(ticker, event, result, settlement string, floor int, open, settled time.Time) string {
	s := ""
	if result != "" {
		s = fmt.Sprintf(`"settlement_ts":%q,"settlement_value_dollars":%q,`, settled.Format(time.RFC3339), settlement)
	}
	return fmt.Sprintf(`{"ticker":%q,"event_ticker":%q,"status":"finalized","result":%q,%s"yes_sub_title":"%d or above",`+
		`"strike_type":"greater","floor_strike":%d,"open_time":%q,"close_time":%q,"volume_fp":"10.00"}`,
		ticker, event, result, s, floor+1, floor, open.Format(time.RFC3339), open.Add(36*time.Hour).Format(time.RFC3339))
}

// candle builds one live-shape 1-minute candle ending minute minutes
// after bfOpen.
func candle(minute int, bidOpen, bidClose, askOpen, askClose string) string {
	return fmt.Sprintf(`{"end_period_ts":%d,"volume_fp":"0.00",`+
		`"yes_bid":{"open_dollars":%q,"close_dollars":%q},"yes_ask":{"open_dollars":%q,"close_dollars":%q}}`,
		bfOpen.Unix()+int64(60*minute), bidOpen, bidClose, askOpen, askClose)
}

func newBackfill(t *testing.T, fake *backfillFake, series ...string) *Backfill {
	t.Helper()
	srv := httptest.NewServer(fake)
	t.Cleanup(srv.Close)
	return &Backfill{
		REST:   &kalshi.REST{Base: srv.URL, Interval: time.Nanosecond, MaxRetries: 1},
		Dir:    t.TempDir(),
		Series: series,
		Since:  bfOpen.Add(-7 * 24 * time.Hour),
	}
}

func rowsByMarket(t *testing.T, dir string) map[string]OpenRow {
	t.Helper()
	rows, err := LoadRows(dir)
	if err != nil {
		t.Fatal(err)
	}
	m := map[string]OpenRow{}
	for _, r := range rows {
		m[r.Market] = r
	}
	return m
}

// A settled market with no capture: the opening quote is the first
// 1-minute candle that closes two-sided, the impact comes from the
// candles around the first trade, and the result from the market.
func TestBackfillFromCandles(t *testing.T) {
	fake := &backfillFake{
		markets: map[string]string{
			"S-D1-T66": settledJSON("S-D1-T66", "S-D1", "no", "0.0000", 66, bfOpen, bfOpen.Add(45*time.Hour)),
			"S-D1-T60": settledJSON("S-D1-T60", "S-D1", "yes", "1.0000", 60, bfOpen, bfOpen.Add(45*time.Hour)),
		},
		settled: map[string][]string{"S": {"S-D1-T66", "S-D1-T60"}},
		candles: map[string]string{
			// Minute 1 closes with only an ask; minute 2 is the first
			// two-sided close; the trade in minute 4 moves the mid.
			"S-D1-T66": `{"candlesticks":[` + strings.Join([]string{
				candle(1, "0.0000", "0.0000", "1.0000", "0.4500"),
				candle(2, "0.0000", "0.3500", "0.4500", "0.4500"),
				candle(3, "0.3500", "0.3500", "0.4500", "0.4500"),
				candle(4, "0.3500", "0.3800", "0.4500", "0.4800"),
			}, ",") + `]}`,
			// T60 never gets a bid inside the horizon.
			"S-D1-T60": `{"candlesticks":[` + candle(1, "0.0000", "0.0000", "1.0000", "0.9900") + `]}`,
		},
		trades: map[string]string{
			// Newest first. The first print is two fills of one taker
			// order (same timestamp): 12 + 8 contracts.
			"S-D1-T66": `{"cursor":"","trades":[
{"trade_id":"c","ticker":"S-D1-T66","created_time":"2026-11-10T15:20:00Z","yes_price_dollars":"0.4700","count_fp":"5.00","taker_outcome_side":"no"},
{"trade_id":"b","ticker":"S-D1-T66","created_time":"2026-11-10T15:03:30.5Z","yes_price_dollars":"0.4600","count_fp":"8.00","taker_outcome_side":"yes"},
{"trade_id":"a","ticker":"S-D1-T66","created_time":"2026-11-10T15:03:30.5Z","yes_price_dollars":"0.4500","count_fp":"12.00","taker_outcome_side":"yes"}]}`,
		},
	}
	b := newBackfill(t, fake, "S")
	stats, err := b.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if stats.Rows != 2 || stats.Built != 2 || stats.Candles != 1 || stats.NoQuote != 1 || stats.Captured != 0 {
		t.Errorf("stats %+v", stats)
	}
	rows := rowsByMarket(t, b.Dir)
	r := rows["S-D1-T66"]
	if r.Series != "S" || r.Event != "S-D1" || r.Rung != "67 or above" || r.StrikeType != "greater" || r.Floor != "66" ||
		r.OpenMs != bfOpen.UnixMilli() || r.Result != "no" || r.SettleBps == nil || *r.SettleBps != 0 || r.SettledMs == 0 {
		t.Errorf("identity/result: %+v", r)
	}
	if r.Source != SourceCandles || r.QuoteMs != bfOpen.Add(2*time.Minute).UnixMilli() ||
		*r.Bid != 3500 || *r.Ask != 4500 || *r.Mid != 4000 || r.BidSz != nil || r.AskSz != nil {
		t.Errorf("opening quote: %+v", r)
	}
	if r.TradeMs != bfOpen.Add(3*time.Minute+30*time.Second+500*time.Millisecond).UnixMilli() ||
		*r.TradePrice != 4500 || *r.TradeCount != 2000 || !*r.TakerYes {
		t.Errorf("first trade: ms %d price %v count %v", r.TradeMs, *r.TradePrice, *r.TradeCount)
	}
	if r.ImpactSource != SourceCandles || *r.MidBefore != 4000 || *r.MidAfter != 4300 {
		t.Errorf("impact: %+v", r)
	}
	// Both trades are after the 60s window; no capture, so no quote flow.
	if r.WindowTraded == nil || *r.WindowTraded != 0 || r.OpenTraded != nil {
		t.Errorf("flow without a capture: %v %v", r.WindowTraded, r.OpenTraded)
	}
	if q := rows["S-D1-T60"]; q.Source != "" || q.Bid != nil || q.Result != "yes" || *q.SettleBps != 10000 || q.TradePrice != nil {
		t.Errorf("market with no two-sided quote: %+v", q)
	}

	// A second run fetches nothing per market: both rows are final.
	before := fake.count("/", "/candlesticks") + fake.count("/markets/trades", "")
	if stats, err := b.Run(context.Background()); err != nil || stats.Built != 0 || stats.Rows != 2 {
		t.Errorf("second run: %+v, %v", stats, err)
	}
	if after := fake.count("/", "/candlesticks") + fake.count("/markets/trades", ""); after != before {
		t.Errorf("second run made %d more per-market requests", after-before)
	}
}

func intp(v int) *int       { return &v }
func int64p(v int64) *int64 { return &v }

// delta is a captured book change at ms after bfOpen: size added to (or
// taken from, if negative) the yes or no bids at price, as Kalshi sends
// it. Contracts are whole.
func delta(seq int64, ms int64, side string, price int, contracts int64) EventRecord {
	t := bfOpen.UnixMilli() + ms
	return EventRecord{Type: "delta", Seq: seq, TsMs: t, RecvMs: t + 30, Side: side, Price: intp(price), Delta: int64p(contracts * 100)}
}

// withTop fills in each record's best bid and ask, any size, as the
// capture writes them.
func withTop(events []EventRecord) []EventRecord {
	b := newQuoteBook()
	for i, r := range events {
		if r.Stale != "" || r.Type == "stale" {
			continue
		}
		b.apply(r)
		if bid, ask, bs, as, ok := b.best(1); ok {
			events[i].Bid, events[i].BidSz, events[i].Ask, events[i].AskSz = intp(bid), int64p(bs), intp(ask), int64p(as)
		}
	}
	return events
}

// writeCapture stores a capture of market for the backfill to find.
func writeCapture(t *testing.T, dir, series, market string, events []EventRecord) {
	t.Helper()
	events = withTop(events)
	for i := range events {
		events[i].Market, events[i].Series, events[i].OpenMs = market, series, bfOpen.UnixMilli()
	}
	path := filepath.Join(dir, "events", series, bfOpen.Format("2006-01-02")+".jsonl")
	old, _ := storage.ReadJSONL[EventRecord](path)
	if err := storage.WriteJSONL(path, append(old, events...)); err != nil {
		t.Fatal(err)
	}
	if err := storage.AppendLine(filepath.Join(dir, "captures.jsonl"), CaptureRecord{Market: market, Series: series, OpenMs: bfOpen.UnixMilli(), Complete: true}); err != nil {
		t.Fatal(err)
	}
}

// A captured market: the opening quote, its sizes and the price impact
// come from the capture, to the millisecond, and no candles are asked
// for. An unsettled market gets a row that a later run completes.
func TestBackfillFromCapture(t *testing.T) {
	ms := bfOpen.UnixMilli()
	fake := &backfillFake{
		markets: map[string]string{
			// NEW is not allowlisted: it is found through its capture.
			"NEW-D1-A": settledJSON("NEW-D1-A", "NEW-D1", "", "", 10, bfOpen, time.Time{}),
		},
		trades: map[string]string{
			"NEW-D1-A": `{"cursor":"","trades":[
{"trade_id":"a","ticker":"NEW-D1-A","created_time":"2026-11-10T15:00:10.25Z","yes_price_dollars":"0.4400","count_fp":"30.00","taker_outcome_side":"yes"}]}`,
		},
	}
	b := newBackfill(t, fake, "S")
	writeCapture(t, b.Dir, "NEW", "NEW-D1-A", []EventRecord{
		{Type: "snapshot", Seq: 1, RecvMs: ms - 4000},
		// A 1c/99c stub first: two-sided, but no quote.
		delta(2, 50, "yes", 100, 1),
		delta(3, 60, "no", 100, 1),
		// The maker posts the ask (a no bid at 56c), then 180ms later
		// the bid: 250 a side.
		delta(4, 120, "no", 5600, 250),
		delta(5, 300, "yes", 3600, 250),
		// The trade at +10.25s lifts 30 of the ask; 2s later the maker
		// reprices 2c up.
		delta(6, 10250, "no", 5600, -30),
		delta(7, 12000, "yes", 3600, -250),
		delta(8, 12000, "yes", 3800, 250),
		delta(9, 12000, "no", 5600, -220),
		delta(10, 12000, "no", 5400, 250),
		// Past the 5s the impact is read at.
		delta(11, 40000, "yes", 3800, -250),
	})

	stats, err := b.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if stats.Built != 1 || stats.Captured != 1 {
		t.Errorf("stats %+v", stats)
	}
	r := rowsByMarket(t, b.Dir)["NEW-D1-A"]
	if r.Series != "NEW" || r.Source != SourceCapture || r.QuoteMs != ms+300 ||
		*r.Bid != 3600 || *r.Ask != 4400 || *r.Mid != 4000 || *r.BidSz != 25000 || *r.AskSz != 25000 {
		t.Errorf("opening quote: %+v", r)
	}
	if r.ImpactSource != SourceCapture || *r.MidBefore != 4000 || *r.MidAfter != 4200 || *r.TradeCount != 3000 {
		t.Errorf("impact: before %v after %v count %v source %q", r.MidBefore, r.MidAfter, r.TradeCount, r.ImpactSource)
	}
	// Flow in the 60s window: 30 traded; the opening ask's other 220
	// and the whole opening bid were pulled at the reprice.
	if r.FlowWindowMs != 60000 || r.WindowTraded == nil || *r.WindowTraded != 3000 ||
		r.OpenTraded == nil || *r.OpenTraded != 3000 || *r.OpenPulled != 47000 {
		t.Errorf("flow: window %d traded %v open traded %v pulled %v", r.FlowWindowMs, r.WindowTraded, r.OpenTraded, r.OpenPulled)
	}
	if r.Result != "" || r.SettleBps != nil {
		t.Errorf("unsettled market has a result: %+v", r)
	}
	if n := fake.count("/", "/candlesticks"); n != 0 {
		t.Errorf("%d candlestick requests for a captured market, want 0", n)
	}

	// The market settles; the next run completes the row.
	fake.mu.Lock()
	fake.markets["NEW-D1-A"] = settledJSON("NEW-D1-A", "NEW-D1", "yes", "1.0000", 10, bfOpen, bfOpen.Add(30*time.Hour))
	fake.mu.Unlock()
	if stats, err := b.Run(context.Background()); err != nil || stats.Built != 1 {
		t.Fatalf("second run: %+v, %v", stats, err)
	}
	if r := rowsByMarket(t, b.Dir)["NEW-D1-A"]; r.Result != "yes" || *r.SettleBps != 10000 || r.Source != SourceCapture {
		t.Errorf("after settlement: %+v", r)
	}
	// And then it is final.
	if stats, err := b.Run(context.Background()); err != nil || stats.Built != 0 {
		t.Errorf("third run: %+v, %v", stats, err)
	}
}

// A capture that lost its book before the first two-sided quote is not
// trusted for the opening quote; candles are used instead.
func TestBackfillIgnoresACaptureWithAHole(t *testing.T) {
	ms := bfOpen.UnixMilli()
	fake := &backfillFake{
		markets: map[string]string{"S-D1-T66": settledJSON("S-D1-T66", "S-D1", "no", "0.0000", 66, bfOpen, bfOpen.Add(45*time.Hour))},
		settled: map[string][]string{"S": {"S-D1-T66"}},
		candles: map[string]string{"S-D1-T66": `{"candlesticks":[` + candle(1, "0.0000", "0.2000", "1.0000", "0.3000") + `]}`},
	}
	b := newBackfill(t, fake, "S")
	writeCapture(t, b.Dir, "S", "S-D1-T66", []EventRecord{
		{Type: "snapshot", Seq: 1, RecvMs: ms - 4000},
		{Type: "delta", Seq: 3, TsMs: ms + 100, RecvMs: ms + 130, Stale: "seq_gap"},
		{Type: "snapshot", Seq: 1, RecvMs: ms + 900, Yes: [][2]int64{{2100, 5000}}, No: [][2]int64{{7100, 5000}}},
	})
	if _, err := b.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if r := rowsByMarket(t, b.Dir)["S-D1-T66"]; r.Source != SourceCandles || *r.Bid != 2000 || *r.Ask != 3000 || r.BidSz != nil || !r.CaptureSeen {
		t.Errorf("row %+v, want the candle quote", r)
	}
	// The capture was consulted, so the settled row is final: it is not
	// rebuilt on every run in the hope of a better capture.
	if stats, err := b.Run(context.Background()); err != nil || stats.Built != 0 {
		t.Errorf("second run: %+v, %v", stats, err)
	}
}

// Markets settled before Kalshi's cutoff come from the /historical
// endpoints, candlesticks and trades included.
func TestBackfillHistorical(t *testing.T) {
	open := time.Date(2026, 9, 10, 14, 0, 0, 0, time.UTC)
	fake := &backfillFake{
		markets:  map[string]string{"S-OLD": settledJSON("S-OLD", "S-OLDEV", "no", "0.0000", 86, open, open.Add(40*time.Hour))},
		settled:  map[string][]string{"S": {"S-OLD"}},
		archived: map[string]bool{"S-OLD": true},
		candles: map[string]string{"S-OLD": fmt.Sprintf(`{"candlesticks":[{"end_period_ts":%d,"volume":"89.00",`+
			`"yes_ask":{"close":"0.0300","open":"1.0000"},"yes_bid":{"close":"0.0100","open":"0.0100"}}]}`, open.Unix()+60)},
	}
	b := newBackfill(t, fake, "S")
	b.Since = open.Add(-24 * time.Hour) // before the 2026-09-13 cutoff
	if _, err := b.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	r, ok := rowsByMarket(t, b.Dir)["S-OLD"]
	if !ok || r.Source != SourceCandles || *r.Bid != 100 || *r.Ask != 300 || r.Result != "no" {
		t.Fatalf("row %+v (found %v)", r, ok)
	}
	if fake.count("/historical/markets/S-OLD", "/candlesticks") != 1 || fake.count("/historical/trades", "") != 1 {
		t.Errorf("historical endpoints not used: %q", fake.requests)
	}
	if fake.count("/series/", "/candlesticks") != 0 || fake.count("/markets/trades", "") != 0 {
		t.Errorf("live endpoints used for an archived market: %q", fake.requests)
	}
}
