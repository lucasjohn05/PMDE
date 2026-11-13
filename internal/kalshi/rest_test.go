package kalshi

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// testREST returns a REST against h that doesn't really sleep: waits are
// recorded instead.
func testREST(t *testing.T, h http.HandlerFunc) (*REST, *[]time.Duration) {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	var mu sync.Mutex
	var waits []time.Duration
	r := &REST{Base: srv.URL, Interval: time.Nanosecond}
	r.sleep = func(ctx context.Context, d time.Duration) error {
		mu.Lock()
		waits = append(waits, d)
		mu.Unlock()
		return ctx.Err()
	}
	return r, &waits
}

// The market JSON is trimmed from live responses (November 2026).
const settledMarket = `{"ticker":"KXHIGHNY-26NOV11-T66","event_ticker":"KXHIGHNY-26NOV11","status":"finalized",
"result":"no","yes_sub_title":"67° or above","strike_type":"greater","floor_strike":66,
"open_time":"2026-11-10T15:00:00Z","close_time":"2026-11-12T06:00:00Z",
"settlement_ts":"2026-11-12T12:09:25.276474Z","settlement_value_dollars":"0.0000","volume_fp":"7078.05"}`

const unopenedMarket = `{"ticker":"KXNCAAWBSPREAD-26NOV12DUKENORF-45","event_ticker":"KXNCAAWBSPREAD-26NOV12DUKENORF",
"status":"initialized","result":"","yes_sub_title":"Duke wins by over 45.5 points","strike_type":"greater",
"floor_strike":45.5,"open_time":"2026-11-13T00:10:00Z","close_time":"2026-11-15T00:00:00Z","volume_fp":"0.00"}`

func TestMarketsPaginatesAndDecodes(t *testing.T) {
	var queries []string
	r, _ := testREST(t, func(w http.ResponseWriter, req *http.Request) {
		queries = append(queries, req.URL.Path+"?"+req.URL.RawQuery)
		switch req.URL.Query().Get("cursor") {
		case "":
			fmt.Fprintf(w, `{"cursor":"p2","markets":[%s,{"ticker":"BAD","open_time":"nope"}]}`, settledMarket)
		case "p2":
			fmt.Fprintf(w, `{"cursor":"","markets":[%s]}`, unopenedMarket)
		}
	})
	var logs []string
	r.Logf = func(f string, a ...any) { logs = append(logs, fmt.Sprintf(f, a...)) }

	got, err := r.Markets(context.Background(), MarketsQuery{Series: "KXHIGHNY", Status: "settled", MinSettled: time.Unix(1794000000, 0)})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d markets, want 2 (the undecodable one skipped)", len(got))
	}
	m := got[0]
	if m.Ticker != "KXHIGHNY-26NOV11-T66" || m.Event != "KXHIGHNY-26NOV11" || m.Status != "finalized" ||
		m.Result != "no" || m.Rung != "67° or above" || m.StrikeType != "greater" ||
		m.FloorStrike != "66" || m.CapStrike != "" {
		t.Errorf("settled market decoded as %+v", m)
	}
	if !m.Open.Equal(time.Date(2026, 11, 10, 15, 0, 0, 0, time.UTC)) || m.Settled.IsZero() {
		t.Errorf("times: open %v settled %v", m.Open, m.Settled)
	}
	if !m.HasSettlement || m.SettlementBps != 0 || m.VolumeCenti != 707805 {
		t.Errorf("settlement %v/%d volume %d", m.HasSettlement, m.SettlementBps, m.VolumeCenti)
	}
	u := got[1]
	if u.FloorStrike != "45.5" || u.HasSettlement || !u.Settled.IsZero() || u.Result != "" {
		t.Errorf("unopened market decoded as %+v", u)
	}
	if len(queries) != 2 || !strings.Contains(queries[0], "series_ticker=KXHIGHNY") ||
		!strings.Contains(queries[0], "status=settled") || !strings.Contains(queries[0], "min_settled_ts=1794000000") ||
		!strings.Contains(queries[0], "mve_filter=exclude") || !strings.Contains(queries[1], "cursor=p2") {
		t.Errorf("queries: %q", queries)
	}
	if len(logs) != 1 || !strings.Contains(logs[0], "BAD") {
		t.Errorf("logs: %q", logs)
	}
}

func TestMarketsHistoricalPath(t *testing.T) {
	var got string
	r, _ := testREST(t, func(w http.ResponseWriter, req *http.Request) {
		got = req.URL.Path + "?" + req.URL.RawQuery
		fmt.Fprint(w, `{"cursor":"","markets":[]}`)
	})
	if _, err := r.Markets(context.Background(), MarketsQuery{Series: "S", Status: "settled", Historical: true}); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(got, "/historical/markets?") || strings.Contains(got, "status=") {
		t.Errorf("historical query was %q; want /historical/markets with no status filter", got)
	}
}

func TestRetriesWithBackoffThenSucceeds(t *testing.T) {
	var calls atomic.Int32
	r, waits := testREST(t, func(w http.ResponseWriter, req *http.Request) {
		switch calls.Add(1) {
		case 1:
			w.WriteHeader(http.StatusTooManyRequests)
			fmt.Fprint(w, `{"error": "too many requests"}`)
		case 2:
			w.WriteHeader(http.StatusBadGateway)
		default:
			fmt.Fprint(w, `{"market_settled_ts":"2026-09-13T00:00:00Z"}`)
		}
	})
	var statuses []int
	r.OnResponse = func(s int) { statuses = append(statuses, s) }

	cut, err := r.HistoricalCutoff(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !cut.Equal(time.Date(2026, 9, 13, 0, 0, 0, 0, time.UTC)) {
		t.Errorf("cutoff %v", cut)
	}
	if fmt.Sprint(statuses) != "[429 502 200]" {
		t.Errorf("statuses %v, want [429 502 200]", statuses)
	}
	// Two backoff waits, each in the upper half of its window (250-500ms,
	// then 500ms-1s), plus the pacing wait the 429's hold-off caused.
	var backoffs []time.Duration
	for _, d := range *waits {
		if d >= backoffBase/2 {
			backoffs = append(backoffs, d)
		}
	}
	if len(backoffs) < 2 || backoffs[0] < 250*time.Millisecond || backoffs[0] > 500*time.Millisecond {
		t.Errorf("backoff waits %v", backoffs)
	}
}

func TestGivesUpAfterMaxRetries(t *testing.T) {
	var calls atomic.Int32
	r, _ := testREST(t, func(w http.ResponseWriter, req *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusTooManyRequests)
	})
	r.MaxRetries = 2
	_, err := r.SeriesList(context.Background(), time.Time{})
	if err == nil || !strings.Contains(err.Error(), "giving up after 3 attempts") {
		t.Fatalf("err = %v", err)
	}
	if calls.Load() != 3 {
		t.Errorf("%d calls, want 3", calls.Load())
	}
}

func TestClientErrorIsNotRetried(t *testing.T) {
	var calls atomic.Int32
	r, _ := testREST(t, func(w http.ResponseWriter, req *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, `{"error":"bad"}`)
	})
	if _, err := r.SeriesList(context.Background(), time.Time{}); err == nil || !strings.Contains(err.Error(), "HTTP 400") {
		t.Fatalf("err = %v", err)
	}
	if calls.Load() != 1 {
		t.Errorf("%d calls, want 1", calls.Load())
	}
}

func TestPageLimit(t *testing.T) {
	r, _ := testREST(t, func(w http.ResponseWriter, req *http.Request) {
		fmt.Fprint(w, `{"cursor":"again","markets":[]}`)
	})
	_, err := r.Markets(context.Background(), MarketsQuery{Status: "unopened"})
	if !errors.Is(err, ErrPageLimit) {
		t.Fatalf("err = %v, want ErrPageLimit", err)
	}
}

// Requests are spaced Interval apart and never more than restInFlight at
// once, however many goroutines call.
func TestPacingAndInFlightBound(t *testing.T) {
	var inFlight, peak atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		n := inFlight.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		time.Sleep(5 * time.Millisecond)
		inFlight.Add(-1)
		fmt.Fprint(w, `{"series":[]}`)
	}))
	defer srv.Close()
	r := &REST{Base: srv.URL, Interval: 2 * time.Millisecond}

	const calls = 12
	start := time.Now()
	var wg sync.WaitGroup
	for range calls {
		wg.Go(func() {
			if _, err := r.SeriesList(context.Background(), time.Time{}); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	if p := peak.Load(); p > restInFlight {
		t.Errorf("%d requests in flight at once, want at most %d", p, restInFlight)
	}
	if el := time.Since(start); el < (calls-1)*2*time.Millisecond {
		t.Errorf("%d calls took %v; pacing should make that at least %v", calls, el, (calls-1)*2*time.Millisecond)
	}
}

func TestSeriesAndEvents(t *testing.T) {
	var queries []string
	r, _ := testREST(t, func(w http.ResponseWriter, req *http.Request) {
		queries = append(queries, req.URL.Path+"?"+req.URL.RawQuery)
		switch req.URL.Path {
		case "/series":
			fmt.Fprint(w, `{"series":[{"ticker":"KXESGDPYOYF","title":"Spain GDP Growth Rate YoY","category":"Economics",
"frequency":"custom","tags":["GDP"],"last_updated_ts":"2026-10-23T18:27:49.671962Z","fee_type":"quadratic"}]}`)
		case "/events":
			fmt.Fprint(w, `{"cursor":"","events":[{"event_ticker":"KXHIGHNY-26NOV13","series_ticker":"KXHIGHNY",
"title":"Highest temperature in New York City on Nov 13, 2026?","sub_title":"On Nov 13, 2026","mutually_exclusive":true}],"milestones":[]}`)
		}
	})
	s, err := r.SeriesList(context.Background(), time.Unix(1700000000, 0))
	if err != nil || len(s) != 1 || s[0].Ticker != "KXESGDPYOYF" || s[0].Category != "Economics" || s[0].Frequency != "custom" || len(s[0].Tags) != 1 {
		t.Fatalf("series %+v, err %v", s, err)
	}
	ev, err := r.Events(context.Background(), []string{"KXHIGHNY-26NOV13"})
	if err != nil || len(ev) != 1 || ev[0].Series != "KXHIGHNY" || !ev[0].MutuallyExclusive {
		t.Fatalf("events %+v, err %v", ev, err)
	}
	if !strings.Contains(queries[0], "min_updated_ts=1700000000") || !strings.Contains(queries[1], "tickers=KXHIGHNY-26NOV13") {
		t.Errorf("queries %q", queries)
	}
}

func TestTradesSortedOldestFirst(t *testing.T) {
	var path string
	r, _ := testREST(t, func(w http.ResponseWriter, req *http.Request) {
		path = req.URL.Path
		// Newest first, as Kalshi returns them.
		fmt.Fprint(w, `{"cursor":"","trades":[
{"count_fp":"8.42","created_time":"2026-11-10T15:19:56.129003Z","no_price_dollars":"0.9900","taker_book_side":"ask","taker_outcome_side":"no","ticker":"T","trade_id":"b","yes_price_dollars":"0.0100"},
{"count_fp":"139.82","created_time":"2026-11-10T15:11:27.009858Z","no_price_dollars":"0.9800","taker_book_side":"bid","taker_outcome_side":"yes","ticker":"T","trade_id":"a","yes_price_dollars":"0.0200"}]}`)
	})
	from := time.Date(2026, 11, 10, 15, 0, 0, 0, time.UTC)
	ts, err := r.Trades(context.Background(), "T", from, from.Add(time.Hour), false)
	if err != nil {
		t.Fatal(err)
	}
	if path != "/markets/trades" || len(ts) != 2 || ts[0].ID != "a" || ts[0].YesBps != 200 || ts[0].CountCenti != 13982 || !ts[0].TakerYes || ts[1].TakerYes {
		t.Errorf("path %s trades %+v", path, ts)
	}
	if _, err := r.Trades(context.Background(), "T", from, from.Add(time.Hour), true); err != nil || path != "/historical/trades" {
		t.Errorf("historical path %s, err %v", path, err)
	}
}

// The live and historical candlestick endpoints name their fields
// differently; both shapes are from live responses.
func TestCandlesticksBothShapes(t *testing.T) {
	var path string
	r, _ := testREST(t, func(w http.ResponseWriter, req *http.Request) {
		path = req.URL.Path
		if strings.HasPrefix(path, "/historical/") {
			fmt.Fprint(w, `{"candlesticks":[{"end_period_ts":1789048860,"open_interest":"89.00",
"price":{"close":"0.0200","open":"0.0100","previous":null},"volume":"89.00",
"yes_ask":{"close":"0.0300","high":"1.0000","low":"0.0300","open":"1.0000"},
"yes_bid":{"close":"0.0100","high":"0.0200","low":"0.0100","open":"0.0100"}}]}`)
			return
		}
		fmt.Fprint(w, `{"candlesticks":[{"end_period_ts":1794322860,"open_interest_fp":"40.00",
"price":{"close_dollars":"0.0100"},"volume_fp":"40.00",
"yes_ask":{"close_dollars":"0.0200","high_dollars":"0.9900","low_dollars":"0.0200","open_dollars":"0.9900"},
"yes_bid":{"close_dollars":"0.0000","high_dollars":"0.0200","low_dollars":"0.0000","open_dollars":"0.0000"}}],"ticker":"T"}`)
	})
	from := time.Unix(1794322800, 0)
	live, err := r.Candlesticks(context.Background(), "KXHIGHNY", "T", from, from.Add(time.Hour), 1, false)
	if err != nil || len(live) != 1 {
		t.Fatalf("live: %+v, %v", live, err)
	}
	if path != "/series/KXHIGHNY/markets/T/candlesticks" {
		t.Errorf("live path %s", path)
	}
	if c := live[0]; c.End != 1794322860 || c.AskOpen != 9900 || c.AskClose != 200 || c.BidClose != 0 || c.VolumeCenti != 4000 || c.TwoSided() {
		t.Errorf("live candle %+v (an empty bid is not two-sided)", c)
	}
	hist, err := r.Candlesticks(context.Background(), "", "T", from, from.Add(time.Hour), 1, true)
	if err != nil || len(hist) != 1 {
		t.Fatalf("historical: %+v, %v", hist, err)
	}
	if path != "/historical/markets/T/candlesticks" {
		t.Errorf("historical path %s", path)
	}
	if c := hist[0]; c.BidClose != 100 || c.AskClose != 300 || c.AskOpen != 10000 || c.VolumeCenti != 8900 || !c.TwoSided() {
		t.Errorf("historical candle %+v", c)
	}
}

// The batch candlestick response is from a live call (November 2026):
// one entry per known market, unknown tickers left out.
func TestBatchCandlesticks(t *testing.T) {
	var got string
	r, _ := testREST(t, func(w http.ResponseWriter, req *http.Request) {
		got = req.URL.Path + "?" + req.URL.RawQuery
		fmt.Fprint(w, `{"markets":[{"candlesticks":[
{"end_period_ts":1794322920,"open_interest_fp":"40.00","price":{"previous_dollars":"0.0100"},"volume_fp":"0.00",
"yes_ask":{"close_dollars":"0.0200","high_dollars":"0.0200","low_dollars":"0.0200","open_dollars":"0.0200"},
"yes_bid":{"close_dollars":"0.0100","high_dollars":"0.0100","low_dollars":"0.0100","open_dollars":"0.0100"}},
{"end_period_ts":1794322860,"open_interest_fp":"40.00","price":{"close_dollars":"0.0100"},"volume_fp":"40.00",
"yes_ask":{"close_dollars":"0.0200","high_dollars":"0.9900","low_dollars":"0.0200","open_dollars":"0.9900"},
"yes_bid":{"close_dollars":"0.0100","high_dollars":"0.0200","low_dollars":"0.0000","open_dollars":"0.0000"}}],
"market_ticker":"KXHIGHNY-26NOV11-T66"}]}`)
	})
	from := time.Unix(1794322800, 0)
	cs, err := r.BatchCandlesticks(context.Background(), []string{"KXHIGHNY-26NOV11-T66", "NOPE-X"}, from, from.Add(5*time.Minute), 1)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(got, "/markets/candlesticks?") || !strings.Contains(got, "market_tickers=KXHIGHNY-26NOV11-T66%2CNOPE-X") ||
		!strings.Contains(got, "start_ts=1794322800") || !strings.Contains(got, "end_ts=1794323100") || !strings.Contains(got, "period_interval=1") {
		t.Errorf("request %q", got)
	}
	c := cs["KXHIGHNY-26NOV11-T66"]
	if len(cs) != 1 || len(c) != 2 || c[0].End != 1794322860 || c[0].BidClose != 100 || c[0].AskClose != 200 || !c[0].TwoSided() || c[0].VolumeCenti != 4000 {
		t.Errorf("candles %+v (want them sorted by time)", cs)
	}
	if _, err := r.BatchCandlesticks(context.Background(), make([]string, 101), from, from, 1); err == nil {
		t.Errorf("no error for 101 tickers")
	}
}
