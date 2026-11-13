package kalshi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	// restInterval is the default spacing between request starts: 2
	// requests a second. Kalshi documents 200 read tokens a second at 10
	// tokens a request for its lowest tier (20 a second), but these
	// endpoints are public and unauthenticated calls are charged to the
	// IP's budget, which is smaller and not documented: in November 2026
	// 4 a second drew occasional 429s from /markets/trades. The backoff
	// absorbs those, but it's better not to cause them.
	restInterval = 500 * time.Millisecond

	// restInFlight bounds how many requests are in flight at once.
	restInFlight = 2

	// restRetries is how many times a request is retried after a 429, a
	// 5xx or a transport error before the error is returned.
	restRetries = 5

	// restBodyLimit bounds a response body. The full series list is the
	// largest response (about 20MB in November 2026).
	restBodyLimit = 128 << 20

	// restPageLimit bounds one paginated call, so a cursor that never
	// ends does not loop forever.
	restPageLimit = 200
)

// ErrPageLimit is wrapped by a paginated call's error when it stopped at
// the page limit with more pages left. What it had read is still
// returned.
var ErrPageLimit = errors.New("page limit reached")

// REST reads Kalshi's public market-data endpoints (series, events,
// markets, trades, candlesticks). None of them needs authentication.
//
// It is bounded and backoff-aware: request starts are spaced Interval
// apart across all callers, at most restInFlight run at once, and a 429,
// a 5xx or a transport error is retried with the same jittered
// exponential backoff the websocket client reconnects with. A 429 also
// pushes every other caller's next request back by that backoff, so one
// rate-limit response slows the whole client down, not one goroutine.
// Kalshi documents no Retry-After header on 429s, only "apply
// exponential backoff".
//
// The zero value with Base set is ready to use. Safe for concurrent use.
type REST struct {
	Base string       // RESTBase outside tests
	HTTP *http.Client // nil: a client with a 60s timeout

	Interval   time.Duration // 0: restInterval
	MaxRetries int           // 0: restRetries

	// Logf receives a line per retry. Nil discards them.
	Logf func(format string, args ...any)

	// OnResponse is called with every response's status code, or 0 for
	// a transport error, retried ones included. For metrics; may be nil.
	OnResponse func(status int)

	once  sync.Once
	slots chan struct{}

	mu   sync.Mutex
	next time.Time // earliest start of the next request

	// Test hook; nil in production.
	sleep func(ctx context.Context, d time.Duration) error
}

var defaultRESTClient = &http.Client{Timeout: 60 * time.Second}

func (r *REST) init() {
	r.once.Do(func() { r.slots = make(chan struct{}, restInFlight) })
}

func (r *REST) wait(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	if r.sleep != nil {
		return r.sleep(ctx, d)
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// pace reserves the next request slot in time and waits for it.
func (r *REST) pace(ctx context.Context) error {
	interval := r.Interval
	if interval <= 0 {
		interval = restInterval
	}
	r.mu.Lock()
	now := time.Now()
	start := now
	if r.next.After(now) {
		start = r.next
	}
	r.next = start.Add(interval)
	r.mu.Unlock()
	return r.wait(ctx, start.Sub(now))
}

// holdOff makes every caller wait at least d before its next request.
func (r *REST) holdOff(d time.Duration) {
	r.mu.Lock()
	if t := time.Now().Add(d); t.After(r.next) {
		r.next = t
	}
	r.mu.Unlock()
}

// get fetches path?query and decodes the JSON body into out, retrying
// what is worth retrying.
func (r *REST) get(ctx context.Context, path string, query url.Values, out any) error {
	r.init()
	u := r.Base + path
	if len(query) > 0 {
		u += "?" + query.Encode()
	}
	retries := r.MaxRetries
	if retries <= 0 {
		retries = restRetries
	}
	for attempt := 0; ; attempt++ {
		status, body, err := r.do(ctx, u)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if r.OnResponse != nil {
			r.OnResponse(status)
		}
		switch {
		case err == nil && status == http.StatusOK:
			if err := json.Unmarshal(body, out); err != nil {
				return fmt.Errorf("kalshi: GET %s: decoding response: %w", path, err)
			}
			return nil
		case err == nil && status != http.StatusTooManyRequests && status < 500:
			return fmt.Errorf("kalshi: GET %s: HTTP %d: %s", path, status, snippet(body))
		}
		if err == nil {
			err = fmt.Errorf("HTTP %d: %s", status, snippet(body))
		}
		if attempt >= retries {
			return fmt.Errorf("kalshi: GET %s: giving up after %d attempts: %w", path, attempt+1, err)
		}
		delay := backoff(attempt, rand.Int64N)
		if status == http.StatusTooManyRequests {
			r.holdOff(delay)
		}
		if r.Logf != nil {
			r.Logf("kalshi: GET %s: %v; retrying in %v", path, err, delay.Round(time.Millisecond))
		}
		if err := r.wait(ctx, delay); err != nil {
			return err
		}
	}
}

// do makes one paced, slot-bounded request.
func (r *REST) do(ctx context.Context, u string) (status int, body []byte, err error) {
	if err := r.pace(ctx); err != nil {
		return 0, nil, err
	}
	select {
	case r.slots <- struct{}{}:
	case <-ctx.Done():
		return 0, nil, ctx.Err()
	}
	defer func() { <-r.slots }()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return 0, nil, err
	}
	client := r.HTTP
	if client == nil {
		client = defaultRESTClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	body, err = io.ReadAll(io.LimitReader(resp.Body, restBodyLimit))
	if err != nil {
		return 0, nil, fmt.Errorf("reading response: %w", err)
	}
	return resp.StatusCode, body, nil
}

func snippet(body []byte) string {
	const n = 200
	if len(body) > n {
		body = body[:n]
	}
	return strings.TrimSpace(string(body))
}

// paginate walks a cursor-paginated endpoint. page decodes one response
// and returns its cursor; an empty cursor ends the walk.
func (r *REST) paginate(ctx context.Context, path string, q url.Values, page func(body []byte) (cursor string, err error)) error {
	for n := 0; ; n++ {
		if n >= restPageLimit {
			return fmt.Errorf("kalshi: GET %s: %w after %d pages", path, ErrPageLimit, n)
		}
		var raw json.RawMessage
		if err := r.get(ctx, path, q, &raw); err != nil {
			return err
		}
		cursor, err := page(raw)
		if err != nil {
			return fmt.Errorf("kalshi: GET %s: decoding response: %w", path, err)
		}
		if cursor == "" {
			return nil
		}
		q.Set("cursor", cursor)
	}
}

// Series is one entry of GET /series.
type Series struct {
	Ticker      string   `json:"ticker"`
	Title       string   `json:"title"`
	Category    string   `json:"category"`
	Frequency   string   `json:"frequency"`
	Tags        []string `json:"tags,omitempty"`
	LastUpdated string   `json:"last_updated_ts,omitempty"` // RFC 3339, as Kalshi sent it

	// FeeType is quadratic, quadratic_with_maker_fees,
	// quadratic_with_combo_maker_fees or flat; FeeMultiplier "a floating
	// point multiplier applied to the fee calculations", kept as Kalshi
	// wrote the number.
	FeeType       string      `json:"fee_type,omitempty"`
	FeeMultiplier json.Number `json:"fee_multiplier,omitempty"`
}

// SeriesList fetches GET /series, which is not paginated: one response
// holds every series. minUpdated, if not zero, asks only for series
// updated since then (min_updated_ts).
func (r *REST) SeriesList(ctx context.Context, minUpdated time.Time) ([]Series, error) {
	q := url.Values{}
	if !minUpdated.IsZero() {
		q.Set("min_updated_ts", strconv.FormatInt(minUpdated.Unix(), 10))
	}
	var out struct {
		Series []Series `json:"series"`
	}
	if err := r.get(ctx, "/series", q, &out); err != nil {
		return nil, err
	}
	return out.Series, nil
}

// Event is one entry of GET /events.
type Event struct {
	Ticker            string `json:"event_ticker"`
	Series            string `json:"series_ticker"`
	Title             string `json:"title"`
	SubTitle          string `json:"sub_title"`
	MutuallyExclusive bool   `json:"mutually_exclusive"`
}

// eventsPerCall is how many event tickers go in one tickers= filter.
const eventsPerCall = 50

// Events fetches the given events by ticker (GET /events?tickers=...),
// in batches. Tickers Kalshi does not return (multivariate events are
// excluded from this endpoint) are simply absent from the result.
func (r *REST) Events(ctx context.Context, tickers []string) ([]Event, error) {
	var all []Event
	for len(tickers) > 0 {
		n := min(len(tickers), eventsPerCall)
		q := url.Values{"tickers": {strings.Join(tickers[:n], ",")}, "limit": {"200"}}
		tickers = tickers[n:]
		err := r.paginate(ctx, "/events", q, func(body []byte) (string, error) {
			var page struct {
				Events []Event `json:"events"`
				Cursor string  `json:"cursor"`
			}
			if err := json.Unmarshal(body, &page); err != nil {
				return "", err
			}
			all = append(all, page.Events...)
			return page.Cursor, nil
		})
		if err != nil {
			return all, err
		}
	}
	return all, nil
}

// Market is the part of Kalshi's market object the open scanner uses.
// Prices are basis points and sizes hundredths of a contract, as
// everywhere else.
type Market struct {
	Ticker string
	Event  string // event_ticker

	// Status is Kalshi's market status: initialized (not open yet),
	// inactive, active, closed, determined, disputed, amended, finalized.
	Status string

	// Result is "yes" or "no" once determined, "scalar" for scalar
	// markets, "" before.
	Result string

	// Rung is yes_sub_title: what this market's yes means within its
	// event ("67° or above").
	Rung string

	// StrikeType is greater, greater_or_equal, less, less_or_equal,
	// between, functional, custom or structured ("" if absent).
	// FloorStrike and CapStrike are the strike bounds as Kalshi wrote
	// the numbers ("66", "45.5"), "" if absent. They are not prices.
	StrikeType             string
	FloorStrike, CapStrike string

	Open, Close time.Time
	Settled     time.Time // zero until settled

	// SettlementBps is settlement_value_dollars, what a yes contract
	// paid; valid only if HasSettlement.
	SettlementBps int
	HasSettlement bool

	VolumeCenti int64
}

type marketWire struct {
	Ticker          string      `json:"ticker"`
	EventTicker     string      `json:"event_ticker"`
	Status          string      `json:"status"`
	Result          string      `json:"result"`
	YesSubTitle     string      `json:"yes_sub_title"`
	StrikeType      string      `json:"strike_type"`
	FloorStrike     json.Number `json:"floor_strike"`
	CapStrike       json.Number `json:"cap_strike"`
	OpenTime        string      `json:"open_time"`
	CloseTime       string      `json:"close_time"`
	SettlementTS    string      `json:"settlement_ts"`
	SettlementValue string      `json:"settlement_value_dollars"`
	VolumeFP        string      `json:"volume_fp"`
}

func (w marketWire) market() (Market, error) {
	m := Market{
		Ticker: w.Ticker, Event: w.EventTicker, Status: w.Status, Result: w.Result,
		Rung: w.YesSubTitle, StrikeType: w.StrikeType,
		FloorStrike: w.FloorStrike.String(), CapStrike: w.CapStrike.String(),
	}
	if m.Ticker == "" {
		return m, errors.New("market with no ticker")
	}
	var err error
	if m.Open, err = time.Parse(time.RFC3339, w.OpenTime); err != nil {
		return m, fmt.Errorf("market %s: open_time: %w", m.Ticker, err)
	}
	// The rest is optional: absent or unparseable leaves the zero value.
	m.Close, _ = time.Parse(time.RFC3339, w.CloseTime)
	m.Settled, _ = time.Parse(time.RFC3339, w.SettlementTS)
	if bps, ok := parseBps(w.SettlementValue); ok {
		m.SettlementBps, m.HasSettlement = bps, true
	}
	m.VolumeCenti, _ = parseFixedPoint(w.VolumeFP, 2)
	return m, nil
}

// parseBps converts a dollars string ("0.5600") to basis points. ok is
// false if it is empty, malformed, finer than a basis point, or outside
// 0-10000.
func parseBps(s string) (bps int, ok bool) {
	n, err := parseFixedPoint(s, 4)
	if err != nil || n < 0 || n > 10000 {
		return 0, false
	}
	return int(n), true
}

// MarketsQuery filters GET /markets. Kalshi allows one status at a time,
// and the settled-time filter only with status settled (or none).
type MarketsQuery struct {
	Series     string    // series_ticker
	Status     string    // unopened, open, paused, closed or settled
	Tickers    []string  // tickers, comma-separated on the wire
	MinSettled time.Time // min_settled_ts

	// Historical reads GET /historical/markets instead: markets settled
	// before the cutoff GET /historical/cutoff reports. It takes Series
	// and Tickers only.
	Historical bool
}

// Markets fetches every page of GET /markets (or /historical/markets)
// matching mq. Multivariate (combo) markets are excluded. A market that
// fails to decode is skipped and logged, so one odd market does not
// fail the scan.
func (r *REST) Markets(ctx context.Context, mq MarketsQuery) ([]Market, error) {
	q := url.Values{"limit": {"1000"}, "mve_filter": {"exclude"}}
	path := "/markets"
	if mq.Historical {
		path = "/historical/markets"
	}
	if mq.Series != "" {
		q.Set("series_ticker", mq.Series)
	}
	if len(mq.Tickers) > 0 {
		q.Set("tickers", strings.Join(mq.Tickers, ","))
	}
	if !mq.Historical {
		if mq.Status != "" {
			q.Set("status", mq.Status)
		}
		if !mq.MinSettled.IsZero() {
			q.Set("min_settled_ts", strconv.FormatInt(mq.MinSettled.Unix(), 10))
		}
	}
	var all []Market
	err := r.paginate(ctx, path, q, func(body []byte) (string, error) {
		var page struct {
			Markets []marketWire `json:"markets"`
			Cursor  string       `json:"cursor"`
		}
		if err := json.Unmarshal(body, &page); err != nil {
			return "", err
		}
		for _, w := range page.Markets {
			m, err := w.market()
			if err != nil {
				if r.Logf != nil {
					r.Logf("kalshi: skipping %v", err)
				}
				continue
			}
			all = append(all, m)
		}
		return page.Cursor, nil
	})
	return all, err
}

// HistoricalCutoff fetches GET /historical/cutoff's market_settled_ts:
// markets settled before it, their candlesticks and their trades are
// served by the /historical endpoints, not the live ones.
func (r *REST) HistoricalCutoff(ctx context.Context) (time.Time, error) {
	var out struct {
		MarketSettled string `json:"market_settled_ts"`
	}
	if err := r.get(ctx, "/historical/cutoff", nil, &out); err != nil {
		return time.Time{}, err
	}
	t, err := time.Parse(time.RFC3339, out.MarketSettled)
	if err != nil {
		return time.Time{}, fmt.Errorf("kalshi: GET /historical/cutoff: market_settled_ts: %w", err)
	}
	return t, nil
}

// Trade is one public trade.
type Trade struct {
	ID         string
	Ticker     string
	Time       time.Time // created_time, the venue's clock
	YesBps     int       // yes_price_dollars
	CountCenti int64     // count_fp
	TakerYes   bool      // taker_outcome_side == "yes": the taker bought yes
}

// Trades fetches a market's trades with from <= created_time <= to,
// oldest first. Kalshi returns trades newest first and has no sort
// parameter (the order is not documented; it is what the API did in
// November 2026), so every page in the window is read and the result is
// sorted here. Keep the window short. historical reads
// /historical/trades, for markets settled before the cutoff.
func (r *REST) Trades(ctx context.Context, ticker string, from, to time.Time, historical bool) ([]Trade, error) {
	q := url.Values{
		"ticker": {ticker}, "limit": {"1000"},
		"min_ts": {strconv.FormatInt(from.Unix(), 10)},
		"max_ts": {strconv.FormatInt(to.Unix(), 10)},
	}
	path := "/markets/trades"
	if historical {
		path = "/historical/trades"
	}
	var all []Trade
	err := r.paginate(ctx, path, q, func(body []byte) (string, error) {
		var page struct {
			Trades []struct {
				TradeID          string `json:"trade_id"`
				Ticker           string `json:"ticker"`
				CreatedTime      string `json:"created_time"`
				YesPriceDollars  string `json:"yes_price_dollars"`
				CountFP          string `json:"count_fp"`
				TakerOutcomeSide string `json:"taker_outcome_side"`
			} `json:"trades"`
			Cursor string `json:"cursor"`
		}
		if err := json.Unmarshal(body, &page); err != nil {
			return "", err
		}
		for _, w := range page.Trades {
			t, err := time.Parse(time.RFC3339, w.CreatedTime)
			bps, ok := parseBps(w.YesPriceDollars)
			count, cerr := parseFixedPoint(w.CountFP, 2)
			if err != nil || !ok || cerr != nil {
				continue // not usable as a price point
			}
			all = append(all, Trade{
				ID: w.TradeID, Ticker: w.Ticker, Time: t, YesBps: bps,
				CountCenti: count, TakerYes: w.TakerOutcomeSide == "yes",
			})
		}
		return page.Cursor, nil
	})
	// Oldest first, by ID within one timestamp so the order is stable.
	slices.SortFunc(all, func(a, b Trade) int {
		if c := a.Time.Compare(b.Time); c != 0 {
			return c
		}
		return strings.Compare(a.ID, b.ID)
	})
	return all, err
}

// Candle is one candlestick's yes bid and ask. A side that was empty
// reads as bid 0 or ask 10000, as Kalshi reports it.
type Candle struct {
	End               int64 // end_period_ts: the period's inclusive end, Unix seconds
	BidOpen, BidClose int
	AskOpen, AskClose int
	HasBid, HasAsk    bool  // the side's open and close both parsed
	VolumeCenti       int64 // contracts traded in the period
}

// TwoSided reports whether the candle closed with a real bid and ask.
func (c Candle) TwoSided() bool {
	return c.HasBid && c.HasAsk && c.BidClose > 0 && c.AskClose < 10000 && c.BidClose < c.AskClose
}

// ohlcWire is a candlestick side. The live endpoint names the fields
// open_dollars/close_dollars; the historical one, open/close.
type ohlcWire struct {
	OpenDollars  string `json:"open_dollars"`
	CloseDollars string `json:"close_dollars"`
	Open         string `json:"open"`
	Close        string `json:"close"`
}

func (o ohlcWire) parse() (open, close int, ok bool) {
	os, cs := o.OpenDollars, o.CloseDollars
	if os == "" && cs == "" {
		os, cs = o.Open, o.Close
	}
	open, ok1 := parseBps(os)
	close, ok2 := parseBps(cs)
	return open, close, ok1 && ok2
}

// Candlesticks fetches a market's candles with from <= period end <= to.
// periodMinutes is 1, 60 or 1440. The live endpoint
// (/series/{series}/markets/{ticker}/candlesticks) needs the series
// ticker; the historical one (/historical/markets/{ticker}/candlesticks,
// for markets settled before the cutoff) does not, and names its fields
// differently (see ohlcWire).
func (r *REST) Candlesticks(ctx context.Context, series, ticker string, from, to time.Time, periodMinutes int, historical bool) ([]Candle, error) {
	q := url.Values{
		"start_ts":        {strconv.FormatInt(from.Unix(), 10)},
		"end_ts":          {strconv.FormatInt(to.Unix(), 10)},
		"period_interval": {strconv.Itoa(periodMinutes)},
	}
	path := "/series/" + url.PathEscape(series) + "/markets/" + url.PathEscape(ticker) + "/candlesticks"
	if historical {
		path = "/historical/markets/" + url.PathEscape(ticker) + "/candlesticks"
	}
	var out struct {
		Candlesticks []candleWire `json:"candlesticks"`
	}
	if err := r.get(ctx, path, q, &out); err != nil {
		return nil, err
	}
	return decodeCandles(out.Candlesticks), nil
}

// candleWire is one candlestick as either endpoint sends it.
type candleWire struct {
	EndPeriodTS int64    `json:"end_period_ts"`
	YesBid      ohlcWire `json:"yes_bid"`
	YesAsk      ohlcWire `json:"yes_ask"`
	VolumeFP    string   `json:"volume_fp"`
	Volume      string   `json:"volume"`
}

func decodeCandles(ws []candleWire) []Candle {
	candles := make([]Candle, 0, len(ws))
	for _, w := range ws {
		c := Candle{End: w.EndPeriodTS}
		c.BidOpen, c.BidClose, c.HasBid = w.YesBid.parse()
		c.AskOpen, c.AskClose, c.HasAsk = w.YesAsk.parse()
		vol := w.VolumeFP
		if vol == "" {
			vol = w.Volume
		}
		c.VolumeCenti, _ = parseFixedPoint(vol, 2)
		candles = append(candles, c)
	}
	slices.SortFunc(candles, func(a, b Candle) int { return int(a.End - b.End) })
	return candles
}

// Limits of GET /markets/candlesticks, as documented: at most 100
// tickers a request, and at most 10,000 candlesticks in a response
// across all of them.
const (
	BatchCandleTickers = 100
	BatchCandleTotal   = 10000
)

// BatchCandlesticks fetches the candles of several markets over one
// window in one request (GET /markets/candlesticks), by ticker. All the
// markets share the window, so it suits the rungs of an event, which
// open together. Tickers Kalshi does not know, and markets with no
// candles in the window, are absent from the result.
//
// The caller keeps within the documented limits: at most
// BatchCandleTickers tickers, and few enough that tickers x candles in
// the window stays under BatchCandleTotal. It serves markets the live
// endpoints still have, not ones settled before the historical cutoff.
func (r *REST) BatchCandlesticks(ctx context.Context, tickers []string, from, to time.Time, periodMinutes int) (map[string][]Candle, error) {
	if len(tickers) == 0 {
		return nil, nil
	}
	if len(tickers) > BatchCandleTickers {
		return nil, fmt.Errorf("kalshi: GET /markets/candlesticks: %d tickers, at most %d a request", len(tickers), BatchCandleTickers)
	}
	q := url.Values{
		"market_tickers":  {strings.Join(tickers, ",")},
		"start_ts":        {strconv.FormatInt(from.Unix(), 10)},
		"end_ts":          {strconv.FormatInt(to.Unix(), 10)},
		"period_interval": {strconv.Itoa(periodMinutes)},
	}
	var out struct {
		Markets []struct {
			MarketTicker string       `json:"market_ticker"`
			Candlesticks []candleWire `json:"candlesticks"`
		} `json:"markets"`
	}
	if err := r.get(ctx, "/markets/candlesticks", q, &out); err != nil {
		return nil, err
	}
	byTicker := make(map[string][]Candle, len(out.Markets))
	for _, m := range out.Markets {
		byTicker[m.MarketTicker] = decodeCandles(m.Candlesticks)
	}
	return byTicker, nil
}
