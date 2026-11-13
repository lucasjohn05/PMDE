package opens

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/lucasjohn05/pmde/internal/kalshi"
	"github.com/lucasjohn05/pmde/internal/storage"
)

// Where a row's opening quote (or its price impact) came from.
const (
	SourceCapture = "capture" // the scanner's own record of the open: exact, with sizes
	SourceCandles = "candles" // Kalshi's 1-minute candlesticks: to the minute, no sizes
)

// OpenRow is one line of backfill.jsonl: one market's opening quote,
// its first trade, and how it settled. Prices are basis points, sizes
// hundredths of a contract; a nil field is unknown.
type OpenRow struct {
	Market string `json:"market"`
	Series string `json:"series"`
	Event  string `json:"event"`

	Rung       string `json:"rung,omitempty"`
	StrikeType string `json:"strike_type,omitempty"`
	Floor      string `json:"floor,omitempty"`
	Cap        string `json:"cap,omitempty"`

	OpenMs int64 `json:"open_ms"`

	// The opening quote: the first book with a two-sided quote under the
	// quote rule (RuleSpread, RuleSize). Source says where it is from,
	// "" if none was found. QuoteMs is when it appeared: the
	// venue's timestamp on the book event (capture), or the end of the
	// first 1-minute candle that closed two-sided (candles), so then it
	// is late by up to a minute and Bid/Ask are that minute's close.
	// Sizes exist only for captures: Kalshi serves no historical book.
	Source  string `json:"source,omitempty"`
	QuoteMs int64  `json:"quote_ms,omitempty"`
	Bid     *int   `json:"bid,omitempty"`
	Ask     *int   `json:"ask,omitempty"`
	Mid     *int   `json:"mid,omitempty"`
	BidSz   *int64 `json:"bid_sz,omitempty"`
	AskSz   *int64 `json:"ask_sz,omitempty"`

	// The QuoteRule the row was built under. A row built under another
	// rule is rebuilt. Candle quotes have no sizes, so they were held
	// to the spread cap only.
	RuleSpread int   `json:"rule_spread_bps,omitempty"`
	RuleSize   int64 `json:"rule_min_size,omitempty"`

	// The first trade within the horizon after the open, if any.
	TradeMs    int64  `json:"trade_ms,omitempty"`
	TradePrice *int   `json:"trade_price,omitempty"`
	TradeCount *int64 `json:"trade_count,omitempty"`
	TakerYes   *bool  `json:"taker_yes,omitempty"` // the taker bought yes

	// The mid just before that trade and after it: 5s after from a
	// capture, or the closes of the candles before and containing the
	// trade. Nil if either moment had no two-sided book.
	MidBefore    *int   `json:"mid_before,omitempty"`
	MidAfter     *int   `json:"mid_after,omitempty"`
	ImpactSource string `json:"impact_source,omitempty"`

	// CaptureSeen is true if the row was built with the market's capture
	// in hand, whether or not the capture had a usable opening quote. A
	// row built before the capture existed is rebuilt once.
	CaptureSeen bool `json:"capture_seen,omitempty"`

	// Result is "yes" or "no" once the market is determined ("scalar"
	// for scalar markets), "" before; the row is refreshed until then.
	Result    string `json:"result"`
	SettleBps *int   `json:"settle_bps,omitempty"` // what yes paid
	SettledMs int64  `json:"settled_ms,omitempty"`
}

// impactAfter is how long after the first trade a capture's mid is read
// for the price impact.
const impactAfter = 5 * time.Second

// tickersPerCall is how many market tickers go in one tickers= filter.
const tickersPerCall = 100

// Backfill builds backfill.jsonl: for every captured market, and every
// market of the allowlisted series settled since Since, one OpenRow.
//
// The opening quote comes from the scanner's capture when there is a
// clean one, and otherwise from Kalshi's 1-minute candlesticks; the
// first trade from the trades endpoint; the result from the market
// itself. Markets settled before Kalshi's historical cutoff are read
// from the /historical endpoints.
//
// Rows whose market has a result are final and are not fetched again,
// unless a capture has since become available for a row built without
// one. So a run is incremental, and can be stopped and rerun.
type Backfill struct {
	REST   *kalshi.REST
	Dir    string    // <data-dir>/opens
	Series []string  // the allowlist
	Since  time.Time // how far back to take settled markets

	// Horizon is how long after the open the first two-sided candle and
	// the first trade are looked for. Zero means an hour.
	Horizon time.Duration

	// Rule is what counts as the opening quote. Zero means
	// DefaultQuoteRule.
	Rule QuoteRule

	Logf func(format string, args ...any)

	cutoff time.Time
	files  map[string]map[string][]EventRecord // events file -> market -> its records; see events
}

// BackfillStats summarizes one Run.
type BackfillStats struct {
	Rows     int // rows in the file after the run
	Built    int // rows fetched this run
	Captured int // of those, with the opening quote from a capture
	Candles  int // of those, with the opening quote from candlesticks
	NoQuote  int // of those, with no two-sided quote found
	Failed   int // markets whose fetch failed; they are retried next run
}

func (b *Backfill) logf(format string, args ...any) {
	if b.Logf != nil {
		b.Logf(format, args...)
	}
}

// LoadRows reads backfill.jsonl; a missing file is no rows.
func LoadRows(dir string) ([]OpenRow, error) {
	rows, err := storage.ReadJSONL[OpenRow](filepath.Join(dir, backfillFile))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("opens: %w", err)
	}
	return rows, nil
}

// Run fetches what is missing and rewrites backfill.jsonl. The file is
// written even if the run fails or is cancelled part way, with whatever
// rows were built, so the work is kept.
func (b *Backfill) Run(ctx context.Context) (BackfillStats, error) {
	var stats BackfillStats
	old, err := LoadRows(b.Dir)
	if err != nil {
		return stats, err
	}
	rows := make(map[string]OpenRow, len(old))
	for _, r := range old {
		rows[r.Market] = r
	}
	captures, err := storage.ReadJSONL[CaptureRecord](filepath.Join(b.Dir, capturesFile))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return stats, fmt.Errorf("opens: %w", err)
	}
	captured := map[string]CaptureRecord{}
	for _, c := range captures {
		captured[c.Market] = c
	}
	b.files = map[string]map[string][]EventRecord{}

	runErr := b.run(ctx, rows, captured, &stats)

	out := make([]OpenRow, 0, len(rows))
	for _, r := range rows {
		out = append(out, r)
	}
	slices.SortFunc(out, func(a, c OpenRow) int {
		return cmp.Or(cmp.Compare(a.Series, c.Series), cmp.Compare(a.OpenMs, c.OpenMs), cmp.Compare(a.Market, c.Market))
	})
	stats.Rows = len(out)
	if err := storage.WriteJSONL(filepath.Join(b.Dir, backfillFile), out); err != nil {
		return stats, errors.Join(runErr, err)
	}
	return stats, runErr
}

type candidate struct {
	m      kalshi.Market
	series string
}

func (b *Backfill) run(ctx context.Context, rows map[string]OpenRow, captured map[string]CaptureRecord, stats *BackfillStats) error {
	var err error
	if b.cutoff, err = b.REST.HistoricalCutoff(ctx); err != nil {
		return err
	}
	if b.Rule == (QuoteRule{}) {
		b.Rule = DefaultQuoteRule()
	}
	// done reports whether a market's row needs nothing more.
	done := func(ticker string) bool {
		r, ok := rows[ticker]
		if !ok || r.Result == "" || !r.builtUnder(b.Rule) {
			return false
		}
		_, hasCapture := captured[ticker]
		return r.CaptureSeen || !hasCapture
	}

	var cands []candidate
	seen := map[string]bool{}
	add := func(series string, ms []kalshi.Market) {
		for _, m := range ms {
			if !seen[m.Ticker] && !done(m.Ticker) {
				seen[m.Ticker] = true
				cands = append(cands, candidate{m, series})
			}
		}
	}
	var errs []error

	// Captured markets without a finished row, looked up by ticker: the
	// live endpoint first, the historical one for what it no longer has.
	bySeries := map[string][]string{}
	for ticker, c := range captured {
		if !done(ticker) {
			bySeries[c.Series] = append(bySeries[c.Series], ticker)
		}
	}
	for series, tickers := range bySeries {
		slices.Sort(tickers)
		for chunk := range slices.Chunk(tickers, tickersPerCall) {
			found, err := b.REST.Markets(ctx, kalshi.MarketsQuery{Tickers: chunk})
			if err != nil {
				errs = append(errs, err)
				continue
			}
			add(series, found)
			var missing []string
			for _, t := range chunk {
				if !seen[t] {
					missing = append(missing, t)
				}
			}
			if len(missing) > 0 {
				found, err := b.REST.Markets(ctx, kalshi.MarketsQuery{Tickers: missing, Historical: true})
				if err != nil {
					errs = append(errs, err)
					continue
				}
				add(series, found)
			}
		}
	}

	// The allowlisted series' settled markets.
	for _, series := range b.Series {
		if ctx.Err() != nil {
			break
		}
		live, err := b.REST.Markets(ctx, kalshi.MarketsQuery{Series: series, Status: "settled", MinSettled: b.Since})
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", series, err))
			continue
		}
		add(series, live)
		if b.Since.Before(b.cutoff) {
			// The historical endpoint has no time filter: it returns
			// the series' whole archive, filtered here.
			hist, err := b.REST.Markets(ctx, kalshi.MarketsQuery{Series: series, Historical: true})
			if err != nil && !errors.Is(err, kalshi.ErrPageLimit) {
				errs = append(errs, fmt.Errorf("%s (historical): %w", series, err))
				continue
			}
			add(series, slices.DeleteFunc(hist, func(m kalshi.Market) bool { return m.Settled.Before(b.Since) }))
		}
	}

	slices.SortFunc(cands, func(a, c candidate) int {
		return cmp.Or(cmp.Compare(a.series, c.series), a.m.Open.Compare(c.m.Open), cmp.Compare(a.m.Ticker, c.m.Ticker))
	})
	b.logf("opens: backfilling %d market(s)", len(cands))
	for i, c := range cands {
		if ctx.Err() != nil {
			errs = append(errs, ctx.Err())
			break
		}
		_, hasCapture := captured[c.m.Ticker]
		row, err := b.row(ctx, c, hasCapture)
		if err != nil {
			stats.Failed++
			b.logf("opens: %s: %v", c.m.Ticker, err)
			continue
		}
		rows[row.Market] = row
		stats.Built++
		switch row.Source {
		case SourceCapture:
			stats.Captured++
		case SourceCandles:
			stats.Candles++
		default:
			stats.NoQuote++
		}
		if (i+1)%100 == 0 {
			b.logf("opens: backfilled %d of %d", i+1, len(cands))
		}
	}
	return errors.Join(errs...)
}

// row builds one market's OpenRow.
func (b *Backfill) row(ctx context.Context, c candidate, hasCapture bool) (OpenRow, error) {
	m := c.m
	row := OpenRow{
		Market: m.Ticker, Series: c.series, Event: m.Event,
		Rung: m.Rung, StrikeType: m.StrikeType, Floor: m.FloorStrike, Cap: m.CapStrike,
		OpenMs: m.Open.UnixMilli(), Result: m.Result,
		RuleSpread: b.Rule.MaxSpreadBps, RuleSize: b.Rule.MinSizeCenti,
	}
	if m.HasSettlement && m.Result != "" {
		row.SettleBps = &m.SettlementBps
	}
	if !m.Settled.IsZero() {
		row.SettledMs = m.Settled.UnixMilli()
	}
	horizon := b.Horizon
	if horizon <= 0 {
		horizon = time.Hour
	}
	historical := !m.Settled.IsZero() && m.Settled.Before(b.cutoff)
	end := m.Open.Add(horizon)

	var events []EventRecord
	if hasCapture {
		row.CaptureSeen = true
		var err error
		if events, err = b.events(c.series, m); err != nil {
			return row, err
		}
		if q, ok := openingFromEvents(events, b.Rule); ok {
			row.Source, row.QuoteMs = SourceCapture, q.ms
			row.Bid, row.Ask, row.Mid = &q.bid, &q.ask, ptr(mid(q.bid, q.ask))
			row.BidSz, row.AskSz = &q.bidSz, &q.askSz
		}
	}
	// Candles are fetched only when something needs them.
	var candles []kalshi.Candle
	haveCandles := false
	getCandles := func() ([]kalshi.Candle, error) {
		if !haveCandles {
			var err error
			if candles, err = b.REST.Candlesticks(ctx, c.series, m.Ticker, m.Open, end, 1, historical); err != nil {
				return nil, err
			}
			haveCandles = true
		}
		return candles, nil
	}
	if row.Source == "" {
		cs, err := getCandles()
		if err != nil {
			return row, err
		}
		for _, cd := range cs {
			if cd.TwoSided() && b.Rule.MeetsSpread(cd.BidClose, cd.AskClose) {
				row.Source, row.QuoteMs = SourceCandles, cd.End*1000
				row.Bid, row.Ask, row.Mid = &cd.BidClose, &cd.AskClose, ptr(mid(cd.BidClose, cd.AskClose))
				break
			}
		}
	}

	trades, err := b.REST.Trades(ctx, m.Ticker, m.Open.Add(-time.Second), end, historical)
	if err != nil && !errors.Is(err, kalshi.ErrPageLimit) {
		return row, err
	}
	if len(trades) == 0 {
		return row, nil
	}
	first := trades[0]
	// Everything that printed at the first trade's timestamp is one
	// taker order sweeping the book; its size is the sum.
	count := int64(0)
	for _, t := range trades {
		if t.Time.Equal(first.Time) && t.TakerYes == first.TakerYes {
			count += t.CountCenti
		}
	}
	row.TradeMs, row.TradePrice, row.TradeCount, row.TakerYes = first.Time.UnixMilli(), &first.YesBps, &count, &first.TakerYes

	if before, after, ok := impactFromEvents(events, first.Time.UnixMilli()); ok {
		row.MidBefore, row.MidAfter, row.ImpactSource = &before, &after, SourceCapture
		return row, nil
	}
	cs, err := getCandles()
	if err != nil {
		return row, err
	}
	if before, after, ok := impactFromCandles(cs, first.Time); ok {
		row.MidBefore, row.MidAfter, row.ImpactSource = &before, &after, SourceCandles
	}
	return row, nil
}

// events returns the captured events of market m, in file order.
func (b *Backfill) events(series string, m kalshi.Market) ([]EventRecord, error) {
	if !storage.SafeComponent(series) {
		return nil, nil
	}
	path := filepath.Join(b.Dir, eventsDir, series, m.Open.UTC().Format("2006-01-02")+".jsonl")
	byMarket, ok := b.files[path]
	if !ok {
		recs, err := storage.ReadJSONL[EventRecord](path)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("opens: %w", err)
		}
		byMarket = map[string][]EventRecord{}
		for _, r := range recs {
			byMarket[r.Market] = append(byMarket[r.Market], r)
		}
		// One day's file at a time is enough: candidates are sorted by
		// series and open time.
		clear(b.files)
		b.files[path] = byMarket
	}
	return byMarket[m.Ticker], nil
}

type quote struct {
	ms           int64
	bid, ask     int
	bidSz, askSz int64
}

func ptr[T any](v T) *T { return &v }

// mid is the midpoint in basis points. With 1c ticks bid+ask is even in
// bps; on a finer tick a half basis point rounds down.
func mid(bid, ask int) int { return (bid + ask) / 2 }

// eventMs is when a book event happened: the venue's timestamp, or the
// local receive time for records without one (snapshots).
func eventMs(r EventRecord) int64 {
	if r.TsMs > 0 {
		return r.TsMs
	}
	return r.RecvMs
}

// twoSided reports whether a record's book has any bid and ask at all,
// whatever their size: what the price impact reads.
func twoSided(r EventRecord) bool {
	return r.Stale == "" && r.Bid != nil && r.Ask != nil && *r.Bid < *r.Ask
}

// builtUnder reports whether the row was built under rule.
func (r OpenRow) builtUnder(rule QuoteRule) bool {
	return r.RuleSpread == rule.MaxSpreadBps && r.RuleSize == rule.MinSizeCenti
}

// meets reports whether the row's opening quote qualifies under rule:
// with its sizes if it has them (captures), on the spread alone if not
// (candles).
func (r OpenRow) meets(rule QuoteRule) bool {
	if r.Bid == nil || r.Ask == nil || r.Mid == nil {
		return false
	}
	if r.BidSz != nil && r.AskSz != nil {
		return rule.Meets(*r.Bid, *r.Ask, *r.BidSz, *r.AskSz)
	}
	return rule.MeetsSpread(*r.Bid, *r.Ask)
}

// impactFromEvents reads the mid in effect just before tradeMs and the
// mid impactAfter later from a capture. ok is false unless the capture
// covers both moments with a two-sided, trusted book.
func impactFromEvents(events []EventRecord, tradeMs int64) (before, after int, ok bool) {
	if len(events) == 0 {
		return 0, 0, false
	}
	if eventMs(events[len(events)-1]) < tradeMs {
		return 0, 0, false // the trade is past the end of the record
	}
	// pre is the book just before the trade; post the book impactAfter
	// later, which is the last event up to then (the trade's own book
	// change at least, since the trade is inside the record).
	afterMs := tradeMs + impactAfter.Milliseconds()
	var pre, post *EventRecord
	for i := range events {
		r := &events[i]
		t := eventMs(*r)
		if t < tradeMs {
			pre = r
		}
		if t <= afterMs {
			post = r
		}
	}
	if pre == nil || post == nil || !twoSided(*pre) || !twoSided(*post) {
		return 0, 0, false
	}
	return mid(*pre.Bid, *pre.Ask), mid(*post.Bid, *post.Ask), true
}

// impactFromCandles reads the mid before and after a trade from
// 1-minute candles: the close of the candle before the one containing
// the trade (or that candle's own open, if it is the first), and that
// candle's close.
func impactFromCandles(candles []kalshi.Candle, trade time.Time) (before, after int, ok bool) {
	sec := trade.Unix()
	for i, c := range candles {
		if c.End < sec {
			continue
		}
		if !c.TwoSided() {
			return 0, 0, false
		}
		after = mid(c.BidClose, c.AskClose)
		if i > 0 && candles[i-1].TwoSided() {
			return mid(candles[i-1].BidClose, candles[i-1].AskClose), after, true
		}
		if c.BidOpen > 0 && c.AskOpen < 10000 && c.BidOpen < c.AskOpen {
			return mid(c.BidOpen, c.AskOpen), after, true
		}
		return 0, 0, false
	}
	return 0, 0, false
}
