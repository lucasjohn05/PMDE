package opens

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/lucasjohn05/pmde/internal/kalshi"
	"github.com/lucasjohn05/pmde/internal/storage"
)

// triageFile holds the rows triage has fetched, so a rerun (or a run
// that was stopped) does not fetch them again. It is separate from
// backfill.jsonl: these rows are candle-only by design and have no
// trades, and must not shadow rows built from captures.
const triageFile = "triage.jsonl"

// Triage is a cheap first pass over many series, to decide which are
// worth pointing the scanner at. It needs no key and no captures: it
// lists each series' settled markets and reads their first minutes from
// Kalshi's 1-minute candlesticks.
//
// Per series it reports how often markets and events open, how often an
// open has a two-sided quote within its first minute (whether anyone
// makes the open at all), the opening spread, and how well a normal CDF
// fits the opening mids across an event's rungs.
//
// Counting is done on the full listing; the candles are fetched for a
// sample of whole events, evenly spaced over the period, so that a
// series with thousands of markets costs about as much as one with
// dozens. An event's rungs open together, so one batch request covers
// up to 100 of them.
type Triage struct {
	REST   *kalshi.REST
	Dir    string // <data-dir>/opens
	Series []string

	// Since is how far back to take settled markets. Zero means Kalshi's
	// historical cutoff, which is as far back as the live endpoints this
	// uses reach.
	Since time.Time

	// Horizon is how long after the open the opening quote is looked
	// for. Zero means 10 minutes.
	Horizon time.Duration

	// SampleEvents is how many events per series have their candles
	// fetched. Zero means 20; negative means every event.
	SampleEvents int

	// Rule is what counts as a two-sided quote. Candles have no sizes,
	// so only its spread cap applies. Zero means DefaultQuoteRule.
	Rule QuoteRule

	Logf func(format string, args ...any)
}

// TriageRow is one series' line.
type TriageRow struct {
	Series string

	// From the full listing of markets settled since Since.
	Opens        int
	Events       int
	Days         int // UTC days from the first open to the last, inclusive
	OpensPerDay  float64
	EventsPerDay float64

	// Truncated is true if the listing stopped at the page limit: the
	// counts cover only what was listed (the rates are still per day of
	// what was listed).
	Truncated bool

	// From the sampled events' candles.
	Sampled       int // markets
	SampledEvents int

	// QuotedFirstMinute is the share of sampled opens whose first
	// 1-minute candle (the one ending within 60s of open_time) closed
	// with a bid and an ask. For a market that does not open on a
	// minute boundary that candle is shorter than a minute, so this
	// errs low. Quoted is the share with a two-sided close anywhere in
	// the horizon.
	QuotedFirstMinute float64
	Quoted            float64

	SpreadBps  float64 // median opening spread
	FitEvents  int
	FitRMSEBps float64 // median over the sampled events with at least 3 quoted rungs

	Err string // why the series could not be triaged, if it could not
}

func (t *Triage) logf(format string, args ...any) {
	if t.Logf != nil {
		t.Logf(format, args...)
	}
}

// Run triages every series. A series that fails is reported in its
// row's Err and does not stop the others; the error returned is for
// failures that stop the run (cancellation, or the cutoff lookup).
func (t *Triage) Run(ctx context.Context) ([]TriageRow, error) {
	since := t.Since
	if since.IsZero() {
		var err error
		if since, err = t.REST.HistoricalCutoff(ctx); err != nil {
			return nil, err
		}
	}
	horizon := t.Horizon
	if horizon <= 0 {
		horizon = 10 * time.Minute
	}
	sample := t.SampleEvents
	if sample == 0 {
		sample = 20
	}
	if t.Rule == (QuoteRule{}) {
		t.Rule = DefaultQuoteRule()
	}

	path := filepath.Join(t.Dir, triageFile)
	old, err := storage.ReadJSONL[OpenRow](path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("opens: %w", err)
	}
	have := make(map[string]OpenRow, len(old))
	for _, r := range old {
		have[r.Market] = r
	}
	save := func() error {
		rows := make([]OpenRow, 0, len(have))
		for _, r := range have {
			rows = append(rows, r)
		}
		slices.SortFunc(rows, func(a, b OpenRow) int {
			return cmp.Or(cmp.Compare(a.Series, b.Series), cmp.Compare(a.OpenMs, b.OpenMs), cmp.Compare(a.Market, b.Market))
		})
		return storage.WriteJSONL(path, rows)
	}

	var out []TriageRow
	for i, series := range t.Series {
		if ctx.Err() != nil {
			return out, errors.Join(ctx.Err(), save())
		}
		row, fetched := t.series(ctx, series, since, horizon, sample, have)
		out = append(out, row)
		if row.Err != "" {
			t.logf("opens: triage %s: %s", series, row.Err)
		} else {
			t.logf("opens: triage %d/%d %s: %d opens in %d events, %d sampled", i+1, len(t.Series), series, row.Opens, row.Events, row.Sampled)
		}
		if fetched > 0 {
			// After every series that fetched anything, so a long run
			// that dies keeps what it has.
			if err := save(); err != nil {
				return out, err
			}
		}
	}
	return out, nil
}

// series triages one series. fetched is how many rows it added to have.
func (t *Triage) series(ctx context.Context, series string, since time.Time, horizon time.Duration, sample int, have map[string]OpenRow) (row TriageRow, fetched int) {
	row.Series = series
	markets, err := t.REST.Markets(ctx, kalshi.MarketsQuery{Series: series, Status: "settled", MinSettled: since})
	switch {
	case errors.Is(err, kalshi.ErrPageLimit):
		row.Truncated = true
	case err != nil:
		row.Err = err.Error()
		return row, 0
	}
	if len(markets) == 0 {
		row.Err = "no markets settled since " + since.UTC().Format("2006-01-02")
		return row, 0
	}

	// Count on the full listing.
	byEvent := map[string][]kalshi.Market{}
	first, last := markets[0].Open, markets[0].Open
	for _, m := range markets {
		key := m.Event
		if key == "" {
			key = m.Ticker
		}
		byEvent[key] = append(byEvent[key], m)
		if m.Open.Before(first) {
			first = m.Open
		}
		if m.Open.After(last) {
			last = m.Open
		}
	}
	row.Opens, row.Events = len(markets), len(byEvent)
	row.Days = int(utcDay(last.UnixMilli())-utcDay(first.UnixMilli())) + 1
	row.OpensPerDay = float64(row.Opens) / float64(row.Days)
	row.EventsPerDay = float64(row.Events) / float64(row.Days)

	// Sample whole events, evenly spaced in time.
	type event struct {
		name    string
		open    time.Time
		markets []kalshi.Market
	}
	events := make([]event, 0, len(byEvent))
	for name, ms := range byEvent {
		slices.SortFunc(ms, func(a, b kalshi.Market) int { return cmp.Or(a.Open.Compare(b.Open), cmp.Compare(a.Ticker, b.Ticker)) })
		events = append(events, event{name, ms[0].Open, ms})
	}
	slices.SortFunc(events, func(a, b event) int { return cmp.Or(a.open.Compare(b.open), cmp.Compare(a.name, b.name)) })
	if sample > 0 && len(events) > sample {
		picked := make([]event, 0, sample)
		for i := range sample {
			// i = 0 picks the first event and i = sample-1 the last.
			picked = append(picked, events[i*(len(events)-1)/(sample-1+boolInt(sample == 1))])
		}
		events = picked
	}
	row.SampledEvents = len(events)

	// Fetch what is not on file, the markets that share an open_time in
	// one request.
	perCall := min(kalshi.BatchCandleTickers, kalshi.BatchCandleTotal/(int(horizon/time.Minute)+2))
	var rows []OpenRow
	for _, e := range events {
		byOpen := map[int64][]kalshi.Market{}
		for _, m := range e.markets {
			if r, ok := have[m.Ticker]; ok && r.builtUnder(t.Rule) {
				rows = append(rows, r)
				continue
			}
			byOpen[m.Open.Unix()] = append(byOpen[m.Open.Unix()], m)
		}
		for _, group := range byOpen {
			for chunk := range slices.Chunk(group, perCall) {
				tickers := make([]string, len(chunk))
				for i, m := range chunk {
					tickers[i] = m.Ticker
				}
				open := chunk[0].Open
				candles, err := t.REST.BatchCandlesticks(ctx, tickers, open, open.Add(horizon), 1)
				if err != nil {
					row.Err = err.Error()
					return row, fetched
				}
				for _, m := range chunk {
					r := candleRow(series, m, candles[m.Ticker], t.Rule)
					have[m.Ticker] = r
					rows = append(rows, r)
					fetched++
				}
			}
		}
	}

	row.Sampled = len(rows)
	var spreads []float64
	quoted, firstMinute := 0, 0
	rungs := map[string][]OpenRow{}
	for _, r := range rows {
		if r.Mid == nil {
			continue
		}
		quoted++
		if r.QuoteMs-r.OpenMs <= time.Minute.Milliseconds() {
			firstMinute++
		}
		spreads = append(spreads, float64(*r.Ask-*r.Bid))
		rungs[r.Event] = append(rungs[r.Event], r)
	}
	if row.Sampled > 0 {
		row.Quoted = float64(quoted) / float64(row.Sampled)
		row.QuotedFirstMinute = float64(firstMinute) / float64(row.Sampled)
	}
	row.SpreadBps = median(spreads)
	var rmses []float64
	for _, rs := range rungs {
		if rmse, ok := normalFitRMSE(rs); ok {
			rmses = append(rmses, rmse)
		}
	}
	row.FitEvents, row.FitRMSEBps = len(rmses), median(rmses)
	return row, fetched
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// candleRow builds a market's row from its candles alone: the opening
// quote is the close of the first candle that closes two-sided within
// rule's spread cap. It is the candle path of Backfill.row, without the
// trades.
func candleRow(series string, m kalshi.Market, candles []kalshi.Candle, rule QuoteRule) OpenRow {
	row := OpenRow{
		Market: m.Ticker, Series: series, Event: m.Event,
		Rung: m.Rung, StrikeType: m.StrikeType, Floor: m.FloorStrike, Cap: m.CapStrike,
		OpenMs: m.Open.UnixMilli(), Result: m.Result,
		RuleSpread: rule.MaxSpreadBps, RuleSize: rule.MinSizeCenti,
	}
	if m.HasSettlement && m.Result != "" {
		row.SettleBps = &m.SettlementBps
	}
	if !m.Settled.IsZero() {
		row.SettledMs = m.Settled.UnixMilli()
	}
	for _, c := range candles {
		if c.TwoSided() && rule.MeetsSpread(c.BidClose, c.AskClose) {
			row.Source, row.QuoteMs = SourceCandles, c.End*1000
			row.Bid, row.Ask, row.Mid = &c.BidClose, &c.AskClose, ptr(mid(c.BidClose, c.AskClose))
			break
		}
	}
	return row
}

// SortTriage orders rows for printing: by the share of opens quoted in
// the first minute, then by opens per day, with failed series last.
func SortTriage(rows []TriageRow) {
	slices.SortFunc(rows, func(a, b TriageRow) int {
		return cmp.Or(
			cmp.Compare(boolInt(a.Err != ""), boolInt(b.Err != "")),
			cmp.Compare(b.QuotedFirstMinute, a.QuotedFirstMinute),
			cmp.Compare(b.OpensPerDay, a.OpensPerDay),
			cmp.Compare(a.Series, b.Series),
		)
	})
}

// WriteTriage prints one line per series.
func WriteTriage(w io.Writer, rows []TriageRow) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', tabwriter.AlignRight)
	fmt.Fprintln(tw, "series\topens/day\tevents/day\topens\tevents\tsampled\tquoted 1m\tquoted\tspread\tfit rmse\t")
	var failed []TriageRow
	for _, r := range rows {
		if r.Err != "" {
			failed = append(failed, r)
			continue
		}
		opens := fmt.Sprint(r.Opens)
		if r.Truncated {
			opens += "+"
		}
		fmt.Fprintf(tw, "%s\t%.1f\t%.1f\t%s\t%d\t%d\t%.0f%%\t%.0f%%\t%s\t%s\t\n",
			r.Series, r.OpensPerDay, r.EventsPerDay, opens, r.Events, r.Sampled,
			r.QuotedFirstMinute*100, r.Quoted*100,
			na(r.Quoted > 0, "%.0f", r.SpreadBps), na(r.FitEvents > 0, "%.0f (%d)", r.FitRMSEBps, r.FitEvents))
	}
	if err := tw.Flush(); err != nil {
		return err
	}
	fmt.Fprint(w, `
opens, events: markets settled in the period and the events they belong to ("+": the listing hit the
page limit); /day: per UTC day over the span seen. sampled: markets whose candles were read (whole
events, evenly spaced). quoted 1m: share of sampled opens with a two-sided quote at the close of the
1-minute candle ending within 60s of open_time; quoted: within the whole horizon. A quote counts only
within the quote rule's spread cap; candles have no sizes, so the rule's minimum size is not checked.
spread: median opening spread, bps. fit rmse: median RMSE, bps, of a normal-CDF fit to an event's
opening mids across its rungs (events fitted). All from 1-minute candle closes: to the minute.
`)
	for _, r := range failed {
		fmt.Fprintf(w, "%s: %s\n", r.Series, r.Err)
	}
	return nil
}

// Passing returns the series whose first-minute quoted share is at
// least minShare, in the rows' order.
func Passing(rows []TriageRow, minShare float64) []string {
	var out []string
	for _, r := range rows {
		if r.Err == "" && r.Sampled > 0 && r.QuotedFirstMinute >= minShare {
			out = append(out, r.Series)
		}
	}
	return out
}

// WriteAllowlist writes a complete scanner config (an opens.yaml) with
// series as its allowlist, rule as its quote rule, and every other
// setting at its default, spelled out. note is put in the header
// comment.
func WriteAllowlist(w io.Writer, series []string, note string, rule QuoteRule) error {
	d := DefaultConfig()
	d.Quote = rule
	var b strings.Builder
	b.WriteString("# Open scanner config, written by pmde-opens triage.\n")
	for _, line := range strings.Split(strings.TrimSpace(note), "\n") {
		b.WriteString("# " + line + "\n")
	}
	b.WriteString("# See docs/open-scanner.md for the settings.\n\nseries:\n")
	for _, s := range series {
		b.WriteString("  - " + s + "\n")
	}
	if len(series) == 0 {
		b.WriteString("  []\n")
	}
	fmt.Fprintf(&b, "\ninclude_new_series: %v\nnew_series_days: %d\nmax_new_series: %d\n\nlead: %s\nwindow: %s\n\nmax_concurrent: %d\n\nmarket_poll: %s\nseries_poll: %s\n\nquote:\n  max_spread_bps: %d\n  min_size: %s\n",
		d.IncludeNewSeries, int(d.NewSeriesFor/(24*time.Hour)), d.MaxNewSeries,
		d.Lead, d.Window, d.MaxConcurrent, d.MarketPoll, d.SeriesPoll,
		d.Quote.MaxSpreadBps, centi(d.Quote.MinSizeCenti))
	_, err := io.WriteString(w, b.String())
	return err
}

// ReadSeriesList reads series tickers, one per line, from r: the
// format the series command prints. Blank lines and # comments are
// skipped, and only the first field of a line is taken, so the
// command's annotated output can be fed straight back in.
func ReadSeriesList(r io.Reader) ([]string, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	var out []string
	seen := map[string]bool{}
	for _, line := range strings.Split(string(data), "\n") {
		if i := strings.IndexByte(line, '#'); i >= 0 {
			line = line[:i]
		}
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		s := fields[0]
		if !storage.SafeComponent(s) {
			return nil, fmt.Errorf("%q is not a series ticker", s)
		}
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	return out, nil
}

// Candidates lists the series of the latest snapshot under dir whose
// frequency is one of frequencies (all of them if none are given),
// sorted by ticker. day is the snapshot's; it is "" if there is no
// snapshot.
func Candidates(dir string, frequencies []string) (series []kalshi.Series, day string, err error) {
	path, day, err := LatestSnapshot(dir)
	if err != nil || path == "" {
		return nil, "", err
	}
	snap, err := storage.ReadJSONL[kalshi.Series](path)
	if err != nil {
		return nil, "", fmt.Errorf("opens: %w", err)
	}
	for _, s := range snap {
		if (len(frequencies) == 0 || slices.Contains(frequencies, s.Frequency)) && storage.SafeComponent(s.Ticker) {
			series = append(series, s)
		}
	}
	slices.SortFunc(series, func(a, b kalshi.Series) int { return cmp.Compare(a.Ticker, b.Ticker) })
	return series, day, nil
}
