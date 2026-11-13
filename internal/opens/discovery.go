package opens

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/lucasjohn05/pmde/internal/kalshi"
	"github.com/lucasjohn05/pmde/internal/metrics"
	"github.com/lucasjohn05/pmde/internal/storage"
)

// File and directory names under the opens directory.
const (
	seriesDir      = "series"
	newSeriesFile  = "new_series.jsonl"
	eventsMetaFile = "events.jsonl"
	eventsDir      = "events"
	capturesFile   = "captures.jsonl"
	backfillFile   = "backfill.jsonl"
)

// seriesOverlap is how far before the last series poll an incremental
// poll's min_updated_ts reaches, so a series updated while the last
// poll was in flight (or sitting in Kalshi's response cache) isn't
// missed.
const seriesOverlap = 10 * time.Minute

// NewSeriesRecord is one line of new_series.jsonl: a series that was in
// no earlier snapshot.
type NewSeriesRecord struct {
	T         int64  `json:"t"` // when it was first seen, Unix ms
	Ticker    string `json:"ticker"`
	Title     string `json:"title"`
	Category  string `json:"category"`
	Frequency string `json:"frequency"`
}

// Discovery polls Kalshi's REST API for series, markets and events, and
// keeps the Schedule of markets about to open in the watched series:
// the config's allowlist plus, if enabled, recently detected series.
//
// It persists a snapshot of the whole series list once per UTC day and
// logs every series that was not in an earlier snapshot. One goroutine
// (Run) owns it; the Schedule is what it shares.
type Discovery struct {
	REST     *kalshi.REST
	Dir      string // <data-dir>/opens
	Cfg      Config
	Schedule *Schedule
	Metrics  *metrics.Opens
	Logf     func(format string, args ...any)
	Now      func() time.Time // nil: time.Now

	mu sync.Mutex // guards newSeries and newInfo, which NewSeries reads from other goroutines

	known      map[string]bool            // every series ticker seen so far
	baseline   bool                       // a snapshot exists, so an unknown series is a new one
	newSeries  map[string]time.Time       // newly detected series -> first seen; guarded by mu
	newInfo    map[string]NewSeriesRecord // the same series' records; guarded by mu
	snapDay    string                     // UTC day of the last snapshot written
	lastSeries time.Time                  // start of the last successful series poll
	watched    map[string]bool            // as of the last market poll
	events     map[string]bool            // event tickers already written to events.jsonl
}

func (d *Discovery) now() time.Time {
	if d.Now != nil {
		return d.Now()
	}
	return time.Now()
}

func (d *Discovery) logf(format string, args ...any) {
	if d.Logf != nil {
		d.Logf(format, args...)
	}
}

// Load restores what earlier runs learned: the latest series snapshot
// and the new-series log. Call it once before polling.
func (d *Discovery) Load() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.known, d.newSeries, d.newInfo = map[string]bool{}, map[string]time.Time{}, map[string]NewSeriesRecord{}
	d.watched, d.events = map[string]bool{}, map[string]bool{}

	names, err := filepath.Glob(filepath.Join(d.Dir, seriesDir, "*.jsonl"))
	if err != nil {
		return fmt.Errorf("opens: %w", err)
	}
	if len(names) > 0 {
		slices.Sort(names) // YYYY-MM-DD sorts by date
		latest := names[len(names)-1]
		snap, err := storage.ReadJSONL[kalshi.Series](latest)
		if err != nil {
			return fmt.Errorf("opens: %w", err)
		}
		for _, s := range snap {
			d.known[s.Ticker] = true
		}
		d.baseline = true
		d.snapDay = strings.TrimSuffix(filepath.Base(latest), ".jsonl")
	}
	recs, err := storage.ReadJSONL[NewSeriesRecord](filepath.Join(d.Dir, newSeriesFile))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("opens: %w", err)
	}
	for _, r := range recs {
		d.known[r.Ticker] = true
		d.newSeries[r.Ticker] = time.UnixMilli(r.T)
		d.newInfo[r.Ticker] = r
	}
	evs, err := storage.ReadJSONL[kalshi.Event](filepath.Join(d.Dir, eventsMetaFile))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("opens: %w", err)
	}
	for _, e := range evs {
		d.events[e.Ticker] = true
	}
	return nil
}

// Run polls until ctx is cancelled: everything once at the start, then
// the series list every SeriesPoll and the watched series' unopened
// markets every MarketPoll. A failed poll is logged and counted, and
// tried again at the next interval; the REST client has already retried
// with backoff by then.
func (d *Discovery) Run(ctx context.Context) {
	d.poll(ctx, true)
	markets := time.NewTicker(d.Cfg.MarketPoll)
	defer markets.Stop()
	series := time.NewTicker(d.Cfg.SeriesPoll)
	defer series.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-series.C:
			d.poll(ctx, true)
		case <-markets.C:
			d.poll(ctx, false)
		}
	}
}

func (d *Discovery) poll(ctx context.Context, series bool) {
	if series {
		if err := d.PollSeries(ctx); err != nil && ctx.Err() == nil {
			d.Metrics.PollError("series")
			d.logf("opens: series poll: %v", err)
		}
	}
	if err := d.PollMarkets(ctx); err != nil && ctx.Err() == nil {
		d.logf("opens: market poll: %v", err)
	}
}

// PollSeries fetches the series list and records what is new. The first
// poll of each UTC day fetches the whole list and writes it as that
// day's snapshot, as does the first poll after a restart; later polls
// that day ask only for series updated since the last poll.
//
// The very first snapshot is the baseline: with nothing to compare
// against, none of its series counts as new.
func (d *Discovery) PollSeries(ctx context.Context) error {
	start := d.now()
	day := start.UTC().Format("2006-01-02")
	// Also full on the first poll after a restart: there is no last
	// poll to be incremental from. It rewrites today's snapshot.
	full := day != d.snapDay || d.lastSeries.IsZero()
	var since time.Time
	if !full {
		since = d.lastSeries.Add(-seriesOverlap)
	}
	list, err := d.REST.SeriesList(ctx, since)
	if err != nil {
		return err
	}
	if full {
		if len(list) == 0 {
			return errors.New("the series list came back empty; not writing a snapshot")
		}
		slices.SortFunc(list, func(a, b kalshi.Series) int { return cmp.Compare(a.Ticker, b.Ticker) })
		if err := storage.WriteJSONL(filepath.Join(d.Dir, seriesDir, day+".jsonl"), list); err != nil {
			return err
		}
		d.snapDay = day
		d.logf("opens: wrote the %s series snapshot (%d series)", day, len(list))
	}
	for _, s := range list {
		if s.Ticker == "" || d.known[s.Ticker] {
			continue
		}
		d.known[s.Ticker] = true
		if !d.baseline {
			continue
		}
		rec := NewSeriesRecord{T: start.UnixMilli(), Ticker: s.Ticker, Title: s.Title, Category: s.Category, Frequency: s.Frequency}
		if err := storage.AppendLine(filepath.Join(d.Dir, newSeriesFile), rec); err != nil {
			return err
		}
		d.mu.Lock()
		d.newSeries[s.Ticker], d.newInfo[s.Ticker] = start, rec
		d.mu.Unlock()
		d.Metrics.NewSeries()
		d.logf("opens: new series %s (%s; %s, %s)", s.Ticker, s.Title, s.Category, s.Frequency)
	}
	d.baseline = true
	d.lastSeries = start
	return nil
}

// NewSeries returns the record of a series detected as new, whenever
// that was; ok is false for a series that was never new. Safe to call
// from any goroutine.
func (d *Discovery) NewSeries(ticker string) (rec NewSeriesRecord, ok bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	rec, ok = d.newInfo[ticker]
	return rec, ok
}

// Watched is the series whose opens are captured: the allowlist, plus
// (with IncludeNewSeries) the series detected within NewSeriesFor, the
// most recent MaxNewSeries of them. Sorted.
func (d *Discovery) Watched() []string {
	set := map[string]bool{}
	for _, s := range d.Cfg.Series {
		set[s] = true
	}
	if d.Cfg.IncludeNewSeries {
		now := d.now()
		type seen struct {
			ticker string
			at     time.Time
		}
		var recent []seen
		d.mu.Lock()
		defer d.mu.Unlock()
		for ticker, at := range d.newSeries {
			// A new series' ticker becomes a directory name; one that
			// isn't safe as one is not captured.
			if now.Sub(at) <= d.Cfg.NewSeriesFor && !set[ticker] && storage.SafeComponent(ticker) {
				recent = append(recent, seen{ticker, at})
			}
		}
		slices.SortFunc(recent, func(a, b seen) int {
			if c := b.at.Compare(a.at); c != 0 {
				return c
			}
			return cmp.Compare(a.ticker, b.ticker)
		})
		for _, s := range recent[:min(len(recent), d.Cfg.MaxNewSeries)] {
			set[s.ticker] = true
		}
	}
	out := make([]string, 0, len(set))
	for s := range set {
		out = append(out, s)
	}
	slices.Sort(out)
	return out
}

// PollMarkets fetches the unopened markets of every watched series and
// updates the Schedule. It keeps going past a series that fails, and
// returns the errors joined.
func (d *Discovery) PollMarkets(ctx context.Context) error {
	watched := d.Watched()
	now := map[string]bool{}
	for _, s := range watched {
		now[s] = true
	}
	for s := range d.watched {
		if !now[s] {
			d.Schedule.Drop(s) // aged out of the new-series window
		}
	}
	d.watched = now
	d.Metrics.SetWatched(len(watched))

	var errs []error
	var newEvents []string
	for _, series := range watched {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		markets, err := d.REST.Markets(ctx, kalshi.MarketsQuery{Series: series, Status: "unopened"})
		if err != nil {
			// Not applied: a partial list would make the rest of the
			// series' markets look like they had vanished.
			d.Metrics.PollError("markets")
			errs = append(errs, fmt.Errorf("%s: %w", series, err))
			continue
		}
		added, vanished := d.Schedule.Update(series, markets)
		if vanished > 0 {
			d.Metrics.Skipped(metrics.SkipVanished, vanished)
			d.logf("opens: %s: %d scheduled market(s) left the unopened list before their open_time", series, vanished)
		}
		if added > 0 {
			d.logf("opens: %s: scheduled %d market(s)", series, added)
		}
		for _, m := range markets {
			if m.Event != "" && !d.events[m.Event] {
				d.events[m.Event] = true
				newEvents = append(newEvents, m.Event)
			}
		}
	}
	d.Metrics.SetScheduled(d.Schedule.Len())
	if err := d.recordEvents(ctx, newEvents); err != nil {
		d.Metrics.PollError("events")
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// recordEvents fetches the events the newly scheduled markets belong to
// and appends them to events.jsonl: their titles, and whether their
// markets are mutually exclusive. Captures don't depend on it, so on
// failure the events are forgotten and fetched again with the next
// market of theirs that shows up.
func (d *Discovery) recordEvents(ctx context.Context, tickers []string) error {
	if len(tickers) == 0 {
		return nil
	}
	slices.Sort(tickers)
	events, err := d.REST.Events(ctx, tickers)
	if err == nil {
		var a *storage.Appender
		if a, _, err = storage.OpenAppender(filepath.Join(d.Dir, eventsMetaFile)); err == nil {
			for _, e := range events {
				if aerr := a.Append(e); aerr != nil {
					err = aerr
					break
				}
			}
			if cerr := a.Close(); err == nil {
				err = cerr
			}
		}
	}
	if err != nil {
		for _, t := range tickers {
			delete(d.events, t)
		}
		return fmt.Errorf("events: %w", err)
	}
	return nil
}

func sortOpenings(os []Opening) {
	slices.SortFunc(os, func(a, b Opening) int {
		if c := a.Market.Open.Compare(b.Market.Open); c != 0 {
			return c
		}
		return cmp.Compare(a.Market.Ticker, b.Market.Ticker)
	})
}
