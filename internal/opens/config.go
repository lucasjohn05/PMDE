// Package opens is the open scanner: it finds Kalshi markets about to
// open, records their order books through the open, joins the result
// with how the markets settled, and ranks series by how crude and how
// consistent their opening quotes are. It records and reports only;
// nothing here places an order.
//
// Layout, under <data-dir>/opens:
//
//	series/<YYYY-MM-DD>.jsonl        daily snapshot of Kalshi's series list
//	new_series.jsonl                 series first seen after the first snapshot
//	events.jsonl                     events of the markets scheduled
//	events/<series>/<YYYY-MM-DD>.jsonl   captured book events (EventRecord)
//	captures.jsonl                   one line per finished capture
//	backfill.jsonl                   one row per market: opening quote and result
package opens

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"time"

	"go.yaml.in/yaml/v3"

	"github.com/lucasjohn05/pmde/internal/storage"
)

// Config is the open scanner's YAML config.
type Config struct {
	// Series is the allowlist: series tickers whose market opens are
	// captured and whose settled markets are backfilled.
	Series []string

	// IncludeNewSeries also captures series first seen within
	// NewSeriesFor, at most MaxNewSeries of them (the most recent).
	IncludeNewSeries bool
	NewSeriesFor     time.Duration
	MaxNewSeries     int

	// Lead is how long before open_time a capture subscribes; Window how
	// long after open_time it records.
	Lead, Window time.Duration

	// MaxConcurrent caps captures running at once. A capture is one
	// websocket connection covering the rungs of one event that open
	// together; an event past the cap is skipped whole and counted.
	MaxConcurrent int

	// MarketPoll and SeriesPoll are how often the unopened markets of
	// the watched series, and the series list, are polled.
	MarketPoll, SeriesPoll time.Duration

	// Quote is what counts as a two-sided quote, in every command.
	Quote QuoteRule
}

// DefaultConfig is what an empty config file means, except that Series
// is then empty and only new series are captured.
func DefaultConfig() Config {
	return Config{
		IncludeNewSeries: true,
		NewSeriesFor:     14 * 24 * time.Hour,
		MaxNewSeries:     100,
		Lead:             5 * time.Second,
		Window:           60 * time.Second,
		MaxConcurrent:    50,
		MarketPoll:       time.Minute,
		SeriesPoll:       15 * time.Minute,
		Quote:            DefaultQuoteRule(),
	}
}

// configFile is the YAML shape. Durations are strings ("5s", "15m"), or
// a number of days for new_series_days.
type configFile struct {
	Series           []string `yaml:"series"`
	IncludeNewSeries *bool    `yaml:"include_new_series"`
	NewSeriesDays    *int     `yaml:"new_series_days"`
	MaxNewSeries     *int     `yaml:"max_new_series"`
	Lead             string   `yaml:"lead"`
	Window           string   `yaml:"window"`
	MaxConcurrent    *int     `yaml:"max_concurrent"`
	MarketPoll       string   `yaml:"market_poll"`
	SeriesPoll       string   `yaml:"series_poll"`
	Quote            *struct {
		MaxSpreadBps *int     `yaml:"max_spread_bps"`
		MinSize      *float64 `yaml:"min_size"` // contracts
	} `yaml:"quote"`
}

// LoadConfig reads and validates a scanner config file.
func LoadConfig(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("reading opens config: %w", err)
	}
	c, err := ParseConfig(data)
	if err != nil {
		return Config{}, fmt.Errorf("%s: %w", path, err)
	}
	return c, nil
}

// ParseConfig parses and validates a scanner config:
//
//	series: [KXHIGHNY, KXHIGHCHI]
//	include_new_series: true
//	new_series_days: 14
//	max_new_series: 100
//	lead: 5s
//	window: 60s
//	max_concurrent: 50
//	market_poll: 1m
//	series_poll: 15m
//	quote:
//	  max_spread_bps: 1000
//	  min_size: 10
//
// Everything but series has a default (DefaultConfig). Unknown keys are
// errors, as in the pair config.
func ParseConfig(data []byte) (Config, error) {
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	var f configFile
	if err := dec.Decode(&f); err != nil && !errors.Is(err, io.EOF) {
		return Config{}, fmt.Errorf("parsing opens config: %w", err)
	}
	c := DefaultConfig()
	seen := map[string]bool{}
	for _, s := range f.Series {
		switch {
		case !storage.SafeComponent(s):
			// It becomes a directory name and a URL path segment.
			return Config{}, fmt.Errorf("series %q is not a series ticker (letters, digits, '.', '_', '-')", s)
		case seen[s]:
			return Config{}, fmt.Errorf("series %q is listed twice", s)
		}
		seen[s] = true
		c.Series = append(c.Series, s)
	}
	if f.IncludeNewSeries != nil {
		c.IncludeNewSeries = *f.IncludeNewSeries
	}
	if f.NewSeriesDays != nil {
		if *f.NewSeriesDays < 1 {
			return Config{}, errors.New("new_series_days must be at least 1")
		}
		c.NewSeriesFor = time.Duration(*f.NewSeriesDays) * 24 * time.Hour
	}
	if f.MaxNewSeries != nil {
		if *f.MaxNewSeries < 0 {
			return Config{}, errors.New("max_new_series must not be negative")
		}
		c.MaxNewSeries = *f.MaxNewSeries
	}
	if f.MaxConcurrent != nil {
		if *f.MaxConcurrent < 1 {
			return Config{}, errors.New("max_concurrent must be at least 1")
		}
		c.MaxConcurrent = *f.MaxConcurrent
	}
	for _, d := range []struct {
		name string
		s    string
		min  time.Duration
		dst  *time.Duration
	}{
		{"lead", f.Lead, time.Second, &c.Lead},
		{"window", f.Window, time.Second, &c.Window},
		{"market_poll", f.MarketPoll, 5 * time.Second, &c.MarketPoll},
		{"series_poll", f.SeriesPoll, time.Minute, &c.SeriesPoll},
	} {
		if d.s == "" {
			continue
		}
		v, err := time.ParseDuration(d.s)
		if err != nil {
			return Config{}, fmt.Errorf("%s: %w", d.name, err)
		}
		if v < d.min {
			return Config{}, fmt.Errorf("%s must be at least %v", d.name, d.min)
		}
		*d.dst = v
	}
	if q := f.Quote; q != nil {
		if q.MaxSpreadBps != nil {
			if *q.MaxSpreadBps < 1 || *q.MaxSpreadBps > 9999 {
				return Config{}, errors.New("quote.max_spread_bps must be between 1 and 9999")
			}
			c.Quote.MaxSpreadBps = *q.MaxSpreadBps
		}
		if q.MinSize != nil {
			// At least one contract: a fraction of a contract is
			// never a quote.
			if *q.MinSize < 1 || *q.MinSize > 1e9 {
				return Config{}, errors.New("quote.min_size must be at least 1 contract")
			}
			c.Quote.MinSizeCenti = int64(math.Round(*q.MinSize * 100))
		}
	}
	if len(c.Series) == 0 && !c.IncludeNewSeries {
		return Config{}, errors.New("nothing to scan: no series listed and include_new_series is false")
	}
	return c, nil
}
