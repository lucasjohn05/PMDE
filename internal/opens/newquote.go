package opens

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/lucasjohn05/pmde/internal/metrics"
	"github.com/lucasjohn05/pmde/internal/storage"
)

// newQuotesFile holds the first qualifying quote of each new series.
const newQuotesFile = "new_series_quotes.jsonl"

// NewSeriesQuote is one line of new_series_quotes.jsonl: the first
// two-sided quote, under the quote rule, on any market of a series
// within its first NewSeriesFor after it was detected.
type NewSeriesQuote struct {
	T          int64  `json:"t"` // when the quote appeared, Unix ms (the venue's clock)
	Series     string `json:"series"`
	Title      string `json:"title"`
	DetectedMs int64  `json:"detected_ms"` // when the series was first seen
	Market     string `json:"market"`
	OpenMs     int64  `json:"open_ms"`
	Bid        int    `json:"bid"`    // bps
	Ask        int    `json:"ask"`    // bps
	BidSz      int64  `json:"bid_sz"` // hundredths of a contract
	AskSz      int64  `json:"ask_sz"`
}

// NewSeriesQuotes raises the new-series alert: the first time a series
// detected within its first For gets a qualifying quote, it logs the
// quote, appends it to new_series_quotes.jsonl and exposes it as
// opens_new_series_quoted. Only the first quote of a series counts.
//
// The metric stays exposed until the series' For has run out, across
// restarts (the file is read back by Load), so an alert on it fires once
// per series. Safe for concurrent use.
type NewSeriesQuotes struct {
	Dir     string        // <data-dir>/opens
	For     time.Duration // how long after detection a series counts as new
	Metrics *metrics.Opens
	Logf    func(format string, args ...any)

	mu     sync.Mutex
	quoted map[string]NewSeriesQuote // series -> its first quote
}

// Load reads back the quotes already alerted on and re-exposes those
// whose series is still new at now.
func (n *NewSeriesQuotes) Load(now time.Time) error {
	recs, err := storage.ReadJSONL[NewSeriesQuote](filepath.Join(n.Dir, newQuotesFile))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("opens: %w", err)
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	n.quoted = map[string]NewSeriesQuote{}
	for _, q := range recs {
		if _, ok := n.quoted[q.Series]; ok {
			continue
		}
		n.quoted[q.Series] = q
		if n.stillNew(q, now) {
			n.Metrics.SetNewSeriesQuoted(q.metric())
		}
	}
	return nil
}

func (n *NewSeriesQuotes) stillNew(q NewSeriesQuote, now time.Time) bool {
	return now.Sub(time.UnixMilli(q.DetectedMs)) <= n.For
}

// Want reports whether series has yet to raise the alert.
func (n *NewSeriesQuotes) Want(series string) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	_, done := n.quoted[series]
	return !done
}

// Quoted raises the alert for q, unless its series already had one or
// is past its first For at now. It reports whether it did.
func (n *NewSeriesQuotes) Quoted(q NewSeriesQuote, now time.Time) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.quoted == nil {
		n.quoted = map[string]NewSeriesQuote{}
	}
	if _, done := n.quoted[q.Series]; done || !n.stillNew(q, now) {
		return false
	}
	n.quoted[q.Series] = q
	if err := storage.AppendLine(filepath.Join(n.Dir, newQuotesFile), q); err != nil && n.Logf != nil {
		n.Logf("opens: %v", err)
	}
	n.Metrics.SetNewSeriesQuoted(q.metric())
	if n.Logf != nil {
		n.Logf("opens: NEW SERIES QUOTED %s (%s, first seen %s): %s bid %s x %s, ask %s x %s contracts, spread %d bps, %+.1fs after its open",
			q.Series, q.Title, time.UnixMilli(q.DetectedMs).UTC().Format(time.RFC3339), q.Market,
			cents(q.Bid), centi(q.BidSz), cents(q.Ask), centi(q.AskSz), q.Ask-q.Bid,
			float64(q.T-q.OpenMs)/1000)
	}
	return true
}

// Expire stops exposing the quotes of series that are no longer new.
func (n *NewSeriesQuotes) Expire(now time.Time) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, q := range n.quoted {
		if !n.stillNew(q, now) {
			n.Metrics.ForgetNewSeriesQuoted(q.Series)
		}
	}
}

func (q NewSeriesQuote) metric() metrics.NewSeriesQuote {
	return metrics.NewSeriesQuote{
		Series: q.Series, Title: q.Title, Market: q.Market,
		Bid: cents(q.Bid), Ask: cents(q.Ask), BidSize: centi(q.BidSz), AskSize: centi(q.AskSz),
		SpreadBps: q.Ask - q.Bid, At: time.UnixMilli(q.T),
	}
}

// cents formats bps as cents: 3600 -> "36c", 3650 -> "36.5c".
func cents(bps int) string {
	return centi(int64(bps)) + "c"
}
