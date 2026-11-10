package analysis

import (
	"time"

	"github.com/lucasjohn05/pmde/internal/divergence"
)

// Where a pair's Kalshi cutoff came from.
const (
	CutoffFromAPI    = "kalshi_api" // the market's close_time from Kalshi's REST API
	CutoffFromConfig = "config"     // the pair's kalshi_last_trade
	CutoffNone       = "none"       // neither: the pair is never cut off
)

// Cutoff is when Kalshi trading on a pair's market ends. The analysis
// excludes the pair from At on (see Input.Cutoff).
type Cutoff struct {
	At     time.Time // zero when Source is CutoffNone
	Source string

	// APIErr is why the API wasn't used, when it was asked and failed.
	// The cutoff then falls back to the config.
	APIErr error
}

// ResolveCutoff picks the pair's cutoff: the Kalshi market's close_time
// from closeTime, or, if that fails, the pair's kalshi_last_trade. A nil
// closeTime skips the API.
//
// The API comes first because the config value is typed by hand from
// the contract terms, and on Oct 28 2026 those were four minutes early
// (1:55 PM ET; trades ran until the 1:59 close_time).
func ResolveCutoff(p divergence.Pair, closeTime func(ticker string) (time.Time, error)) Cutoff {
	var c Cutoff
	if closeTime != nil {
		t, err := closeTime(p.Kalshi)
		if err == nil {
			return Cutoff{At: t, Source: CutoffFromAPI}
		}
		c.APIErr = err
	}
	if lt, ok := p.LastTrade(); ok {
		c.At, c.Source = lt, CutoffFromConfig
		return c
	}
	c.Source = CutoffNone
	return c
}
