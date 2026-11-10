package analysis

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/lucasjohn05/pmde/internal/storage"
)

var errAPIDown = errors.New("kalshi: market KX-H0: HTTP 503")

func failingAPI(string) (time.Time, error) { return time.Time{}, errAPIDown }

// TestCutoffFallsBackToConfig: when the API call fails, the pair's
// kalshi_last_trade is the cutoff, the source says so, the API's error
// is kept for the log, and the analysis cuts the pair off there.
func TestCutoffFallsBackToConfig(t *testing.T) {
	pair := testPair
	pair.KalshiLastTrade = "1970-01-01T00:00:09-00:00" // 9000 ms
	var asked string
	c := ResolveCutoff(pair, func(ticker string) (time.Time, error) {
		asked = ticker
		return failingAPI(ticker)
	})
	if asked != pair.Kalshi {
		t.Errorf("API asked for %q, want %q", asked, pair.Kalshi)
	}
	if c.Source != CutoffFromConfig || c.At.UnixMilli() != 9000 || !errors.Is(c.APIErr, errAPIDown) {
		t.Fatalf("cutoff = %+v; want 9000 ms from the config, with the API error", c)
	}

	k := []storage.TickRecord{ok(0, 2100, 2200, 10, 10)}
	p := []storage.TickRecord{ok(0, 1800, 1900, 10, 10)}
	r := analyze(k, p, runRecs("r1", 0, 12000), 0, 12000, func(in *Input) { in.Pair, in.Cutoff = pair, c })
	if r.Coverage.CoveredMs != 9000 || r.Coverage.AfterLastTradeMs != 3000 {
		t.Fatalf("coverage = %+v; want 9000 covered, 3000 after the cutoff", r.Coverage)
	}
	if r.KalshiLastTradeSource != CutoffFromConfig {
		t.Errorf("report source = %q, want %q", r.KalshiLastTradeSource, CutoffFromConfig)
	}
	var b strings.Builder
	if err := WriteText(&b, r, 0); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(b.String(), "from the pair config's kalshi_last_trade") {
		t.Errorf("text output doesn't name the config as the source:\n%s", b.String())
	}
}

// TestCutoffPrefersAPI: a working API overrides the config value (which
// on Oct 28 2026 was four minutes early).
func TestCutoffPrefersAPI(t *testing.T) {
	pair := testPair
	pair.KalshiLastTrade = "1970-01-01T00:00:09-00:00"
	api := time.UnixMilli(11000).UTC()
	c := ResolveCutoff(pair, func(string) (time.Time, error) { return api, nil })
	if c.Source != CutoffFromAPI || !c.At.Equal(api) || c.APIErr != nil {
		t.Fatalf("cutoff = %+v; want %v from the API", c, api)
	}
}

// TestCutoffNone: no API (or a failed one) and nothing in the config
// means no cutoff, and the analysis doesn't invent one.
func TestCutoffNone(t *testing.T) {
	for name, api := range map[string]func(string) (time.Time, error){"no API": nil, "API down": failingAPI} {
		c := ResolveCutoff(testPair, api)
		if c.Source != CutoffNone || !c.At.IsZero() {
			t.Fatalf("%s: cutoff = %+v; want none", name, c)
		}
		k := []storage.TickRecord{ok(0, 2100, 2200, 10, 10)}
		p := []storage.TickRecord{ok(0, 1800, 1900, 10, 10)}
		r := analyze(k, p, runRecs("r1", 0, 12000), 0, 12000, func(in *Input) { in.Cutoff = c })
		if r.KalshiLastTrade != nil || r.Coverage.AfterLastTradeMs != 0 {
			t.Fatalf("%s: report has a cutoff: %v, %+v", name, r.KalshiLastTrade, r.Coverage)
		}
	}
}
