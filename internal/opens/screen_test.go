package opens

import (
	"testing"
	"time"

	"github.com/lucasjohn05/pmde/internal/kalshi"
)

func TestScreen(t *testing.T) {
	r := DefaultScreenRule()
	in := ScreenInputs{WindowNotional: 4000, HasNotional: true, TopQuoterShare: 0.86, HasQuoters: true, SpreadBps: 400, HasSpread: true}
	if s := r.Evaluate(in); !s.Candidate || s.String() != "CANDIDATE" {
		t.Errorf("candidate: %+v %q", s, s.String())
	}
	for name, c := range map[string]struct {
		mod  func(*ScreenInputs)
		want string
	}{
		"too little flow":   {func(i *ScreenInputs) { i.WindowNotional = 999 }, "no: $"},
		"too much flow":     {func(i *ScreenInputs) { i.WindowNotional = 50001 }, "no: $"},
		"no dominant maker": {func(i *ScreenInputs) { i.TopQuoterShare = 0.69 }, "no: q"},
		"tight":             {func(i *ScreenInputs) { i.SpreadBps = 200 }, "no: sp"},
		"never captured":    {func(i *ScreenInputs) { i.HasQuoters = false }, "?: quoters"},
		"tight, no volume":  {func(i *ScreenInputs) { i.SpreadBps = 100; i.HasNotional = false }, "no: sp; ?: notional"},
	} {
		x := in
		c.mod(&x)
		if s := r.Evaluate(x); s.Candidate || s.String() != c.want {
			t.Errorf("%s: %+v %q, want %q", name, s, s.String(), c.want)
		}
	}
	// the band's edges are inside it
	for _, v := range []float64{1000, 50000} {
		x := in
		x.WindowNotional = v
		if !r.Evaluate(x).Candidate {
			t.Errorf("$%.0f should be inside the band", v)
		}
	}
}

func TestScreenConfig(t *testing.T) {
	c, err := ParseConfig([]byte("series: [A]\nscreen:\n  min_window_notional: 500\n  max_window_notional: 20000\n  min_top_quoter_share: 0.8\n  min_spread_bps: 250\n"))
	if err != nil {
		t.Fatal(err)
	}
	if c.Screen != (ScreenRule{500, 20000, 0.8, 250}) {
		t.Errorf("%+v", c.Screen)
	}
	if d, _ := ParseConfig([]byte("series: [A]\n")); d.Screen != DefaultScreenRule() {
		t.Errorf("default %+v", d.Screen)
	}
	for _, bad := range []string{
		"series: [A]\nscreen: {min_top_quoter_share: 1.5}\n",
		"series: [A]\nscreen: {min_window_notional: 9000, max_window_notional: 100}\n",
		"series: [A]\nscreen: {min_spread_bps: -1}\n",
		"series: [A]\nscreen: {spread: 300}\n",
	} {
		if _, err := ParseConfig([]byte(bad)); err == nil {
			t.Errorf("no error for %q", bad)
		}
	}
}

// The candle estimate of window notional: volume x mean yes price, over
// whole minutes inside the window.
func TestCandleNotional(t *testing.T) {
	open := time.Date(2026, 11, 14, 13, 0, 0, 0, time.UTC)
	r := OpenRow{OpenMs: open.UnixMilli()}
	cs := []kalshi.Candle{
		{End: open.Unix() + 60, VolumeCenti: 1000, MeanBps: 4000, HasMean: true}, // 10 @ 40c: $4
		{End: open.Unix() + 120, VolumeCenti: 500, MeanBps: 6000, HasMean: true}, // past a 60s window
		{End: open.Unix() + 60, VolumeCenti: 300, HasMean: false},                // no mean: not counted
	}
	setCandleNotional(&r, cs, time.Minute)
	if r.WindowNotional == nil || *r.WindowNotional != 400 || !r.NotionalFromCandles {
		t.Errorf("60s window: %v", deref(r.WindowNotional))
	}
	setCandleNotional(&r, cs, 30*time.Minute)
	if *r.WindowNotional != 700 {
		t.Errorf("30m window: %v", deref(r.WindowNotional))
	}
}
