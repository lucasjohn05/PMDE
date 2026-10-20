package divergence

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/lucasjohn05/pmde/internal/metrics"
	"github.com/lucasjohn05/pmde/internal/tick"
)

func kt(market string, bid, ask int) tick.Tick {
	return tick.Tick{Venue: tick.Kalshi, Market: market, BidBps: bid, AskBps: ask, OK: true}
}

func pt(market string, bid, ask int) tick.Tick {
	return tick.Tick{Venue: tick.Polymarket, Market: market, BidBps: bid, AskBps: ask, OK: true}
}

func stale(t tick.Tick) tick.Tick {
	t.BidBps, t.AskBps, t.OK = 0, 0, false
	return t
}

func TestCompute(t *testing.T) {
	tests := []struct {
		name   string
		k, p   tick.Tick
		want   Divergence
		wantOK bool
	}{
		// Kalshi 87/88 vs Polymarket 86/87: mids 8750 and 8650, so
		// Kalshi is 100 bps higher. The books touch but don't cross.
		{"kalshi higher, touching", kt("K", 8700, 8800), pt("P", 8600, 8700), Divergence{MidBps: 100, GapBps: 0}, true},
		{"polymarket higher", kt("K", 1200, 1300), pt("P", 1300, 1400), Divergence{MidBps: -100, GapBps: 0}, true},
		{"identical", kt("K", 5000, 5100), pt("P", 5000, 5100), Divergence{MidBps: 0, GapBps: -100}, true},
		// Kalshi bid 6000 above Polymarket ask 5900: buy on Polymarket,
		// sell on Kalshi, 100 bps before fees.
		{"crossed, kalshi bid over polymarket ask", kt("K", 6000, 6100), pt("P", 5800, 5900), Divergence{MidBps: 200, GapBps: 100}, true},
		{"crossed, polymarket bid over kalshi ask", kt("K", 4000, 4100), pt("P", 4300, 4400), Divergence{MidBps: -300, GapBps: 200}, true},
		{"apart, wide spreads", kt("K", 4000, 5000), pt("P", 5500, 6500), Divergence{MidBps: -1500, GapBps: 500}, true},
		// Sum of spreads odd in bps: the exact mid difference is 2.5
		// (4501 vs 4498.5); integer division truncates toward zero.
		{"half bp, truncated", kt("K", 4500, 4502), pt("P", 4498, 4499), Divergence{MidBps: 2, GapBps: 1}, true},
		{"half bp negative, truncated toward zero", kt("K", 4498, 4499), pt("P", 4500, 4502), Divergence{MidBps: -2, GapBps: 1}, true},
		{"kalshi stale", stale(kt("K", 0, 0)), pt("P", 5000, 5100), Divergence{}, false},
		{"polymarket stale", kt("K", 5000, 5100), stale(pt("P", 0, 0)), Divergence{}, false},
		{"never reported (zero Tick)", tick.Tick{}, pt("P", 5000, 5100), Divergence{}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := Compute(tt.k, tt.p)
			if got != tt.want || ok != tt.wantOK {
				t.Fatalf("Compute = %+v, %v; want %+v, %v", got, ok, tt.want, tt.wantOK)
			}
		})
	}
}

type logRec struct{ lines []string }

func (l *logRec) logf(format string, args ...any) {
	l.lines = append(l.lines, fmt.Sprintf(format, args...))
}

func (l *logRec) take() []string {
	out := l.lines
	l.lines = nil
	return out
}

// gauge reads a divergence gauge; ok is false if the series is absent.
func gauge(t *testing.T, reg *prometheus.Registry, name, pair string) (float64, bool) {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range families {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "pair" && l.GetValue() == pair {
					return m.GetGauge().GetValue(), true
				}
			}
		}
	}
	return 0, false
}

func TestTracker(t *testing.T) {
	pairs := []Pair{
		{"hold", "K-H0", "111"},
		{"hike", "K-H25", "222"},
		// Shares the Kalshi market with "hold": one Kalshi Tick must
		// update both.
		{"hold-vs-hike", "K-H0", "222"},
	}
	reg := prometheus.NewRegistry()
	logs := &logRec{}
	tr := NewTracker(pairs, metrics.New(reg), logs.logf)

	expect := func(pair string, want Divergence, wantOK bool) {
		t.Helper()
		got, ok := tr.Get(pair)
		if ok != wantOK || (ok && got != want) {
			t.Fatalf("%s = %+v, %v; want %+v, %v", pair, got, ok, want, wantOK)
		}
		mid, midOK := gauge(t, reg, "divergence_mid_bps", pair)
		gap, gapOK := gauge(t, reg, "divergence_gap_bps", pair)
		if midOK != wantOK || gapOK != wantOK {
			t.Fatalf("%s gauges present = %v/%v, want %v", pair, midOK, gapOK, wantOK)
		}
		if wantOK && (mid != float64(want.MidBps) || gap != float64(want.GapBps)) {
			t.Fatalf("%s gauges = mid %v gap %v, want %+v", pair, mid, gap, want)
		}
	}

	// One side alone: nothing to compare yet, and nothing logged.
	tr.Update(kt("K-H0", 8700, 8800))
	expect("hold", Divergence{}, false)
	if l := logs.take(); len(l) != 0 {
		t.Fatalf("logged %q with only one side reported", l)
	}

	tr.Update(pt("111", 8600, 8700))
	expect("hold", Divergence{MidBps: 100, GapBps: 0}, true)
	expect("hold-vs-hike", Divergence{}, false) // still no 222
	if l := logs.take(); len(l) != 1 || !strings.Contains(l[0], "divergence hold: mid=+100 gap=+0 bps (kalshi 8700/8800, polymarket 8600/8700)") {
		t.Fatalf("log = %q", l)
	}

	tr.Update(pt("222", 1300, 1400))
	tr.Update(kt("K-H25", 1200, 1300))
	expect("hike", Divergence{MidBps: -100, GapBps: 0}, true)
	expect("hold-vs-hike", Divergence{MidBps: 7400, GapBps: 7300}, true)
	logs.take()

	// A Kalshi move on the shared market updates both of its pairs.
	tr.Update(kt("K-H0", 8800, 8900))
	expect("hold", Divergence{MidBps: 200, GapBps: 100}, true)
	expect("hold-vs-hike", Divergence{MidBps: 7500, GapBps: 7400}, true)
	expect("hike", Divergence{MidBps: -100, GapBps: 0}, true)
	if l := logs.take(); len(l) != 2 {
		t.Fatalf("want one log line per changed pair, got %q", l)
	}

	// A Kalshi spread widening around the same mid changes only the gap.
	tr.Update(kt("K-H25", 1100, 1400))
	expect("hike", Divergence{MidBps: -100, GapBps: -100}, true)
	if l := logs.take(); len(l) != 1 || !strings.Contains(l[0], "divergence hike: mid=-100 gap=-100") {
		t.Fatalf("log = %q", l)
	}
	// An unchanged divergence isn't logged again.
	tr.Update(kt("K-H25", 1100, 1400))
	if l := logs.take(); len(l) != 0 {
		t.Fatalf("unchanged divergence logged %q", l)
	}

	// Stale side: skipped, gauges removed, logged once.
	tr.Update(stale(pt("111", 0, 0)))
	expect("hold", Divergence{}, false)
	expect("hold-vs-hike", Divergence{MidBps: 7500, GapBps: 7400}, true) // unaffected
	if l := logs.take(); len(l) != 1 || !strings.Contains(l[0], "divergence hold: unavailable (no trusted polymarket book)") {
		t.Fatalf("log = %q", l)
	}
	tr.Update(kt("K-H0", 8700, 8800)) // Kalshi moves while Polymarket is stale
	expect("hold", Divergence{}, false)
	if l := logs.take(); len(l) != 1 || strings.Contains(l[0], "divergence hold:") {
		t.Fatalf("want only hold-vs-hike logged while hold is unavailable, got %q", l)
	}

	// Recovery: valid again from the fresh Tick.
	tr.Update(pt("111", 8650, 8750))
	expect("hold", Divergence{MidBps: 50, GapBps: -50}, true)

	// A market in no pair is ignored.
	tr.Update(kt("K-OTHER", 100, 200))
	logs.take()
	tr.Update(pt("999", 100, 200))
	if l := logs.take(); len(l) != 0 {
		t.Fatalf("unpaired market logged %q", l)
	}
}

// TestTrackerRun checks the channel loop: Ticks sent by other goroutines
// are applied in order, and Run returns on cancel.
func TestTrackerRun(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := metrics.New(reg)
	tr := NewTracker([]Pair{{"hold", "K", "1"}}, m, nil)

	ticks := make(chan tick.Tick, TickQueueSize)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		tr.Run(ctx, ticks)
	}()

	ticks <- kt("K", 5000, 5100)
	for i := 0; i < 100; i++ {
		ticks <- pt("1", 4000+i, 4100+i)
	}
	// Last Polymarket tick: 4099/4199. Mid diff (10100 - 8298)/2 = 901.
	deadline := time.Now().Add(5 * time.Second)
	for {
		if v, ok := gauge(t, reg, "divergence_mid_bps", "hold"); ok && v == 901 {
			break
		}
		if time.Now().After(deadline) {
			v, ok := gauge(t, reg, "divergence_mid_bps", "hold")
			t.Fatalf("divergence_mid_bps = %v (present %v), want 901", v, ok)
		}
		time.Sleep(time.Millisecond)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
	if n, err := testutil.GatherAndCount(reg, "divergence_gap_bps"); err != nil || n != 1 {
		t.Fatalf("divergence_gap_bps series = %d (err %v), want 1", n, err)
	}
}
