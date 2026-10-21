package storage

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/lucasjohn05/pmde/internal/divergence"
	"github.com/lucasjohn05/pmde/internal/tick"
)

// TestDivergenceRecomputedFromDisk: because both venues' tick files are
// stamped on one clock (t, the local receive time), merging them by t and
// replaying through a Tracker reproduces exactly the divergence the live
// Tracker computed, step by step, including the gaps where one side was
// untrusted.
func TestDivergenceRecomputedFromDisk(t *testing.T) {
	pair := divergence.Pair{Name: "hold", Kalshi: "KX-H0", Polymarket: "111"}
	k := func(ms int64, bid, ask int, ok bool) tick.Tick {
		tk := tick.Tick{Venue: tick.Kalshi, Market: pair.Kalshi, Time: at(ms), VenueTime: at(ms - 30), OK: ok}
		if ok {
			tk.BidBps, tk.AskBps, tk.BidSize, tk.AskSize = bid, ask, 100, 200
		}
		return tk
	}
	p := func(ms int64, bid, ask int, ok bool) tick.Tick {
		tk := k(ms, bid, ask, ok)
		tk.Venue, tk.Market = tick.Polymarket, pair.Polymarket
		return tk
	}
	// Live arrival order (by receive time). Venue times run behind and,
	// across venues, are on different clocks, which is why windows and
	// replay use t.
	live := []tick.Tick{
		k(100, 8700, 8800, true),
		p(150, 8600, 8700, true),
		k(900, 8800, 8900, true),
		p(1200, 0, 0, false), // Polymarket resyncing
		k(1300, 8900, 9000, true),
		p(1700, 8650, 8750, true),
		k(2500, 8600, 8700, true),
		p(2600, 8700, 8800, true),
	}

	w, dir, _ := newTestWriter(t)
	liveTracker := divergence.NewTracker([]divergence.Pair{pair}, nil, nil)
	var want []string
	for _, tk := range live {
		w.write(tk)
		liveTracker.Update(tk)
		want = append(want, divString(liveTracker.Get("hold")))
	}
	w.close(at(3000))

	// Read both venues' files back and merge by t.
	var replay []tick.Tick
	for _, m := range []struct{ venue, market string }{{tick.Kalshi, pair.Kalshi}, {tick.Polymarket, pair.Polymarket}} {
		recs, err := ReadTicks(file(dir, m.venue, m.market, "2026-10-21", kindTicks))
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range recs {
			tk := tick.Tick{Venue: m.venue, Market: m.market, OK: r.OK, Time: time.UnixMilli(r.T)}
			if r.OK {
				tk.BidBps, tk.AskBps, tk.BidSize, tk.AskSize = *r.Bid, *r.Ask, *r.BidSize, *r.AskSize
			}
			if r.VT != 0 {
				tk.VenueTime = time.UnixMilli(r.VT)
			}
			replay = append(replay, tk)
		}
	}
	slices.SortStableFunc(replay, func(a, b tick.Tick) int { return a.Time.Compare(b.Time) })

	replayed := divergence.NewTracker([]divergence.Pair{pair}, nil, nil)
	var got []string
	for i, tk := range replay {
		if !tk.Time.Equal(live[i].Time) || tk.VenueTime.UnixMilli() != live[i].VenueTime.UnixMilli() {
			t.Fatalf("replayed Tick %d = %+v, want %+v", i, tk, live[i])
		}
		replayed.Update(tk)
		got = append(got, divString(replayed.Get("hold")))
	}
	if !slices.Equal(got, want) {
		t.Fatalf("replayed divergence:\n got  %v\n want %v", got, want)
	}
}

func divString(d divergence.Divergence, ok bool) string {
	if !ok {
		return "unavailable"
	}
	return fmt.Sprintf("mid %d gap %d", d.MidBps, d.GapBps)
}
