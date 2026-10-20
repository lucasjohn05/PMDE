// Package divergence compares the two venues' top of book for each
// hand-written venue pair (see Pair) and reports how far apart they are.
package divergence

import (
	"context"

	"github.com/lucasjohn05/pmde/internal/metrics"
	"github.com/lucasjohn05/pmde/internal/tick"
)

// TickQueueSize is the capacity of the channel from the venue clients'
// book goroutines to the Tracker.
//
// Ticks are much rarer than websocket messages: a client only sends one
// when its top of book changes, and the Tracker's work per tick is a few
// map lookups plus a log line when a divergence changes. So, like the
// reader->book queue, this is slack for short stalls (a slow log write),
// not for a consumer that's steadily too slow. Per CLAUDE.md, this stage
// is downstream of the books, so a full channel blocks the sender rather
// than dropping: a dropped Tick would leave the Tracker holding an old
// top of book with nothing to mark it stale. If the Tracker ever stalls
// for long, the block backs up into that client's reader->book queue,
// which drops, marks the book stale and resyncs: the existing,
// accounted-for failure path.
const TickQueueSize = 256

// Divergence is one pair's comparison, in basis points.
type Divergence struct {
	// MidBps is Kalshi's mid minus Polymarket's mid: positive when Kalshi
	// prices the outcome higher.
	MidBps int

	// GapBps is the executable gap: max(Kalshi bid - Polymarket ask,
	// Polymarket bid - Kalshi ask). Positive means the books cross, so
	// buying on one venue and selling on the other locks in that much
	// before fees. Zero or negative is how far apart the closest quotes
	// are.
	GapBps int
}

// Compute compares a Kalshi and a Polymarket Tick for the same outcome.
// ok is false if either has no trusted top of book.
//
// The mid difference is computed as ((kBid + kAsk) - (pBid + pAsk)) / 2,
// with Go's integer division (truncating toward zero). Halving the
// difference once, rather than each mid, rounds once instead of twice.
// It's only inexact when the two spreads' sum is odd in bps, and then by
// half a basis point.
func Compute(k, p tick.Tick) (d Divergence, ok bool) {
	if !k.OK || !p.OK {
		return Divergence{}, false
	}
	return Divergence{
		MidBps: ((k.BidBps + k.AskBps) - (p.BidBps + p.AskBps)) / 2,
		GapBps: max(k.BidBps-p.AskBps, p.BidBps-k.AskBps),
	}, true
}

type marketKey struct{ venue, market string }

type pairState struct {
	Pair
	metrics *metrics.Pair

	valid bool // last computed divergence was valid
	last  Divergence
}

// Tracker holds the latest Tick per market and recomputes every pair the
// market belongs to whenever one arrives. Clients only send a Tick when
// the top of book changes, so this is "whenever either book's best
// changes". Not safe for concurrent use: one goroutine (Run) owns it.
type Tracker struct {
	pairs    []*pairState
	byMarket map[marketKey][]*pairState
	latest   map[marketKey]tick.Tick
	logf     func(format string, args ...any)
}

// NewTracker returns a Tracker for pairs. m and logf may be nil.
func NewTracker(pairs []Pair, m *metrics.Metrics, logf func(format string, args ...any)) *Tracker {
	t := &Tracker{
		byMarket: map[marketKey][]*pairState{},
		latest:   map[marketKey]tick.Tick{},
		logf:     logf,
	}
	for _, p := range pairs {
		ps := &pairState{Pair: p}
		if m != nil {
			ps.metrics = m.Pair(p.Name)
		}
		t.pairs = append(t.pairs, ps)
		k := marketKey{tick.Kalshi, p.Kalshi}
		t.byMarket[k] = append(t.byMarket[k], ps)
		pm := marketKey{tick.Polymarket, p.Polymarket}
		t.byMarket[pm] = append(t.byMarket[pm], ps)
	}
	return t
}

// Run applies Ticks from ticks until ctx is cancelled or ticks is closed.
func (t *Tracker) Run(ctx context.Context, ticks <-chan tick.Tick) {
	for {
		select {
		case <-ctx.Done():
			return
		case tk, open := <-ticks:
			if !open {
				return
			}
			t.Update(tk)
		}
	}
}

// Update records tk and recomputes the pairs its market belongs to. A
// Tick for a market in no pair is ignored.
func (t *Tracker) Update(tk tick.Tick) {
	key := marketKey{tk.Venue, tk.Market}
	affected := t.byMarket[key]
	if len(affected) == 0 {
		return
	}
	t.latest[key] = tk
	for _, ps := range affected {
		t.recompute(ps)
	}
}

// Get returns a pair's current divergence; ok is false if either side has
// no trusted top of book (or hasn't reported yet).
func (t *Tracker) Get(name string) (d Divergence, ok bool) {
	for _, ps := range t.pairs {
		if ps.Name == name {
			return ps.last, ps.valid
		}
	}
	return Divergence{}, false
}

func (t *Tracker) recompute(ps *pairState) {
	k := t.latest[marketKey{tick.Kalshi, ps.Kalshi}]
	p := t.latest[marketKey{tick.Polymarket, ps.Polymarket}]
	d, ok := Compute(k, p)

	if !ok {
		// Skipped: no trusted divergence while either side is untrusted.
		// The gauges are removed rather than left at a value that would
		// look current.
		if ps.valid {
			ps.valid = false
			ps.metrics.Clear()
			t.log("divergence %s: unavailable (%s)", ps.Name, unavailableReason(k, p))
		}
		return
	}
	ps.metrics.Set(d.MidBps, d.GapBps)
	if ps.valid && d == ps.last {
		return // the top of book moved, but not the divergence
	}
	ps.valid, ps.last = true, d
	t.log("divergence %s: mid=%+d gap=%+d bps (kalshi %d/%d, polymarket %d/%d)",
		ps.Name, d.MidBps, d.GapBps, k.BidBps, k.AskBps, p.BidBps, p.AskBps)
}

func unavailableReason(k, p tick.Tick) string {
	switch {
	case !k.OK && !p.OK:
		return "no trusted book on either venue"
	case !k.OK:
		return "no trusted kalshi book"
	default:
		return "no trusted polymarket book"
	}
}

func (t *Tracker) log(format string, args ...any) {
	if t.logf != nil {
		t.logf(format, args...)
	}
}
