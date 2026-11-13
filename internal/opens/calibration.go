package opens

import (
	"cmp"
	"hash/fnv"
	"math"
	"math/rand/v2"
	"slices"
	"strconv"
)

// The calibration statistics treat the event, not the rung, as the
// unit: one outcome decides every rung of an event, so an event with 30
// rungs is one observation of the maker's model being right or wrong,
// not 30.

// calRung is one settled open in a calibration sample.
type calRung struct {
	p      float64 // opening mid as a probability
	decile int
	yes    bool

	// The rung resolves yes in a simulated event iff the event's
	// uniform draw u falls in [lo, hi), taken around the unit circle
	// (hi may exceed 1). The arc's length is p. See arcs.
	lo, hi float64
}

type calEvent struct {
	name  string
	rungs []calRung
}

// calEvents groups a series' settled, quoted rows by event, sorted by
// event ticker so that every run draws the same random numbers for the
// same events.
func calEvents(rows []OpenRow) []calEvent {
	byEvent := map[string][]OpenRow{}
	for _, r := range rows {
		if r.Mid != nil && (r.Result == "yes" || r.Result == "no") {
			key := r.Event
			if key == "" {
				key = r.Market
			}
			byEvent[key] = append(byEvent[key], r)
		}
	}
	events := make([]calEvent, 0, len(byEvent))
	for name, rs := range byEvent {
		events = append(events, calEvent{name: name, rungs: arcs(rs)})
	}
	slices.SortFunc(events, func(a, b calEvent) int { return cmp.Compare(a.name, b.name) })
	return events
}

// arcs lays an event's rungs out on the unit interval so that a single
// uniform draw u decides all of them at once, each resolving yes with
// exactly the probability its opening mid states. Low u stands for a
// high value of the underlying:
//
//   - "greater" rungs are yes on [0, p): nested, so a draw that makes
//     "above 60" yes makes "above 55" yes too, as one outcome would;
//   - "less" rungs are yes on [1-p, 1), nested the other way;
//   - every other rung ("between" buckets, and rungs with no usable
//     strike) gets its own arc of length p, laid end to end starting
//     where the smallest "greater" tail stops, buckets from the highest
//     strike down. They exclude each other as long as the mids sum to
//     at most 1; past that the arcs wrap around and overlap, which is
//     what mids summing to more than 1 imply.
//
// Rungs with no strike are treated as mutually exclusive too. That is
// right for an event's alternative outcomes and wrong for unrelated
// markets filed under one event; the API's strike fields don't say
// which, and the marginals are exact either way.
func arcs(rows []OpenRow) []calRung {
	slices.SortFunc(rows, func(a, b OpenRow) int {
		fa, _ := strconv.ParseFloat(a.Floor, 64)
		fb, _ := strconv.ParseFloat(b.Floor, 64)
		return cmp.Or(cmp.Compare(fb, fa), cmp.Compare(a.Market, b.Market))
	})
	start := 1.0
	for _, r := range rows {
		if r.StrikeType == "greater" || r.StrikeType == "greater_or_equal" {
			start = min(start, float64(*r.Mid)/10000)
		}
	}
	if start == 1 {
		start = 0 // no "greater" tail
	}
	rungs := make([]calRung, 0, len(rows))
	for _, r := range rows {
		g := calRung{p: float64(*r.Mid) / 10000, decile: min(*r.Mid*10/10000, 9), yes: r.Result == "yes"}
		switch r.StrikeType {
		case "greater", "greater_or_equal":
			g.lo, g.hi = 0, g.p
		case "less", "less_or_equal":
			g.lo, g.hi = 1-g.p, 1
		default:
			g.lo, g.hi = start, start+g.p
			start = math.Mod(start+g.p, 1)
		}
		rungs = append(rungs, g)
	}
	return rungs
}

// in reports whether the draw u resolves the rung yes.
func (g calRung) in(u float64) bool {
	return u >= g.lo && u < g.hi || u+1 < g.hi
}

// calAcc accumulates a calibration table.
type calAcc struct {
	n    [10]int
	sumP [10]float64
	yes  [10]int
	sq   float64 // sum of squared errors, for the Brier score
}

func (a *calAcc) add(g calRung, yes bool) {
	a.n[g.decile]++
	a.sumP[g.decile] += g.p
	y := 0.0
	if yes {
		a.yes[g.decile]++
		y = 1
	}
	a.sq += (g.p - y) * (g.p - y)
}

// miscal is the expected calibration error as a fraction of $1: the
// decile-weighted mean of |mean mid - realized frequency|.
func (a *calAcc) miscal() float64 {
	total, sum := 0, 0.0
	for d := range a.n {
		total += a.n[d]
		sum += math.Abs(a.sumP[d] - float64(a.yes[d]))
	}
	if total == 0 {
		return 0
	}
	return sum / float64(total)
}

// Calibration is a series' opening mids measured against results.
type Calibration struct {
	Settled   int // rungs
	CalEvents int // events those rungs belong to: the independent observations
	Deciles   [10]Decile
	Brier     float64

	// MiscalBps is the expected calibration error. It is a mean of
	// absolute values, so it is positive even for perfectly calibrated
	// quotes, and more so the fewer events there are: NullBps is its
	// expected value if the opening mids were exactly right, and
	// ExcessBps = max(0, MiscalBps - NullBps) is what is left to
	// explain.
	MiscalBps float64
	NullBps   float64
	ExcessBps float64

	// LoBps and HiBps are a 90% interval for the true miscalibration,
	// from resampling whole events. It is the basic bootstrap interval,
	// [2m - q95, 2m - q05] for the estimate m and the resamples'
	// quantiles, floored at zero. The plain percentile interval
	// [q05, q95] would be wrong here: resampling adds noise, noise adds
	// to a mean of absolute values, so the resamples sit above m by
	// about as much as m sits above the truth. Reflecting them around m
	// takes that bias back out, which is why the interval can lie below
	// the estimate when the quotes are close to calibrated.
	LoBps, HiBps float64
}

// calibrate computes a series' Calibration. resamples is how many
// bootstrap resamples and null simulations to run. The random numbers
// come from seed and the series name only, so a report is reproducible
// and one series' numbers don't depend on which others are in it.
func calibrate(series string, rows []OpenRow, seed uint64, resamples int) Calibration {
	events := calEvents(rows)
	var c Calibration
	c.CalEvents = len(events)
	var acc calAcc
	for _, e := range events {
		for _, g := range e.rungs {
			acc.add(g, g.yes)
			c.Settled++
		}
	}
	if c.Settled == 0 {
		return c
	}
	for d := range c.Deciles {
		if n := acc.n[d]; n > 0 {
			c.Deciles[d] = Decile{N: n, MeanBps: acc.sumP[d] / float64(n) * 10000, Freq: float64(acc.yes[d]) / float64(n)}
		}
	}
	c.Brier = acc.sq / float64(c.Settled)
	c.MiscalBps = acc.miscal() * 10000

	h := fnv.New64a()
	h.Write([]byte(series))
	rng := rand.New(rand.NewPCG(seed, h.Sum64()))

	// Bootstrap: resample events with replacement, each keeping all its
	// rungs and its real outcome.
	boot := make([]float64, resamples)
	for i := range boot {
		var a calAcc
		for range events {
			for _, g := range events[rng.IntN(len(events))].rungs {
				a.add(g, g.yes)
			}
		}
		boot[i] = a.miscal() * 10000
	}
	slices.Sort(boot)
	c.LoBps = max(0, 2*c.MiscalBps-quantile(boot, 0.95))
	c.HiBps = max(0, 2*c.MiscalBps-quantile(boot, 0.05))

	// Null: keep the mids, and draw each event's outcome from them.
	null := 0.0
	for range resamples {
		var a calAcc
		for _, e := range events {
			u := rng.Float64()
			for _, g := range e.rungs {
				a.add(g, g.in(u))
			}
		}
		null += a.miscal()
	}
	c.NullBps = null / float64(resamples) * 10000
	c.ExcessBps = max(0, c.MiscalBps-c.NullBps)
	return c
}

// quantile reads the q-quantile off sorted xs (nearest rank).
func quantile(xs []float64, q float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	return xs[min(int(q*float64(len(xs))), len(xs)-1)]
}
