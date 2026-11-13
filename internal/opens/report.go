package opens

import (
	"cmp"
	"math"
	"slices"
	"strconv"
	"time"
)

// SeriesReport is one series' line of the opens report. Prices behind
// it are integer basis points; the statistics over them (means, RMSE,
// Brier) are floats, in the units their names say.
type SeriesReport struct {
	Series string

	// Opens is the markets with a row; Days the UTC calendar days from
	// the first open to the last, inclusive.
	Opens       int
	Days        int
	OpensPerDay float64

	// Quoted is the opens with an opening (first two-sided) quote, and
	// Captured how many of those came from a capture rather than from
	// 1-minute candlesticks.
	Quoted   int
	Captured int

	// QuoteDelayMs is the median time from open_time to the first
	// two-sided quote, over captured opens when there are any (exact),
	// else over candlestick ones (each late by up to a minute).
	// QuoteDelayExact says which.
	QuoteDelayMs    float64
	QuoteDelayExact bool
	QuoteDelayN     int

	// The most common opening size on each side, in hundredths of a
	// contract, and the share of sized opens that have exactly it.
	// Sized is the opens with sizes: captures only.
	Sized        int
	BidSizeMode  int64
	BidModeShare float64
	AskSizeMode  int64
	AskModeShare float64

	SpreadBps float64 // median opening spread

	// FitRMSEBps is the median, over the events with at least
	// fitMinRungs quoted rungs, of the RMSE of a normal CDF fitted to
	// the event's opening mids across its strikes. Near zero means the
	// ladder was priced off a two-parameter normal model.
	FitEvents  int
	FitRMSEBps float64

	// ImpactBpsPerContract is the median mid change after the first
	// trade, in the taker's direction, per contract traded.
	ImpactN              int
	ImpactBpsPerContract float64

	// Calibration of the opening mid against the result, over the opens
	// with both (yes/no results only).
	Settled int
	Deciles [10]Decile
	Brier   float64

	// MiscalBps is the expected calibration error: the decile-weighted
	// mean of |mean opening mid - realized frequency|, in bps.
	MiscalBps float64

	// Score = miscalibration (as a fraction of $1) x modal opening size
	// (contracts, the mean of the two sides' modes) x opens per day:
	// roughly the dollars a day a perfectly informed taker of the modal
	// size could have taken from the opening quotes. HasScore is false
	// when a factor is missing (no settled opens, or no sizes).
	Score    float64
	HasScore bool
}

// Decile is one tenth of the price range: opens whose opening mid fell
// in it, their mean mid, and how often they resolved yes.
type Decile struct {
	N       int
	MeanBps float64
	Freq    float64
}

// fitMinRungs is the fewest quoted rungs an event needs for the normal
// fit: with two parameters, two rungs always fit exactly.
const fitMinRungs = 3

// Report computes one SeriesReport per series in rows, ranked: by Score
// where there is one, then by miscalibration x opens per day, then by
// name. Series with fewer than minOpens rows are left out.
func Report(rows []OpenRow, minOpens int) []SeriesReport {
	bySeries := map[string][]OpenRow{}
	for _, r := range rows {
		bySeries[r.Series] = append(bySeries[r.Series], r)
	}
	var out []SeriesReport
	for series, rs := range bySeries {
		if len(rs) >= max(minOpens, 1) {
			out = append(out, seriesReport(series, rs))
		}
	}
	slices.SortFunc(out, func(a, b SeriesReport) int {
		if a.HasScore != b.HasScore {
			if a.HasScore {
				return -1
			}
			return 1
		}
		return cmp.Or(
			cmp.Compare(b.Score, a.Score),
			cmp.Compare(b.MiscalBps*b.OpensPerDay, a.MiscalBps*a.OpensPerDay),
			cmp.Compare(a.Series, b.Series),
		)
	})
	return out
}

func seriesReport(series string, rows []OpenRow) SeriesReport {
	rep := SeriesReport{Series: series, Opens: len(rows)}

	first, last := rows[0].OpenMs, rows[0].OpenMs
	for _, r := range rows {
		first, last = min(first, r.OpenMs), max(last, r.OpenMs)
	}
	rep.Days = int(utcDay(last)-utcDay(first)) + 1
	rep.OpensPerDay = float64(rep.Opens) / float64(rep.Days)

	var exactDelays, candleDelays, spreads, impacts []float64
	var bidSizes, askSizes []int64
	events := map[string][]OpenRow{}
	var probSum [10]float64
	var yes [10]int
	var brier float64
	for _, r := range rows {
		if r.Mid != nil {
			rep.Quoted++
			spreads = append(spreads, float64(*r.Ask-*r.Bid))
			delay := float64(r.QuoteMs - r.OpenMs)
			if r.Source == SourceCapture {
				rep.Captured++
				exactDelays = append(exactDelays, delay)
			} else {
				candleDelays = append(candleDelays, delay)
			}
			events[r.Event] = append(events[r.Event], r)
			if r.Result == "yes" || r.Result == "no" {
				p, y := float64(*r.Mid)/10000, 0.0
				if r.Result == "yes" {
					y = 1
				}
				d := min(*r.Mid*10/10000, 9)
				rep.Deciles[d].N++
				probSum[d] += p
				if y == 1 {
					yes[d]++
				}
				brier += (p - y) * (p - y)
				rep.Settled++
			}
		}
		if r.BidSz != nil && r.AskSz != nil {
			rep.Sized++
			bidSizes, askSizes = append(bidSizes, *r.BidSz), append(askSizes, *r.AskSz)
		}
		if r.MidBefore != nil && r.MidAfter != nil && r.TradeCount != nil && *r.TradeCount > 0 && r.TakerYes != nil {
			move := float64(*r.MidAfter - *r.MidBefore)
			if !*r.TakerYes {
				move = -move
			}
			impacts = append(impacts, move/(float64(*r.TradeCount)/100))
		}
	}

	if len(exactDelays) > 0 {
		rep.QuoteDelayMs, rep.QuoteDelayExact, rep.QuoteDelayN = median(exactDelays), true, len(exactDelays)
	} else if len(candleDelays) > 0 {
		rep.QuoteDelayMs, rep.QuoteDelayN = median(candleDelays), len(candleDelays)
	}
	rep.SpreadBps = median(spreads)
	rep.BidSizeMode, rep.BidModeShare = mode(bidSizes)
	rep.AskSizeMode, rep.AskModeShare = mode(askSizes)
	rep.ImpactN, rep.ImpactBpsPerContract = len(impacts), median(impacts)

	var rmses []float64
	for _, rungs := range events {
		if rmse, ok := normalFitRMSE(rungs); ok {
			rmses = append(rmses, rmse)
		}
	}
	rep.FitEvents, rep.FitRMSEBps = len(rmses), median(rmses)

	if rep.Settled > 0 {
		rep.Brier = brier / float64(rep.Settled)
		ece := 0.0
		for d := range rep.Deciles {
			if n := rep.Deciles[d].N; n > 0 {
				rep.Deciles[d].MeanBps = probSum[d] / float64(n) * 10000
				rep.Deciles[d].Freq = float64(yes[d]) / float64(n)
				ece += float64(n) / float64(rep.Settled) * math.Abs(probSum[d]/float64(n)-rep.Deciles[d].Freq)
			}
		}
		rep.MiscalBps = ece * 10000
	}
	if rep.Settled > 0 && rep.Sized > 0 {
		modal := float64(rep.BidSizeMode+rep.AskSizeMode) / 2 / 100 // contracts
		rep.Score = rep.MiscalBps / 10000 * modal * rep.OpensPerDay
		rep.HasScore = true
	}
	return rep
}

func utcDay(ms int64) int64 {
	return time.UnixMilli(ms).UTC().Unix() / 86400
}

// median of xs; 0 if empty. It sorts xs.
func median(xs []float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	slices.Sort(xs)
	n := len(xs)
	if n%2 == 1 {
		return xs[n/2]
	}
	return (xs[n/2-1] + xs[n/2]) / 2
}

// mode is the most common value and the share of xs equal to it; ties
// go to the smaller value. Zeroes if xs is empty.
func mode(xs []int64) (value int64, share float64) {
	if len(xs) == 0 {
		return 0, 0
	}
	counts := map[int64]int{}
	for _, x := range xs {
		counts[x]++
	}
	best := 0
	for x, n := range counts {
		if n > best || n == best && x < value {
			value, best = x, n
		}
	}
	return value, float64(best) / float64(len(xs))
}

// rung is one market of an event for the normal fit: the yes outcome is
// the underlying landing in (lo, hi), either bound possibly infinite.
// loAdj and hiAdj are how each bound moves, per unit of rounding
// half-width, if the underlying is a whole number (see normalFitRMSE).
type rung struct {
	lo, hi       float64
	loAdj, hiAdj float64
	p            float64 // opening mid as a probability
}

// normalFitRMSE fits one normal distribution to an event's opening mids
// across its strikes (the probability each rung's mid implies for its
// interval) by least squares, and returns the fit's RMSE in bps. ok is
// false if fewer than fitMinRungs rungs have a usable strike and mid.
//
// On a whole-number underlying (degrees, goals) the strikes are not the
// real boundaries: "54 to 55" wins on anything that rounds to 54 or 55,
// so it covers 53.5 to 55.5; "above 57" starts at 57.5 and "below 50"
// ends at 49.5. Whether an underlying is whole-number is not in the
// API, so the fit is run both ways and the better one kept.
func normalFitRMSE(rows []OpenRow) (rmseBps float64, ok bool) {
	var rungs []rung
	lo, hi := math.Inf(1), math.Inf(-1)
	for _, r := range rows {
		if r.Mid == nil {
			continue
		}
		floor, ferr := strconv.ParseFloat(r.Floor, 64)
		ceil, cerr := strconv.ParseFloat(r.Cap, 64)
		g := rung{lo: math.Inf(-1), hi: math.Inf(1), p: float64(*r.Mid) / 10000}
		switch r.StrikeType {
		case "greater", "greater_or_equal":
			if ferr != nil {
				continue
			}
			g.lo, g.loAdj = floor, 1
		case "less", "less_or_equal":
			if cerr != nil {
				continue
			}
			g.hi, g.hiAdj = ceil, -1
		case "between":
			if ferr != nil || cerr != nil || ceil < floor {
				continue
			}
			g.lo, g.hi, g.loAdj, g.hiAdj = floor, ceil, -1, 1
		default:
			continue // functional, custom, structured: no strike to fit
		}
		for _, k := range []float64{g.lo, g.hi} {
			if !math.IsInf(k, 0) {
				lo, hi = min(lo, k), max(hi, k)
			}
		}
		rungs = append(rungs, g)
	}
	if len(rungs) < fitMinRungs {
		return 0, false
	}
	span := hi - lo
	if span <= 0 {
		span = max(math.Abs(hi), 1)
	}
	best := math.Inf(1)
	for _, half := range []float64{0, 0.5} {
		best = min(best, fitNormal(rungs, lo, hi, span, half))
	}
	return math.Sqrt(best/float64(len(rungs))) * 10000, true
}

// fitNormal minimizes the sum of squared errors over (mu, sigma) and
// returns it. It starts from the best point of a coarse grid and then
// does a pattern search, halving the step when no neighbour is better:
// crude, but the surface is smooth and two-dimensional, and it needs
// nothing beyond the standard library.
func fitNormal(rungs []rung, lo, hi, span, half float64) float64 {
	sse := func(mu, sigma float64) float64 {
		sum := 0.0
		for _, g := range rungs {
			l, h := g.lo+g.loAdj*half, g.hi+g.hiAdj*half
			model := normCDF((h-mu)/sigma) - normCDF((l-mu)/sigma)
			sum += (model - g.p) * (model - g.p)
		}
		return sum
	}
	mu, sigma, best := (lo+hi)/2, span/4, math.Inf(1)
	for i := 0; i <= 16; i++ {
		m := lo - span/2 + 2*span*float64(i)/16
		for _, s := range []float64{span / 16, span / 8, span / 4, span / 2, span, 2 * span} {
			if e := sse(m, s); e < best {
				mu, sigma, best = m, s, e
			}
		}
	}
	stepMu, stepSigma := span/16, sigma/2
	for iter := 0; iter < 400 && (stepMu > span*1e-7 || stepSigma > span*1e-7); iter++ {
		improved := false
		for _, c := range [][2]float64{{mu + stepMu, sigma}, {mu - stepMu, sigma}, {mu, sigma + stepSigma}, {mu, sigma - stepSigma}} {
			if c[1] <= span*1e-9 {
				continue
			}
			if e := sse(c[0], c[1]); e < best {
				mu, sigma, best, improved = c[0], c[1], e, true
			}
		}
		if !improved {
			stepMu, stepSigma = stepMu/2, stepSigma/2
		}
	}
	return best
}

// normCDF is the standard normal CDF; it handles infinite x.
func normCDF(x float64) float64 {
	return 0.5 * math.Erfc(-x/math.Sqrt2)
}
