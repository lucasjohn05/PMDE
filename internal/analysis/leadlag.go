package analysis

import (
	"cmp"
	"math"
	"slices"

	"github.com/lucasjohn05/pmde/internal/storage"
)

// LeadLag is which venue's mid tends to move first, on this host's
// receive clock (see TCaveat).
type LeadLag struct {
	Caveat string `json:"caveat"`
	// Transport is each venue's median t - vt over the ticks in range
	// that carry a venue timestamp: network and processing delay plus
	// that venue's clock offset from this host. The clock error of this
	// host cancels in the difference between the two; the venues' own
	// offsets don't. A lead shorter than |TransportDiffMs| can't be told
	// apart from one venue's messages simply taking longer to get here.
	Transport       [2]Transport `json:"transport"`         // Kalshi, Polymarket
	TransportDiffMs *int64       `json:"transport_diff_ms"` // Polymarket median - Kalshi median

	MoveBps         int    `json:"move_bps"`
	FollowWindowMs  int64  `json:"follow_window_ms"`
	KalshiFirst     Follow `json:"kalshi_first"`     // Kalshi moves, Polymarket follows
	PolymarketFirst Follow `json:"polymarket_first"` // Polymarket moves, Kalshi follows
	Simultaneous    int    `json:"simultaneous"`     // same-direction moves on both at the same ms

	Corr Correlation `json:"correlation"`
}

// Transport is one venue's receive delay as measured against its own
// timestamps.
type Transport struct {
	Ticks    int    `json:"ticks"` // ticks with a venue timestamp
	MedianMs *int64 `json:"median_ms"`
}

// Follow counts one direction. Moves are matched in time order, one to
// one: a move follows the earliest still-unmatched same-direction move on
// the other venue at most the window earlier (with both books trusted in
// between); a move that follows nothing is a leader. A leader is followed
// if it gets matched, censored if coverage ends before its window does
// without a match, and otherwise not followed. A match at the same ms is
// counted as simultaneous, not as a lead.
type Follow struct {
	Leaders  int    `json:"leaders"`
	Followed int    `json:"followed"`
	Censored int    `json:"censored"`
	MedianMs *int64 `json:"median_delay_ms"`
	P90Ms    *int64 `json:"p90_delay_ms"`
}

// Correlation is the Pearson correlation of 1-second changes in the two
// mids, Kalshi's change at second s against Polymarket's at s + lag, for
// lags from -MaxLag to +MaxLag seconds. A peak at a positive lag means
// Polymarket's changes tend to come after Kalshi's.
type Correlation struct {
	Seconds         int       `json:"seconds"` // seconds with both changes known
	KalshiMoves     int       `json:"kalshi_nonzero"`
	PolymarketMoves int       `json:"polymarket_nonzero"`
	Lags            []LagCorr `json:"lags"`
	PeakLagS        *int      `json:"peak_lag_s"`
	PeakCorr        *float64  `json:"peak_corr"`
}

// LagCorr is the correlation at one lag; Corr is nil if either series
// has no variance over the overlapping seconds.
type LagCorr struct {
	LagS int      `json:"lag_s"`
	N    int      `json:"n"`
	Corr *float64 `json:"corr"`
}

func (sw *sweep) leadLag(in Input) LeadLag {
	ll := LeadLag{
		Caveat:         TCaveat,
		MoveBps:        sw.opts.MoveBps,
		FollowWindowMs: sw.opts.FollowWindow.Milliseconds(),
	}
	for v, recs := range [][]storage.TickRecord{in.Kalshi, in.Polymarket} {
		ll.Transport[v] = transport(recs, sw.from, sw.to)
	}
	if k, p := ll.Transport[0].MedianMs, ll.Transport[1].MedianMs; k != nil && p != nil {
		d := *p - *k
		ll.TransportDiffMs = &d
	}
	ll.KalshiFirst, ll.PolymarketFirst, ll.Simultaneous = sw.follows()
	ll.Corr = sw.correlation()
	return ll
}

func transport(recs []storage.TickRecord, from, to int64) Transport {
	var d []int64
	for _, r := range recs {
		if r.VT != 0 && r.T >= from && r.T < to {
			d = append(d, r.T-r.VT)
		}
	}
	return Transport{Ticks: len(d), MedianMs: percentile(d, 50)}
}

// percentile is the nearest-rank percentile of xs (sorted in place), or
// nil if xs is empty.
func percentile(xs []int64, pct int) *int64 {
	if len(xs) == 0 {
		return nil
	}
	slices.Sort(xs)
	i := (len(xs)*pct + 99) / 100 // nearest rank, 1-based
	v := xs[max(i, 1)-1]
	return &v
}

func (sw *sweep) follows() (kFirst, pFirst Follow, simultaneous int) {
	w := sw.opts.FollowWindow.Milliseconds()
	results := [2]*Follow{&kFirst, &pFirst}
	var delays [2][]int64
	var pending [2][]move    // unmatched leaders per venue, oldest first
	settle := func(l move) { // a leader whose window passed unmatched
		if !sw.coveredThrough(l.t, l.t+w) {
			results[l.venue].Censored++
		}
	}
	for _, m := range sw.moves { // in time order
		other := 1 - m.venue
		matched := false
		var keep []move
		for _, l := range pending[other] {
			switch {
			case matched:
				keep = append(keep, l)
			case l.t < m.t-w:
				settle(l) // expired
			case l.sign == m.sign && sw.coveredThrough(l.t, m.t):
				matched = true
				if d := m.t - l.t; d == 0 {
					simultaneous++
					results[l.venue].Leaders-- // not a lead after all
				} else {
					results[l.venue].Followed++
					delays[l.venue] = append(delays[l.venue], d)
				}
			default:
				keep = append(keep, l)
			}
		}
		pending[other] = keep
		if !matched {
			results[m.venue].Leaders++
			pending[m.venue] = append(pending[m.venue], m)
		}
	}
	for v := range pending {
		for _, l := range pending[v] {
			settle(l)
		}
	}
	for v, f := range results {
		f.MedianMs = percentile(delays[v], 50)
		f.P90Ms = percentile(delays[v], 90)
	}
	return kFirst, pFirst, simultaneous
}

// coveredThrough reports whether [a, b] is inside one covered interval
// (and inside the report's range).
func (sw *sweep) coveredThrough(a, b int64) bool {
	if b > sw.to {
		return false
	}
	i, _ := slices.BinarySearchFunc(sw.intervals, a, func(s span, t int64) int { return cmp.Compare(s.b, t+1) })
	return i < len(sw.intervals) && sw.intervals[i].a <= a && sw.intervals[i].b >= b
}

func (sw *sweep) correlation() Correlation {
	var c Correlation
	if len(sw.grid) == 0 {
		return c
	}
	first := sw.grid[0].sec
	n := int(sw.grid[len(sw.grid)-1].sec-first) + 1
	dk, dp := make([]int, n), make([]int, n)
	ok := make([]bool, n)
	for i := 1; i < len(sw.grid); i++ {
		g, prev := sw.grid[i], sw.grid[i-1]
		if !g.cont || prev.sec != g.sec-1 {
			continue
		}
		j := int(g.sec - first)
		dk[j], dp[j], ok[j] = g.mid2[0]-prev.mid2[0], g.mid2[1]-prev.mid2[1], true
		c.Seconds++
		if dk[j] != 0 {
			c.KalshiMoves++
		}
		if dp[j] != 0 {
			c.PolymarketMoves++
		}
	}
	maxLag := int(sw.opts.CorrMaxLag.Seconds())
	for lag := -maxLag; lag <= maxLag; lag++ {
		var xs, ys []int
		for j := range n {
			if jj := j + lag; ok[j] && jj >= 0 && jj < n && ok[jj] {
				xs, ys = append(xs, dk[j]), append(ys, dp[jj])
			}
		}
		lc := LagCorr{LagS: lag, N: len(xs), Corr: pearson(xs, ys)}
		c.Lags = append(c.Lags, lc)
		if lc.Corr != nil && (c.PeakCorr == nil || *lc.Corr > *c.PeakCorr) {
			l, v := lag, *lc.Corr
			c.PeakLagS, c.PeakCorr = &l, &v
		}
	}
	return c
}

// pearson is the correlation coefficient of xs and ys, or nil if either
// has no variance. Floats are fine here: these are statistics of price
// changes, not prices.
func pearson(xs, ys []int) *float64 {
	n := float64(len(xs))
	if n < 2 {
		return nil
	}
	var sx, sy, sxx, syy, sxy float64
	for i := range xs {
		x, y := float64(xs[i]), float64(ys[i])
		sx, sy, sxx, syy, sxy = sx+x, sy+y, sxx+x*x, syy+y*y, sxy+x*y
	}
	vx, vy := sxx-sx*sx/n, syy-sy*sy/n
	if vx <= 0 || vy <= 0 {
		return nil
	}
	r := (sxy - sx*sy/n) / math.Sqrt(vx*vy)
	return &r
}
