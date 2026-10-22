// Package analysis computes a pair's report from stored ticks: how much
// of the time both books were trusted, how far apart the venues' mids
// were, how often and by how much the books crossed before and after
// fees, and which venue's mid moved first.
//
// Everything is computed from the tick files and runs.jsonl as written
// by cmd/pmde. Both venues' ticks are merged by t, the local receive
// time, so the timeline is the order this host saw things in (see
// TCaveat).
package analysis

import (
	"cmp"
	"slices"
	"time"

	"github.com/lucasjohn05/pmde/internal/divergence"
	"github.com/lucasjohn05/pmde/internal/storage"
	"github.com/lucasjohn05/pmde/internal/tick"
)

// TCaveat is printed with every lead/lag result.
const TCaveat = "t is local receive time at this host: lead/lag reflects the order messages arrived here, not true venue-side ordering."

// Options tune the lead/lag analysis.
type Options struct {
	// MoveBps is the smallest change in a venue's mid counted as a move.
	// Mids move in steps of half a tick (50 bps on 1-cent ticks).
	MoveBps int
	// FollowWindow is how long after one venue's move the other venue's
	// same-direction move still counts as following it.
	FollowWindow time.Duration
	// CorrMaxLag is the largest lag, in whole seconds, the 1-second
	// cross-correlation is computed for, in each direction.
	CorrMaxLag time.Duration
}

// DefaultOptions are the cmd/pmde-analyze flag defaults.
func DefaultOptions() Options {
	return Options{MoveBps: 50, FollowWindow: time.Minute, CorrMaxLag: 30 * time.Second}
}

// Input is one pair's data and settings.
type Input struct {
	Pair       divergence.Pair
	Kalshi     []storage.TickRecord // in file order
	Polymarket []storage.TickRecord // in file order
	Runs       []storage.RunRecord
	From, To   time.Time // the report covers [From, To)
	Fees       Fees
	Opts       Options
}

// Report is one pair's analysis. Durations are in ms, money in
// micro-dollars, prices in bps, sizes in hundredths of a contract/share.
type Report struct {
	Pair            string     `json:"pair"`
	Kalshi          string     `json:"kalshi"`
	Polymarket      string     `json:"polymarket"`
	From            time.Time  `json:"from"`
	To              time.Time  `json:"to"`
	KalshiLastTrade *time.Time `json:"kalshi_last_trade,omitempty"`
	Coverage        Coverage   `json:"coverage"`
	Mid             MidStats   `json:"mid"`
	Gap             GapStats   `json:"gap"`
	LeadLag         LeadLag    `json:"lead_lag"`
	Fees            Fees       `json:"fees"`
	NotModelled     []string   `json:"not_modelled"`
	Warnings        []string   `json:"warnings,omitempty"`
}

// Coverage splits [From, To) by why it was or wasn't analyzed. Each
// instant is counted once, in the first category that applies, in this
// order: after Kalshi's last trade, pmde not running, a book untrusted.
type Coverage struct {
	TotalMs               int64 `json:"total_ms"`
	CoveredMs             int64 `json:"covered_ms"` // both books trusted, pmde up, before the last trade
	AfterLastTradeMs      int64 `json:"after_last_trade_ms"`
	DownMs                int64 `json:"down_ms"` // pmde not known to be running (runs.jsonl)
	KalshiUntrustedMs     int64 `json:"kalshi_untrusted_ms"`
	PolymarketUntrustedMs int64 `json:"polymarket_untrusted_ms"`
	BothUntrustedMs       int64 `json:"both_untrusted_ms"`
	Runs                  int   `json:"runs"` // runs overlapping [From, To)
}

// MidStats summarize Kalshi mid minus Polymarket mid over covered time,
// weighted by how long each value held (not by tick count). MeanCentiBps
// is the mean in hundredths of a bp.
type MidStats struct {
	MeanCentiBps int64 `json:"mean_centi_bps"`
	MedianBps    int   `json:"median_bps"`
	P5Bps        int   `json:"p5_bps"`
	P95Bps       int   `json:"p95_bps"`
	MaxAbsBps    int   `json:"max_abs_bps"`
}

// GapStats describe when the books crossed. Gross is before fees; net is
// after both venues' taker fees at the executable size, with
// Polymarket's rate from fees.yaml, and "stress" with its stress rate.
type GapStats struct {
	CrossedMs           int64     `json:"crossed_ms"`
	NetPositiveMs       int64     `json:"net_positive_ms"`
	NetPositiveStressMs int64     `json:"net_positive_stress_ms"`
	Episodes            []Episode `json:"episodes"`
}

// Direction of a crossing.
const (
	SellKalshiBuyPolymarket = "sell Kalshi yes, buy Polymarket yes" // Kalshi bid > Polymarket ask
	BuyKalshiSellPolymarket = "buy Kalshi yes, sell Polymarket yes" // Polymarket bid > Kalshi ask
)

// Episode is one continuous stretch of crossed books (gross gap > 0,
// both books trusted). Its snapshot fields describe the moment of its
// largest gross gap (the first, if it recurs).
type Episode struct {
	Start      time.Time `json:"start"`
	DurationMs int64     `json:"duration_ms"`
	Direction  string    `json:"direction"`
	Snapshot   Crossing  `json:"at_max_gap"`
	// Over the whole episode.
	NetPositiveMs       int64 `json:"net_positive_ms"`
	NetPositiveStressMs int64 `json:"net_positive_stress_ms"`
	MaxNetMicro         int64 `json:"max_net_micro"`
	MaxNetStressMicro   int64 `json:"max_net_stress_micro"`
}

// Crossing is the executable trade at one moment: both legs taker, at
// the smaller of the two crossing top-of-book sizes.
type Crossing struct {
	At                 time.Time `json:"at"`
	GapBps             int       `json:"gap_bps"`
	KalshiPriceBps     int       `json:"kalshi_price_bps"`
	PolymarketPriceBps int       `json:"polymarket_price_bps"`
	SizeCenti          int64     `json:"size_centi"`
	GrossMicro         int64     `json:"gross_micro"`
	KalshiFeeMicro     int64     `json:"kalshi_fee_micro"`
	// KalshiFeePerContractMicro is the effective Kalshi fee per contract
	// at this size: the per-order round-up to a cent weighs more on small
	// orders.
	KalshiFeePerContractMicro int64 `json:"kalshi_fee_per_contract_micro"`
	PolymarketFeeMicro        int64 `json:"polymarket_fee_micro"`
	PolymarketStressFeeMicro  int64 `json:"polymarket_stress_fee_micro"`
	NetMicro                  int64 `json:"net_micro"`
	NetStressMicro            int64 `json:"net_stress_micro"`
	// Net per contract, in hundredths of a bp.
	NetPerContractCentiBps       int64 `json:"net_per_contract_centi_bps"`
	NetStressPerContractCentiBps int64 `json:"net_stress_per_contract_centi_bps"`
}

// crossing returns the executable trade if the books cross.
func crossing(k, p tick.Tick, fees Fees) (c Crossing, dir string, ok bool) {
	var size int64
	switch {
	case k.BidBps > p.AskBps:
		dir, c.GapBps = SellKalshiBuyPolymarket, k.BidBps-p.AskBps
		c.KalshiPriceBps, c.PolymarketPriceBps = k.BidBps, p.AskBps
		size = min(k.BidSize, p.AskSize)
	case p.BidBps > k.AskBps:
		dir, c.GapBps = BuyKalshiSellPolymarket, p.BidBps-k.AskBps
		c.KalshiPriceBps, c.PolymarketPriceBps = k.AskBps, p.BidBps
		size = min(k.AskSize, p.BidSize)
	default:
		return Crossing{}, "", false
	}
	c.SizeCenti = size
	// gap bps x size centi = micro-dollars: 1 bp is $0.0001 a contract,
	// and a hundredth of that is 1 micro-dollar.
	c.GrossMicro = int64(c.GapBps) * size
	c.KalshiFeeMicro = KalshiFee(fees.Kalshi.TakerRatePPM, size, c.KalshiPriceBps)
	c.PolymarketFeeMicro = PolymarketFee(fees.Polymarket.TakerRatePPM, size, c.PolymarketPriceBps)
	c.PolymarketStressFeeMicro = PolymarketFee(fees.Polymarket.StressTakerRatePPM, size, c.PolymarketPriceBps)
	c.NetMicro = c.GrossMicro - c.KalshiFeeMicro - c.PolymarketFeeMicro
	c.NetStressMicro = c.GrossMicro - c.KalshiFeeMicro - c.PolymarketStressFeeMicro
	if size > 0 {
		c.KalshiFeePerContractMicro = c.KalshiFeeMicro * 100 / size
		c.NetPerContractCentiBps = c.NetMicro * 100 / size
		c.NetStressPerContractCentiBps = c.NetStressMicro * 100 / size
	}
	return c, dir, true
}

// Point kinds, in the order they apply at the same instant: a run ending
// before one starting, both before the cutoff, ticks last (so a tick at a
// run's start lands on the reset books).
const (
	pointRunEnd = iota
	pointRunStart
	pointCutoff
	pointTick
)

type point struct {
	t     int64
	kind  int
	venue int // 0 Kalshi, 1 Polymarket (ticks only)
	seq   int // file order, for ticks at the same t
	rec   storage.TickRecord
}

// state is everything that decides whether an instant is covered.
type state struct {
	books    [2]tick.Tick // Kalshi, Polymarket
	up       int          // runs up (normally 0 or 1)
	afterCut bool
}

func (s *state) covered() bool {
	return s.up > 0 && !s.afterCut && s.books[0].OK && s.books[1].OK
}

func (s *state) mid2(venue int) int { return s.books[venue].BidBps + s.books[venue].AskBps }

// Analyze builds the report.
func Analyze(in Input) Report {
	from, to := in.From.UnixMilli(), in.To.UnixMilli()
	r := Report{
		Pair: in.Pair.Name, Kalshi: in.Pair.Kalshi, Polymarket: in.Pair.Polymarket,
		From: in.From.UTC(), To: in.To.UTC(), Fees: in.Fees, NotModelled: notModelled,
	}
	r.Coverage.TotalMs = max(to-from, 0)

	var points []point
	for v, recs := range [][]storage.TickRecord{in.Kalshi, in.Polymarket} {
		for i, rec := range recs {
			points = append(points, point{t: rec.T, kind: pointTick, venue: v, seq: i, rec: rec})
		}
	}
	spans := runSpans(in.Runs, in.Pair.Name)
	for _, run := range spans {
		if run.end > run.start {
			points = append(points, point{t: run.start, kind: pointRunStart}, point{t: run.end, kind: pointRunEnd})
		}
		if run.start < to && run.end > from {
			r.Coverage.Runs++
		}
	}
	if len(spans) == 0 {
		r.Warnings = append(r.Warnings, "no run in runs.jsonl tracked this pair, so pmde can't be shown to have been running and nothing counts as covered (data from before the run log existed?)")
	}
	if lt, ok := in.Pair.LastTrade(); ok {
		points = append(points, point{t: lt.UnixMilli(), kind: pointCutoff})
		ltUTC := lt
		r.KalshiLastTrade = &ltUTC
	}
	slices.SortStableFunc(points, func(a, b point) int {
		return cmp.Or(cmp.Compare(a.t, b.t), cmp.Compare(a.kind, b.kind), cmp.Compare(a.venue, b.venue), cmp.Compare(a.seq, b.seq))
	})

	sw := newSweep(in, from, to)
	prev := int64(-1 << 62)
	for _, pt := range points {
		if pt.t > prev {
			sw.account(max(prev, from), min(pt.t, to))
			prev = pt.t
		}
		sw.apply(pt)
	}
	sw.account(max(prev, from), to)
	sw.closeEpisode()

	r.Coverage = sw.cov(r.Coverage)
	r.Mid = midStats(sw.mids)
	r.Gap = sw.gap
	r.LeadLag = sw.leadLag(in)
	return r
}

type runSpan struct{ start, end int64 }

// runSpans turns runs.jsonl into the intervals each run that tracked
// pair is known to have been up: start to stop, or to the last alive line
// after a crash. A run whose start line lists pairs without this one
// didn't subscribe to its markets, so it doesn't count.
func runSpans(recs []storage.RunRecord, pair string) []runSpan {
	spans := map[string]*runSpan{}
	var order []string
	for _, rec := range recs {
		s, ok := spans[rec.Run]
		if !ok {
			if rec.Ev != "start" || !slices.Contains(rec.Pairs, pair) {
				continue // start line missing, or not tracking this pair
			}
			s = &runSpan{start: rec.T, end: rec.T}
			spans[rec.Run] = s
			order = append(order, rec.Run)
			continue
		}
		s.end = max(s.end, rec.T)
	}
	out := make([]runSpan, 0, len(order))
	for _, id := range order {
		out = append(out, *spans[id])
	}
	return out
}

// sweep accumulates the report over the merged timeline.
type sweep struct {
	fees     Fees
	opts     Options
	from, to int64

	st state

	// Coverage by category.
	covered, afterCut, down, kUntrusted, pUntrusted, bothUntrusted int64

	mids      map[int]int64 // mid divergence -> ms held
	gap       GapStats
	open      *Episode
	openMax   int    // the open episode's max gross gap
	intervals []span // covered intervals, merged

	moves []move

	// 1-second grid: mid (x2) per venue at each whole second, if covered
	// continuously since the previous second.
	grid []gridSample
}

type span struct{ a, b int64 }

type move struct {
	t     int64
	venue int
	sign  int
}

type gridSample struct {
	sec  int64 // Unix seconds
	mid2 [2]int
	cont bool // covered throughout (sec-1, sec]
}

func newSweep(in Input, from, to int64) *sweep {
	return &sweep{fees: in.Fees, opts: in.Opts, from: from, to: to, mids: map[int]int64{}}
}

// apply applies one point to the state, recording a mid move if it
// changed a covered venue's mid while staying covered.
func (sw *sweep) apply(pt point) {
	switch pt.kind {
	case pointRunStart:
		sw.st.up++
		sw.st.books = [2]tick.Tick{} // a new process starts with no book
	case pointRunEnd:
		sw.st.up--
		sw.st.books = [2]tick.Tick{}
	case pointCutoff:
		sw.st.afterCut = true
	case pointTick:
		wasCovered, before := sw.st.covered(), sw.st.mid2(pt.venue)
		sw.st.books[pt.venue] = toTick(pt.rec)
		if wasCovered && sw.st.covered() && pt.t >= sw.from && pt.t < sw.to {
			d := sw.st.mid2(pt.venue) - before // twice the mid change
			if abs(d) >= 2*sw.opts.MoveBps && d != 0 {
				sw.moves = append(sw.moves, move{t: pt.t, venue: pt.venue, sign: sign(d)})
			}
		}
	}
}

func toTick(r storage.TickRecord) tick.Tick {
	tk := tick.Tick{OK: r.OK}
	if r.OK && r.Bid != nil && r.Ask != nil {
		tk.BidBps, tk.AskBps = *r.Bid, *r.Ask
		if r.BidSize != nil {
			tk.BidSize = *r.BidSize
		}
		if r.AskSize != nil {
			tk.AskSize = *r.AskSize
		}
	} else {
		tk.OK = false
	}
	return tk
}

// account adds [a, b), during which the state is constant.
func (sw *sweep) account(a, b int64) {
	if b <= a {
		return
	}
	dur := b - a
	st := &sw.st
	if !st.covered() {
		switch {
		case st.afterCut:
			sw.afterCut += dur
		case st.up <= 0:
			sw.down += dur
		case !st.books[0].OK && !st.books[1].OK:
			sw.bothUntrusted += dur
		case !st.books[0].OK:
			sw.kUntrusted += dur
		default:
			sw.pUntrusted += dur
		}
		sw.closeEpisode()
		return
	}

	sw.covered += dur
	if n := len(sw.intervals); n > 0 && sw.intervals[n-1].b == a {
		sw.intervals[n-1].b = b
	} else {
		sw.intervals = append(sw.intervals, span{a, b})
	}
	k, p := st.books[0], st.books[1]
	d, _ := divergence.Compute(k, p)
	sw.mids[d.MidBps] += dur
	sw.sampleGrid(a, b)

	c, dir, crossed := crossing(k, p, sw.fees)
	if !crossed {
		sw.closeEpisode()
		return
	}
	c.At = time.UnixMilli(a).UTC()
	sw.gap.CrossedMs += dur
	if c.NetMicro > 0 {
		sw.gap.NetPositiveMs += dur
	}
	if c.NetStressMicro > 0 {
		sw.gap.NetPositiveStressMs += dur
	}
	if sw.open != nil && sw.open.Direction != dir {
		sw.closeEpisode()
	}
	e := sw.open
	if e == nil {
		e = &Episode{Start: c.At, Direction: dir, Snapshot: c, MaxNetMicro: c.NetMicro, MaxNetStressMicro: c.NetStressMicro}
		sw.open, sw.openMax = e, c.GapBps
	}
	e.DurationMs = b - e.Start.UnixMilli()
	if c.GapBps > sw.openMax {
		e.Snapshot, sw.openMax = c, c.GapBps
	}
	e.MaxNetMicro = max(e.MaxNetMicro, c.NetMicro)
	e.MaxNetStressMicro = max(e.MaxNetStressMicro, c.NetStressMicro)
	if c.NetMicro > 0 {
		e.NetPositiveMs += dur
	}
	if c.NetStressMicro > 0 {
		e.NetPositiveStressMs += dur
	}
}

func (sw *sweep) closeEpisode() {
	if sw.open != nil {
		sw.gap.Episodes = append(sw.gap.Episodes, *sw.open)
		sw.open = nil
	}
}

// sampleGrid samples both mids at each whole second in [a, b), which is
// covered with constant books.
func (sw *sweep) sampleGrid(a, b int64) {
	first := (a + 999) / 1000 // first whole second >= a
	for sec := first; sec*1000 < b; sec++ {
		// Continuous since the previous second if a covered interval
		// reaches back that far (the interval list includes [a, b)).
		iv := sw.intervals[len(sw.intervals)-1]
		cont := iv.a <= (sec-1)*1000
		sw.grid = append(sw.grid, gridSample{sec: sec, mid2: [2]int{sw.st.mid2(0), sw.st.mid2(1)}, cont: cont})
	}
}

func (sw *sweep) cov(c Coverage) Coverage {
	c.CoveredMs, c.AfterLastTradeMs, c.DownMs = sw.covered, sw.afterCut, sw.down
	c.KalshiUntrustedMs, c.PolymarketUntrustedMs, c.BothUntrustedMs = sw.kUntrusted, sw.pUntrusted, sw.bothUntrusted
	return c
}

func midStats(mids map[int]int64) MidStats {
	var total, sum int64
	var vals []int
	var s MidStats
	for v, d := range mids {
		total += d
		sum += int64(v) * d
		vals = append(vals, v)
		s.MaxAbsBps = max(s.MaxAbsBps, abs(v))
	}
	if total == 0 {
		return MidStats{}
	}
	slices.Sort(vals)
	s.MeanCentiBps = sum * 100 / total
	s.P5Bps = weightedQuantile(vals, mids, total, 5)
	s.MedianBps = weightedQuantile(vals, mids, total, 50)
	s.P95Bps = weightedQuantile(vals, mids, total, 95)
	return s
}

// weightedQuantile is the smallest value whose cumulative time reaches
// pct percent of the total.
func weightedQuantile(sorted []int, w map[int]int64, total int64, pct int64) int {
	var cum int64
	for _, v := range sorted {
		cum += w[v]
		if cum*100 >= total*pct {
			return v
		}
	}
	return sorted[len(sorted)-1]
}

func abs(x int) int {
	if x < 0 {
		return -x
	}
	return x
}

func sign(x int) int {
	switch {
	case x > 0:
		return 1
	case x < 0:
		return -1
	}
	return 0
}

var notModelled = []string{
	"Resolution-rule differences (see the pair's notes): a crossing is only an arbitrage if both contracts resolve the same way; e.g. a 12.5 bp hike resolves Polymarket's 25 bp market Yes but has no listed Kalshi outcome.",
	"Polymarket here is the international venue (polymarket.com), which is not open to US persons: executable means executable by a non-US trader with accounts on both venues.",
	"Capital: the trade locks up money on both venues until settlement, and USD (Kalshi) vs USDC (Polymarket) are treated as equal.",
	"Settlement timing and risk: Kalshi settles itself; Polymarket resolves through UMA's optimistic oracle.",
	"Depth beyond the top of book, queue position, latency, and whether the quote was still there by the time an order would arrive.",
	"Selling yes is taken as buying no at 1 - price on both venues (the same fee on both, as the formulas are symmetric).",
}
