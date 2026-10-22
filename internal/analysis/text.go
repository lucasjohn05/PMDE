package analysis

import (
	"cmp"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"
)

// WriteText writes the report for a person to read. At most maxEpisodes
// crossing episodes are listed (the longest, in time order); the JSON
// output has all of them.
func WriteText(w io.Writer, r Report, maxEpisodes int) error {
	b := &strings.Builder{}
	pf := func(format string, args ...any) { fmt.Fprintf(b, format, args...) }

	pf("== %s\n", r.Pair)
	pf("Kalshi      %s\n", r.Kalshi)
	pf("Polymarket  %s  (%s)\n", r.Polymarket, r.Fees.Polymarket.Venue)
	pf("Range       %s to %s (%s)\n", r.From.Format(time.RFC3339), r.To.Format(time.RFC3339), dur(r.Coverage.TotalMs))
	if r.KalshiLastTrade != nil {
		pf("Kalshi last trade %s; excluded from then on\n", r.KalshiLastTrade.Format(time.RFC3339))
	}
	for _, warn := range r.Warnings {
		pf("WARNING: %s\n", warn)
	}

	c := r.Coverage
	pf("\nCoverage (%d run(s) of pmde in range)\n", c.Runs)
	row := func(label string, ms int64) {
		if ms > 0 || label == "both books trusted (analyzed)" {
			pf("  %-32s %10s  %6s\n", label, dur(ms), pct(ms, c.TotalMs))
		}
	}
	row("both books trusted (analyzed)", c.CoveredMs)
	row("after Kalshi's last trade", c.AfterLastTradeMs)
	row("pmde not running", c.DownMs)
	row("Kalshi book untrusted", c.KalshiUntrustedMs)
	row("Polymarket book untrusted", c.PolymarketUntrustedMs)
	row("both books untrusted", c.BothUntrustedMs)
	if c.CoveredMs == 0 {
		pf("\nNothing to analyze.\n")
		_, err := io.WriteString(w, b.String())
		return err
	}

	m := r.Mid
	pf("\nMid divergence, Kalshi mid - Polymarket mid (bps, weighted by time held)\n")
	pf("  mean %s   median %+d   p5 %+d   p95 %+d   max |x| %d\n", centiBps(m.MeanCentiBps), m.MedianBps, m.P5Bps, m.P95Bps, m.MaxAbsBps)

	g := r.Gap
	pf("\nExecutable gap: buy on one venue and sell on the other, both legs taker, at the\n")
	pf("smaller of the two crossing top-of-book sizes\n")
	pf("  Kalshi fee      %s, rate %s (checked %s)\n", KalshiFormula, rate(r.Fees.Kalshi.TakerRatePPM), r.Fees.Kalshi.Checked)
	for _, s := range r.Fees.Kalshi.Sources {
		pf("                  %s\n", s)
	}
	pf("  Polymarket fee  %s, rate %s; stress rate %s (checked %s)\n", PolymarketFormula, rate(r.Fees.Polymarket.TakerRatePPM), rate(r.Fees.Polymarket.StressTakerRatePPM), r.Fees.Polymarket.Checked)
	for _, s := range r.Fees.Polymarket.Sources {
		pf("                  %s\n", s)
	}
	pf("  crossed, before fees              %10s  %6s of analyzed time\n", dur(g.CrossedMs), pct(g.CrossedMs, c.CoveredMs))
	pf("  crossed, after fees               %10s  %6s\n", dur(g.NetPositiveMs), pct(g.NetPositiveMs, c.CoveredMs))
	pf("  crossed, after fees (stress rate) %10s  %6s\n", dur(g.NetPositiveStressMs), pct(g.NetPositiveStressMs, c.CoveredMs))
	pf("  episodes: %d\n", len(g.Episodes))
	if len(g.Episodes) > 0 {
		shown := longest(g.Episodes, maxEpisodes)
		pf("\n  Each at its largest gross gap. Kalshi fee/ct is the effective Kalshi fee per contract\n")
		pf("  at that size (the per-order round-up to a cent weighs more on small orders).\n")
		pf("  %-20s %9s %-5s %5s %10s %8s %8s %13s %10s %13s %10s %9s\n",
			"start (UTC)", "duration", "dir", "gap", "size", "K price", "P price", "Kalshi fee/ct", "net/ct", "net $", "stress/ct", "net>0")
		for _, e := range shown {
			s := e.Snapshot
			dir := "K>P"
			if e.Direction == BuyKalshiSellPolymarket {
				dir = "P>K"
			}
			pf("  %-20s %9s %-5s %5d %10s %8s %8s %13s %10s %13s %10s %9s\n",
				e.Start.Format("2006-01-02 15:04:05"), dur(e.DurationMs), dir, s.GapBps, size(s.SizeCenti),
				cents(s.KalshiPriceBps), cents(s.PolymarketPriceBps), perContract(s.KalshiFeePerContractMicro),
				centiBps(s.NetPerContractCentiBps), money(s.NetMicro), centiBps(s.NetStressPerContractCentiBps), dur(e.NetPositiveMs))
		}
		if n := len(g.Episodes) - len(shown); n > 0 {
			pf("  (%d shorter episode(s) not listed; -json has all)\n", n)
		}
		pf("  dir K>P: Kalshi bid above Polymarket ask (%s); P>K: the reverse.\n", SellKalshiBuyPolymarket)
		pf("  gap, net/ct and stress/ct in bps per contract; net $ for the whole size.\n")
	}

	ll := r.LeadLag
	pf("\nLead/lag (mid moves of at least %d bps)\n", ll.MoveBps)
	pf("  NOTE: %s\n", ll.Caveat)
	pf("  Transport, median t - vt (receive delay plus that venue's clock offset):\n")
	pf("    Kalshi %s (%d ticks)   Polymarket %s (%d ticks)   difference %s\n",
		optMs(ll.Transport[0].MedianMs), ll.Transport[0].Ticks, optMs(ll.Transport[1].MedianMs), ll.Transport[1].Ticks, optMs(ll.TransportDiffMs))
	pf("    A lead shorter than the difference can't be told apart from transport.\n")
	follow := func(label string, f Follow) {
		pf("  %-36s leaders %5d  followed %5d (%s)  censored %d  delay median %s p90 %s  [transport diff %s]\n",
			label, f.Leaders, f.Followed, pct(int64(f.Followed), int64(f.Leaders-f.Censored)), f.Censored,
			optMs(f.MedianMs), optMs(f.P90Ms), optMs(ll.TransportDiffMs))
	}
	pf("  Followed within %s:\n", dur(ll.FollowWindowMs))
	follow("Kalshi first, Polymarket followed:", ll.KalshiFirst)
	follow("Polymarket first, Kalshi followed:", ll.PolymarketFirst)
	pf("  same-direction moves at the same ms: %d\n", ll.Simultaneous)
	cr := ll.Corr
	pf("  1s mid-change correlation over %d s (Kalshi changed in %d, Polymarket in %d):\n", cr.Seconds, cr.KalshiMoves, cr.PolymarketMoves)
	if cr.PeakLagS != nil {
		who := "same second"
		switch {
		case *cr.PeakLagS > 0:
			who = "Kalshi first"
		case *cr.PeakLagS < 0:
			who = "Polymarket first"
		}
		pf("    peak %.3f at lag %+ds (%s)   [transport diff %s]\n", *cr.PeakCorr, *cr.PeakLagS, who, optMs(ll.TransportDiffMs))
		if min(cr.KalshiMoves, cr.PolymarketMoves) < minCorrMoves {
			pf("    fewer than %d changing seconds on a venue: the peak is likely noise\n", minCorrMoves)
		}
		var parts []string
		for _, l := range cr.Lags {
			if l.LagS >= -5 && l.LagS <= 5 && l.Corr != nil {
				parts = append(parts, fmt.Sprintf("%+d:%.2f", l.LagS, *l.Corr))
			}
		}
		pf("    lags -5..+5 s: %s\n", strings.Join(parts, " "))
	} else {
		pf("    not enough movement on both venues to correlate\n")
	}

	pf("\nNot modelled\n")
	for _, n := range r.NotModelled {
		pf("  - %s\n", n)
	}
	_, err := io.WriteString(w, b.String())
	return err
}

// minCorrMoves is how many seconds with a mid change each venue needs
// before the text report stops flagging the correlation peak as noise.
const minCorrMoves = 30

// longest returns up to n episodes, the longest ones, in time order.
func longest(eps []Episode, n int) []Episode {
	if n <= 0 || len(eps) <= n {
		return eps
	}
	out := slices.Clone(eps)
	slices.SortStableFunc(out, func(a, b Episode) int { return cmp.Compare(b.DurationMs, a.DurationMs) })
	out = out[:n]
	slices.SortStableFunc(out, func(a, b Episode) int { return a.Start.Compare(b.Start) })
	return out
}

func dur(ms int64) string {
	switch {
	case ms < 1000:
		return fmt.Sprintf("%dms", ms)
	case ms < 60_000:
		return fmt.Sprintf("%d.%ds", ms/1000, ms%1000/100)
	case ms < 3_600_000:
		return fmt.Sprintf("%dm%02ds", ms/60_000, ms%60_000/1000)
	case ms < 86_400_000:
		return fmt.Sprintf("%dh%02dm", ms/3_600_000, ms%3_600_000/60_000)
	}
	return fmt.Sprintf("%dd%02dh", ms/86_400_000, ms%86_400_000/3_600_000)
}

func pct(part, total int64) string {
	if total <= 0 {
		return "-"
	}
	pm := part * 1000 / total
	return fmt.Sprintf("%d.%d%%", pm/10, pm%10)
}

// centiBps formats hundredths of a bp with a sign: "+12.34".
func centiBps(c int64) string {
	s := "+"
	if c < 0 {
		s, c = "-", -c
	}
	return fmt.Sprintf("%s%d.%02d", s, c/100, c%100)
}

// money formats micro-dollars: "+$1.7500".
func money(micro int64) string {
	s := "+"
	if micro < 0 {
		s, micro = "-", -micro
	}
	return fmt.Sprintf("%s$%d.%04d", s, micro/1_000_000, micro%1_000_000/100)
}

// perContract formats micro-dollars per contract as cents: "1.20c".
func perContract(micro int64) string {
	return fmt.Sprintf("%d.%02dc", micro/10_000, micro%10_000/100)
}

// cents formats a price in bps as cents: "21c", "21.5c".
func cents(bps int) string {
	if bps%100 == 0 {
		return fmt.Sprintf("%dc", bps/100)
	}
	return fmt.Sprintf("%d.%02dc", bps/100, bps%100)
}

// size formats hundredths of a contract/share.
func size(c int64) string {
	if c%100 == 0 {
		return fmt.Sprintf("%d", c/100)
	}
	return fmt.Sprintf("%d.%02d", c/100, c%100)
}

func rate(ppm int64) string {
	return fmt.Sprintf("%d.%06d", ppm/1_000_000, ppm%1_000_000)
}

func optMs(v *int64) string {
	if v == nil {
		return "n/a"
	}
	return fmt.Sprintf("%dms", *v)
}
