package opens

// Near the money is an opening mid from 10c to 90c inclusive; the rest
// are wings.
const (
	ntmLowBps  = 1000
	ntmHighBps = 9000
)

// nearTheMoney reports whether an opening mid is near the money.
func nearTheMoney(midBps int) bool { return midBps >= ntmLowBps && midBps <= ntmHighBps }

// Capacity is how much a series offers at its opens, per UTC day, on
// quotes that meet the quote rule: the contracts resting at the opening
// quote's bid and ask (both sides, summed across every rung opened that
// day), and the dollars at risk behind them.
//
// Dollars at risk is the maker's collateral on the opening quote: a bid
// of B for S contracts risks B x S, an ask of A for S risks (1 - A) x S.
// It is also the most a taker of the whole quote could win.
//
// Only captured opens have sizes. A day counts if it has a captured
// open; a captured open without a qualifying quote adds nothing to its
// day. Each figure is the median over those days, taken on its own, so
// the near-the-money and wing medians need not add up to the total.
type Capacity struct {
	CapDays int // UTC days with a captured open

	Contracts, NTMContracts, WingContracts float64 // median daily
	Dollars, NTMDollars, WingDollars       float64 // median daily
}

// capacity computes a series' Capacity from its rows, already held to
// the quote rule.
func capacity(rows []OpenRow) Capacity {
	type day struct{ ntm, wing, ntmRisk, wingRisk float64 }
	days := map[int64]*day{}
	for _, r := range rows {
		if !r.CaptureSeen {
			continue
		}
		d := days[utcDay(r.OpenMs)]
		if d == nil {
			d = &day{}
			days[utcDay(r.OpenMs)] = d
		}
		if r.Source != SourceCapture || r.BidSz == nil || r.AskSz == nil || r.Mid == nil {
			continue
		}
		contracts := float64(*r.BidSz+*r.AskSz) / 100
		// bps x hundredths of a contract -> dollars.
		risk := (float64(*r.Bid)*float64(*r.BidSz) + float64(10000-*r.Ask)*float64(*r.AskSz)) / 1e6
		if nearTheMoney(*r.Mid) {
			d.ntm, d.ntmRisk = d.ntm+contracts, d.ntmRisk+risk
		} else {
			d.wing, d.wingRisk = d.wing+contracts, d.wingRisk+risk
		}
	}
	c := Capacity{CapDays: len(days)}
	if len(days) == 0 {
		return c
	}
	var all, ntm, wing, risk, ntmRisk, wingRisk []float64
	for _, d := range days {
		all, ntm, wing = append(all, d.ntm+d.wing), append(ntm, d.ntm), append(wing, d.wing)
		risk, ntmRisk, wingRisk = append(risk, d.ntmRisk+d.wingRisk), append(ntmRisk, d.ntmRisk), append(wingRisk, d.wingRisk)
	}
	c.Contracts, c.NTMContracts, c.WingContracts = median(all), median(ntm), median(wing)
	c.Dollars, c.NTMDollars, c.WingDollars = median(risk), median(ntmRisk), median(wingRisk)
	return c
}
