package opens

import (
	"encoding/csv"
	"fmt"
	"io"
	"strconv"
	"text/tabwriter"
)

// centi formats hundredths of a contract as contracts: 25000 -> "250",
// 1250 -> "12.5".
func centi(n int64) string {
	return strconv.FormatFloat(float64(n)/100, 'f', -1, 64)
}

func na(ok bool, format string, args ...any) string {
	if !ok {
		return "-"
	}
	return fmt.Sprintf(format, args...)
}

// WriteText prints the ranked table, and, with deciles, each series'
// calibration table under it.
func WriteText(w io.Writer, reps []SeriesReport, deciles bool) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', tabwriter.AlignRight)
	fmt.Fprintln(tw, "rank\tseries\topens\t/day\tquoted\tcapt\tt→quote\tbid sz\tshare\task sz\tshare\tspread\tfit rmse\timpact\tsettled\tevents\tbrier\tmiscal [90%]\tnull\texcess\tfee\tnet edge\tscore\tgross\tcap/day (ntm/wing)\t$risk/day (ntm/wing)\t")
	for i, r := range reps {
		delay := na(r.QuoteDelayN > 0, "%.0fms", r.QuoteDelayMs)
		if r.QuoteDelayN > 0 && !r.QuoteDelayExact {
			delay = "≤" + delay // candlesticks: an upper bound, to the minute
		}
		// ≥: some opening quotes are candle closes, so the measured
		// miscalibration is a lower bound.
		lb := ""
		if r.MiscalLowerBound {
			lb = "≥"
		}
		fmt.Fprintf(tw, "%d\t%s\t%d\t%.1f\t%d\t%d\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%d\t%d\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t\n",
			i+1, r.Series, r.Opens, r.OpensPerDay, r.Quoted, r.Captured, delay,
			na(r.Sized > 0, "%s", centi(r.BidSizeMode)), na(r.Sized > 0, "%.0f%%", r.BidModeShare*100),
			na(r.Sized > 0, "%s", centi(r.AskSizeMode)), na(r.Sized > 0, "%.0f%%", r.AskModeShare*100),
			na(r.Quoted > 0, "%.0f", r.SpreadBps),
			na(r.FitEvents > 0, "%.0f (%d)", r.FitRMSEBps, r.FitEvents),
			na(r.ImpactN > 0, "%.2f (%d)", r.ImpactBpsPerContract, r.ImpactN),
			r.Settled, r.CalEvents,
			na(r.Settled > 0, "%.4f", r.Brier),
			na(r.Settled > 0, "%s%.0f [%.0f, %.0f]", lb, r.MiscalBps, r.LoBps, r.HiBps),
			na(r.Settled > 0, "%.0f", r.NullBps),
			na(r.Settled > 0, "%s%.0f", lb, r.ExcessBps),
			na(r.HasFee, "%.0f", r.FeeBps),
			na(r.HasNet, "%s%.0f", lb, r.NetEdgeBps),
			na(r.HasScore, "%s%.2f", lb, r.Score),
			na(r.HasGross, "%.2f", r.GrossScore),
			na(r.CapDays > 0, "%.0f (%.0f/%.0f)", r.Contracts, r.NTMContracts, r.WingContracts),
			na(r.CapDays > 0, "%.0f (%.0f/%.0f)", r.Dollars, r.NTMDollars, r.WingDollars))
	}
	if err := tw.Flush(); err != nil {
		return err
	}
	fmt.Fprint(w, `
opens: markets with a row; /day: per UTC day over the span seen. quoted: with a first two-sided quote;
capt: of those, from a capture (else from 1-minute candlesticks). t→quote: median open to first
two-sided quote (≤: candlestick-only, to the minute). bid/ask sz: modal opening size in contracts and
the share of captured opens matching it. spread, fit rmse, miscal, null, excess: bps. fit rmse: median
RMSE of a normal-CDF fit to an event's opening mids across rungs (events fitted). impact: median mid
move after the first trade, in the taker's direction, bps per contract (trades).
settled, events: opens with a yes/no result and an opening quote, and the events they belong to. One
outcome decides all the rungs of an event, so events are the sample size. brier: opening mid vs result.
miscal: decile-weighted mean |mean mid - realized frequency|, with a 90% interval for the true value
from resampling events (bias-corrected, so it can lie below the estimate). null: what miscal would average if the mids were exactly right (outcomes simulated from the
mids, one draw per event); it is not zero, and grows as events get fewer. excess = miscal - null,
floored at 0. ≥: some of the opening quotes are 1-minute candle closes, up to a minute late and possibly
already repriced, so the miscalibration shown is a lower bound.
fee: taker fee per contract at the median opening mid, for an order of the modal size (100 contracts
where no sizes were captured), from fees.yaml's rate and the series' fee_multiplier.
net edge = excess - half the median spread - fee, floored at 0.
score = net edge/10000 x modal size x opens/day. gross = miscal/10000 x modal size x opens/day: the
old score, with nothing taken off. "-" where a factor is missing. Series with no score rank by cap/day.
cap/day: contracts resting at the opening quote's bid and ask (both sides, every rung opened that UTC
day), on quotes meeting the quote rule; median over days with a captured open. ntm: opening mid 10-90c;
wing: the rest. $risk/day: the dollars behind them, the maker's collateral (bid x size, (1 - ask) x
size). From captures only: "-" without any.
`)
	if !deciles {
		return nil
	}
	for _, r := range reps {
		if r.Settled == 0 {
			continue
		}
		note := ""
		if r.MiscalLowerBound {
			note = fmt.Sprintf("; %d from candle closes, so gaps are lower bounds", r.CandleCal)
		}
		fmt.Fprintf(w, "\n%s: opening mid vs realized frequency (%d settled opens in %d events%s)\n", r.Series, r.Settled, r.CalEvents, note)
		tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', tabwriter.AlignRight)
		fmt.Fprintln(tw, "mid\tn\tmean mid\trealized\tgap\t")
		for d, dec := range r.Deciles {
			if dec.N == 0 {
				continue
			}
			fmt.Fprintf(tw, "%d-%d%%\t%d\t%.1f%%\t%.1f%%\t%+.1f\t\n",
				d*10, d*10+10, dec.N, dec.MeanBps/100, dec.Freq*100, dec.Freq*100-dec.MeanBps/100)
		}
		if err := tw.Flush(); err != nil {
			return err
		}
	}
	return nil
}

// WriteCSV writes the same table as CSV, one row per series in rank
// order, with every decile's count, mean mid (bps) and realized
// frequency. Statistics with nothing behind them are empty cells.
func WriteCSV(w io.Writer, reps []SeriesReport) error {
	header := []string{"rank", "series", "opens", "days", "opens_per_day", "quoted", "captured",
		"quote_delay_ms", "quote_delay_exact", "sized", "bid_size_mode", "bid_mode_share", "ask_size_mode", "ask_mode_share",
		"spread_bps", "fit_events", "fit_rmse_bps", "impact_trades", "impact_bps_per_contract",
		"settled", "cal_events", "cal_from_candles", "miscal_is_lower_bound", "brier",
		"miscal_bps", "miscal_lo_bps", "miscal_hi_bps", "null_miscal_bps", "excess_miscal_bps",
		"median_mid_bps", "fee_bps", "fee_size", "net_edge_bps", "score", "gross_score",
		"cap_days", "cap_contracts", "cap_ntm_contracts", "cap_wing_contracts", "risk_dollars", "risk_ntm_dollars", "risk_wing_dollars"}
	for d := range 10 {
		header = append(header, fmt.Sprintf("d%d_n", d), fmt.Sprintf("d%d_mean_mid_bps", d), fmt.Sprintf("d%d_realized", d))
	}
	cw := csv.NewWriter(w)
	if err := cw.Write(header); err != nil {
		return err
	}
	f := func(ok bool, v float64, prec int) string {
		if !ok {
			return ""
		}
		return strconv.FormatFloat(v, 'f', prec, 64)
	}
	for i, r := range reps {
		sized := r.Sized > 0
		row := []string{
			strconv.Itoa(i + 1), r.Series, strconv.Itoa(r.Opens), strconv.Itoa(r.Days), f(true, r.OpensPerDay, 3),
			strconv.Itoa(r.Quoted), strconv.Itoa(r.Captured),
			f(r.QuoteDelayN > 0, r.QuoteDelayMs, 0), strconv.FormatBool(r.QuoteDelayExact), strconv.Itoa(r.Sized),
			f(sized, float64(r.BidSizeMode)/100, 2), f(sized, r.BidModeShare, 4),
			f(sized, float64(r.AskSizeMode)/100, 2), f(sized, r.AskModeShare, 4),
			f(r.Quoted > 0, r.SpreadBps, 1), strconv.Itoa(r.FitEvents), f(r.FitEvents > 0, r.FitRMSEBps, 1),
			strconv.Itoa(r.ImpactN), f(r.ImpactN > 0, r.ImpactBpsPerContract, 4),
			strconv.Itoa(r.Settled), strconv.Itoa(r.CalEvents), strconv.Itoa(r.CandleCal), strconv.FormatBool(r.MiscalLowerBound),
			f(r.Settled > 0, r.Brier, 6),
			f(r.Settled > 0, r.MiscalBps, 1), f(r.Settled > 0, r.LoBps, 1), f(r.Settled > 0, r.HiBps, 1),
			f(r.Settled > 0, r.NullBps, 1), f(r.Settled > 0, r.ExcessBps, 1),
			f(r.Quoted > 0, r.MedianMidBps, 0), f(r.HasFee, r.FeeBps, 2), f(r.HasFee, float64(r.FeeSizeCenti)/100, 2),
			f(r.HasNet, r.NetEdgeBps, 1), f(r.HasScore, r.Score, 4), f(r.HasGross, r.GrossScore, 4),
			strconv.Itoa(r.CapDays), f(r.CapDays > 0, r.Contracts, 2), f(r.CapDays > 0, r.NTMContracts, 2), f(r.CapDays > 0, r.WingContracts, 2),
			f(r.CapDays > 0, r.Dollars, 2), f(r.CapDays > 0, r.NTMDollars, 2), f(r.CapDays > 0, r.WingDollars, 2),
		}
		for _, dec := range r.Deciles {
			row = append(row, strconv.Itoa(dec.N), f(dec.N > 0, dec.MeanBps, 1), f(dec.N > 0, dec.Freq, 4))
		}
		if err := cw.Write(row); err != nil {
			return err
		}
	}
	cw.Flush()
	return cw.Error()
}
