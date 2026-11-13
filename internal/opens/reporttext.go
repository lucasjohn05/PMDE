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
	fmt.Fprintln(tw, "rank\tseries\topens\t/day\tquoted\tcapt\tt→quote\tbid sz\tshare\task sz\tshare\tspread\tfit rmse\timpact\tsettled\tbrier\tmiscal\tscore\t")
	for i, r := range reps {
		delay := na(r.QuoteDelayN > 0, "%.0fms", r.QuoteDelayMs)
		if r.QuoteDelayN > 0 && !r.QuoteDelayExact {
			delay = "≤" + delay // candlesticks: an upper bound, to the minute
		}
		fmt.Fprintf(tw, "%d\t%s\t%d\t%.1f\t%d\t%d\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%d\t%s\t%s\t%s\t\n",
			i+1, r.Series, r.Opens, r.OpensPerDay, r.Quoted, r.Captured, delay,
			na(r.Sized > 0, "%s", centi(r.BidSizeMode)), na(r.Sized > 0, "%.0f%%", r.BidModeShare*100),
			na(r.Sized > 0, "%s", centi(r.AskSizeMode)), na(r.Sized > 0, "%.0f%%", r.AskModeShare*100),
			na(r.Quoted > 0, "%.0f", r.SpreadBps),
			na(r.FitEvents > 0, "%.0f (%d)", r.FitRMSEBps, r.FitEvents),
			na(r.ImpactN > 0, "%.2f (%d)", r.ImpactBpsPerContract, r.ImpactN),
			r.Settled,
			na(r.Settled > 0, "%.4f", r.Brier), na(r.Settled > 0, "%.0f", r.MiscalBps),
			na(r.HasScore, "%.2f", r.Score))
	}
	if err := tw.Flush(); err != nil {
		return err
	}
	fmt.Fprint(w, `
opens: markets with a row; /day: per UTC day over the span seen. quoted: with a first two-sided quote;
capt: of those, from a capture (else from 1-minute candlesticks). t→quote: median open to first
two-sided quote (≤: candlestick-only, to the minute). bid/ask sz: modal opening size in contracts and
the share of captured opens matching it. spread, fit rmse, miscal: bps. fit rmse: median RMSE of a
normal-CDF fit to an event's opening mids across rungs (events fitted). impact: median mid move after
the first trade, in the taker's direction, bps per contract (trades). brier, miscal: opening mid vs
result. score = miscal/10000 x modal size x opens/day; "-" where a factor is missing.
`)
	if !deciles {
		return nil
	}
	for _, r := range reps {
		if r.Settled == 0 {
			continue
		}
		fmt.Fprintf(w, "\n%s: opening mid vs realized frequency (%d settled opens)\n", r.Series, r.Settled)
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
		"settled", "brier", "miscal_bps", "score"}
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
			strconv.Itoa(r.Settled), f(r.Settled > 0, r.Brier, 6), f(r.Settled > 0, r.MiscalBps, 1), f(r.HasScore, r.Score, 4),
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
