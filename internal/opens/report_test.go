package opens

import (
	"bytes"
	"encoding/csv"
	"math"
	"strconv"
	"strings"
	"testing"
	"time"
)

var repOpen = time.Date(2026, 11, 2, 15, 0, 0, 0, time.UTC).UnixMilli()

const dayMs = 24 * 60 * 60 * 1000

func near(got, want, tol float64) bool { return math.Abs(got-want) <= tol }

// quoted returns a row with an opening quote of mid±half.
func quoted(series, event, market string, openMs int64, midBps, half int, result string) OpenRow {
	bid, ask, m := midBps-half, midBps+half, midBps
	return OpenRow{Market: market, Series: series, Event: event, OpenMs: openMs,
		Source: SourceCandles, QuoteMs: openMs + 60000, Bid: &bid, Ask: &ask, Mid: &m, Result: result}
}

func TestReportBasics(t *testing.T) {
	var rows []OpenRow
	// Series A: 6 opens over 3 UTC days (2 a day), captured, sizes
	// mostly 250 a side, 800bps wide.
	for i := range 6 {
		r := quoted("A", "A-E"+strconv.Itoa(i), "A-"+strconv.Itoa(i), repOpen+int64(i/2)*dayMs, 4000, 400, "")
		r.Source, r.QuoteMs = SourceCapture, r.OpenMs+int64(100*(i+1)) // 100..600ms
		bid, ask := int64(25000), int64(25000)
		if i == 5 {
			bid, ask = 1000, 5000
		}
		r.BidSz, r.AskSz = &bid, &ask
		rows = append(rows, r)
	}
	// Two first trades: a 20-lot yes taker moves the mid up 100bps
	// (5 bps/contract), a 10-lot no taker moves it down 20 (2/contract).
	rows[0].MidBefore, rows[0].MidAfter, rows[0].TradeCount, rows[0].TakerYes = intp(4000), intp(4100), int64p(2000), ptr(true)
	rows[1].MidBefore, rows[1].MidAfter, rows[1].TradeCount, rows[1].TakerYes = intp(4000), intp(3980), int64p(1000), ptr(false)
	// Series B: one unquoted open.
	rows = append(rows, OpenRow{Market: "B-0", Series: "B", Event: "B-E", OpenMs: repOpen, Result: "no"})

	reps := Report(rows, Options{MinOpens: 1, Seed: 1, Resamples: 300})
	if len(reps) != 2 {
		t.Fatalf("%d reports", len(reps))
	}
	var a, b SeriesReport
	for _, r := range reps {
		if r.Series == "A" {
			a = r
		} else {
			b = r
		}
	}
	if a.Opens != 6 || a.Days != 3 || a.OpensPerDay != 2 || a.Quoted != 6 || a.Captured != 6 {
		t.Errorf("counts: %+v", a)
	}
	if !a.QuoteDelayExact || a.QuoteDelayMs != 350 || a.QuoteDelayN != 6 {
		t.Errorf("quote delay %v exact %v", a.QuoteDelayMs, a.QuoteDelayExact)
	}
	if a.Sized != 6 || a.BidSizeMode != 25000 || !near(a.BidModeShare, 5.0/6, 1e-9) || a.AskSizeMode != 25000 || !near(a.AskModeShare, 5.0/6, 1e-9) {
		t.Errorf("sizes: %+v", a)
	}
	if a.SpreadBps != 800 {
		t.Errorf("spread %v", a.SpreadBps)
	}
	if a.ImpactN != 2 || !near(a.ImpactBpsPerContract, 3.5, 1e-9) {
		t.Errorf("impact %v over %d", a.ImpactBpsPerContract, a.ImpactN)
	}
	if a.Settled != 0 || a.HasScore {
		t.Errorf("unsettled series has calibration or a score: %+v", a)
	}
	if b.Opens != 1 || b.Quoted != 0 || b.Settled != 0 || b.HasScore {
		t.Errorf("unquoted series: %+v", b)
	}
	if got := Report(rows, Options{MinOpens: 2, Seed: 1, Resamples: 300}); len(got) != 1 || got[0].Series != "A" {
		t.Errorf("minOpens 2 kept %d series", len(got))
	}
}

func TestCalibrationBrierAndScore(t *testing.T) {
	var rows []OpenRow
	add := func(series string, n, midBps, yes int) {
		for i := range n {
			result := "no"
			if i < yes {
				result = "yes"
			}
			r := quoted(series, series+"-E"+strconv.Itoa(len(rows)), series+"-"+strconv.Itoa(len(rows)), repOpen, midBps, 100, result)
			bid, ask := int64(10000), int64(30000) // 100 and 300 contracts
			r.BidSz, r.AskSz = &bid, &ask
			rows = append(rows, r)
		}
	}
	// CAL is well calibrated: 20% mids resolve yes 2 in 10, 80% mids 8 in 10.
	add("CAL", 10, 2000, 2)
	add("CAL", 10, 8000, 8)
	// OFF quotes 30% for things that happen 70% of the time.
	add("OFF", 10, 3000, 7)
	// A scalar result and an unsettled open don't count.
	rows = append(rows, quoted("OFF", "OFF-X", "OFF-X", repOpen, 5000, 100, "scalar"), quoted("OFF", "OFF-Y", "OFF-Y", repOpen, 5000, 100, ""))

	reps := Report(rows, Options{MinOpens: 1, Seed: 1, Resamples: 300})
	if len(reps) != 2 || reps[0].Series != "OFF" || reps[1].Series != "CAL" {
		t.Fatalf("ranking: %v then %v, want OFF first", reps[0].Series, reps[1].Series)
	}
	off, cal := reps[0], reps[1]

	if cal.Settled != 20 || cal.MiscalBps > 1e-6 {
		t.Errorf("CAL: settled %d miscal %v, want 20 and 0", cal.Settled, cal.MiscalBps)
	}
	// Brier: each bucket is p(1-p) = 0.16.
	if !near(cal.Brier, 0.16, 1e-9) {
		t.Errorf("CAL Brier %v, want 0.16", cal.Brier)
	}
	if d := cal.Deciles[2]; d.N != 10 || !near(d.MeanBps, 2000, 1e-6) || !near(d.Freq, 0.2, 1e-9) {
		t.Errorf("CAL 20-30%% decile %+v", d)
	}
	if d := cal.Deciles[8]; d.N != 10 || !near(d.Freq, 0.8, 1e-9) {
		t.Errorf("CAL 80-90%% decile %+v", d)
	}
	if !cal.HasGross || cal.GrossScore > 1e-6 {
		t.Errorf("CAL gross score %v, want 0", cal.GrossScore)
	}

	if off.Settled != 10 || !near(off.MiscalBps, 4000, 1e-6) {
		t.Errorf("OFF: settled %d miscal %v, want 10 and 4000", off.Settled, off.MiscalBps)
	}
	// 7 x (0.3-1)^2 + 3 x 0.3^2 over 10.
	if !near(off.Brier, (7*0.49+3*0.09)/10, 1e-9) {
		t.Errorf("OFF Brier %v", off.Brier)
	}
	// 0.40 miscalibration x 200 contracts (mean of 100 and 300) x 12 opens a day.
	if !off.HasGross || !near(off.GrossScore, 0.4*200*12, 1e-6) {
		t.Errorf("OFF gross score %v, want %v", off.GrossScore, 0.4*200*12)
	}
}

// ladder builds one event's rungs from a normal model with the given
// mean and sd: thresholds every step from lo, as "greater" markets.
func ladder(series, event string, mean, sd, lo, step float64, n int, noise func(i int) int) []OpenRow {
	var rows []OpenRow
	for i := range n {
		k := lo + float64(i)*step
		p := 1 - normCDF((k-mean)/sd)
		midBps := int(math.Round(p*100))*100 + noise(i) // whole cents, as quoted
		r := quoted(series, event, event+"-T"+strconv.Itoa(i), repOpen, midBps, 100, "")
		r.StrikeType, r.Floor = "greater", strconv.FormatFloat(k, 'f', -1, 64)
		rows = append(rows, r)
	}
	return rows
}

func TestNormalFitRMSE(t *testing.T) {
	none := func(int) int { return 0 }
	// Mids straight off a normal model, rounded to the cent: the fit
	// recovers it to within rounding.
	clean := ladder("N", "N-E1", 87.3, 1.9, 83.5, 0.5, 14, none)
	rmse, ok := normalFitRMSE(clean)
	if !ok || rmse > 40 {
		t.Errorf("clean ladder: RMSE %v bps (ok %v), want under 40 (cent rounding)", rmse, ok)
	}
	// The same ladder with 6c of alternating noise does not fit.
	noisy := ladder("N", "N-E2", 87.3, 1.9, 83.5, 0.5, 14, func(i int) int { return 600 * (2*(i%2) - 1) })
	if rmse, ok := normalFitRMSE(noisy); !ok || rmse < 400 {
		t.Errorf("noisy ladder: RMSE %v bps (ok %v), want over 400", rmse, ok)
	}
	// Two rungs always fit; they are not fitted.
	if _, ok := normalFitRMSE(clean[:2]); ok {
		t.Errorf("a two-rung event was fitted")
	}

	// A weather-style event: a "less" tail, "between" buckets on whole
	// degrees (each covering half a degree beyond its bounds), and a
	// "greater" tail, priced off N(53, 2).
	mean, sd := 53.0, 2.0
	mk := func(name, kind, floor, capStrike string, lo, hi float64) OpenRow {
		p := normCDF((hi-mean)/sd) - normCDF((lo-mean)/sd)
		r := quoted("W", "W-E", name, repOpen, int(math.Round(p*100))*100, 100, "")
		r.StrikeType, r.Floor, r.Cap = kind, floor, capStrike
		return r
	}
	weather := []OpenRow{
		mk("W-T50", "less", "", "50", math.Inf(-1), 49.5),
		mk("W-B50", "between", "50", "51", 49.5, 51.5),
		mk("W-B52", "between", "52", "53", 51.5, 53.5),
		mk("W-B54", "between", "54", "55", 53.5, 55.5),
		mk("W-B56", "between", "56", "57", 55.5, 57.5),
		mk("W-T57", "greater", "57", "", 57.5, math.Inf(1)),
		{Market: "W-CUSTOM", Series: "W", Event: "W-E", StrikeType: "custom", Mid: intp(5000), Bid: intp(4900), Ask: intp(5100)},
	}
	if rmse, ok := normalFitRMSE(weather); !ok || rmse > 60 {
		t.Errorf("weather ladder: RMSE %v bps (ok %v), want under 60", rmse, ok)
	}

	// Report takes the median over a series' events.
	reps := Report(append(clean, noisy...), Options{MinOpens: 1, Seed: 1, Resamples: 300})
	if len(reps) != 1 || reps[0].FitEvents != 2 || reps[0].FitRMSEBps < 100 {
		t.Errorf("series fit: %d events, RMSE %v", reps[0].FitEvents, reps[0].FitRMSEBps)
	}
}

func TestWriteTextAndCSV(t *testing.T) {
	rows := ladder("N", "N-E1", 87.3, 1.9, 83.5, 0.5, 14, func(int) int { return 0 })
	for i := range rows {
		rows[i].Result = "no"
		if i < 3 {
			rows[i].Result = "yes"
		}
	}
	rows = append(rows, OpenRow{Market: "Q-0", Series: "Q", Event: "Q-E", OpenMs: repOpen})
	reps := Report(rows, Options{MinOpens: 1, Seed: 1, Resamples: 300})

	var text bytes.Buffer
	if err := WriteText(&text, reps, true); err != nil {
		t.Fatal(err)
	}
	out := text.String()
	for _, want := range []string{"rank", "series", "score", "N", "Q", "opening mid vs realized frequency (14 settled opens in 1 events", "≤60000ms"} {
		if !strings.Contains(out, want) {
			t.Errorf("text output lacks %q:\n%s", want, out)
		}
	}

	var buf bytes.Buffer
	if err := WriteCSV(&buf, reps); err != nil {
		t.Fatal(err)
	}
	recs, err := csv.NewReader(&buf).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 3 || len(recs[0]) != 35+30 {
		t.Fatalf("%d CSV rows of %d columns, want 3 of 65", len(recs), len(recs[0]))
	}
	col := map[string]int{}
	for i, h := range recs[0] {
		col[h] = i
	}
	n := recs[1]
	if n[col["series"]] != "N" || n[col["rank"]] != "1" || n[col["opens"]] != "14" || n[col["settled"]] != "14" ||
		n[col["fit_events"]] != "1" || n[col["score"]] != "" || n[col["bid_size_mode"]] != "" || n[col["quote_delay_exact"]] != "false" {
		t.Errorf("CSV row for N: %v", n)
	}
	if q := recs[2]; q[col["series"]] != "Q" || q[col["brier"]] != "" || q[col["spread_bps"]] != "" || q[col["d0_n"]] != "0" {
		t.Errorf("CSV row for Q: %v", q)
	}
}
