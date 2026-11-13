package opens

import (
	"bytes"
	"math"
	"math/rand/v2"
	"strconv"
	"strings"
	"testing"
)

var calOpts = Options{MinOpens: 1, Seed: 7, Resamples: 1000}

// calibratedRows builds n single-rung events whose outcomes are drawn
// from their own opening mids: perfectly calibrated quotes.
func calibratedRows(series string, n int, rng *rand.Rand) []OpenRow {
	var rows []OpenRow
	for i := range n {
		midBps := 500 + 100*rng.IntN(91) // 5% to 95%, whole cents
		result := "no"
		if rng.Float64() < float64(midBps)/10000 {
			result = "yes"
		}
		rows = append(rows, quoted(series, series+"-E"+strconv.Itoa(i), series+"-"+strconv.Itoa(i), repOpen, midBps, 100, result))
	}
	return rows
}

// Perfectly calibrated quotes still show a positive miscal, more of it
// the fewer the events. The null reproduces it, so the excess is small.
func TestNullMiscalIsTheNoiseFloor(t *testing.T) {
	rng := rand.New(rand.NewPCG(11, 12))
	big := Report(calibratedRows("BIG", 400, rng), calOpts)[0]
	small := Report(calibratedRows("SMALL", 12, rng), calOpts)[0]

	if big.CalEvents != 400 || small.CalEvents != 12 {
		t.Fatalf("events %d and %d", big.CalEvents, small.CalEvents)
	}
	if big.MiscalBps <= 0 || small.NullBps <= big.NullBps*2 {
		t.Errorf("null miscal %0.f bps at 400 events and %.0f at 12: want it positive and much larger for the small sample", big.NullBps, small.NullBps)
	}
	if math.Abs(big.MiscalBps-big.NullBps) > 250 || big.ExcessBps > 250 {
		t.Errorf("calibrated quotes, 400 events: miscal %.0f, null %.0f, excess %.0f; want miscal near its null", big.MiscalBps, big.NullBps, big.ExcessBps)
	}
	if small.ExcessBps > small.MiscalBps/2 {
		t.Errorf("calibrated quotes, 12 events: miscal %.0f but excess %.0f; the null (%.0f) should account for most of it", small.MiscalBps, small.ExcessBps, small.NullBps)
	}
	// The truth here is zero. The interval is for the truth, with the
	// estimator's upward bias reflected out, so it reaches down toward
	// zero rather than sitting above the estimate.
	if big.LoBps > 100 || big.HiBps > big.MiscalBps+100 || big.HiBps < big.LoBps {
		t.Errorf("calibrated quotes: interval [%.0f, %.0f] around miscal %.0f; want it to reach down to about zero", big.LoBps, big.HiBps, big.MiscalBps)
	}
	// (At 12 events each decile holds one or two opens and the bootstrap
	// has little to work with; there the null, checked above, is the
	// guard, not the interval.)
}

// Quotes that are wrong in a repeatable way keep most of their miscal
// as excess.
func TestExcessMiscalSurvivesTheNull(t *testing.T) {
	var rows []OpenRow
	for i := range 40 { // 30% quoted, resolves yes 7 times in 10
		result := "no"
		if i%10 < 7 {
			result = "yes"
		}
		rows = append(rows, quoted("OFF", "OFF-E"+strconv.Itoa(i), "OFF-"+strconv.Itoa(i), repOpen, 3000, 100, result))
	}
	r := Report(rows, calOpts)[0]
	if !near(r.MiscalBps, 4000, 1e-6) {
		t.Fatalf("miscal %v, want 4000", r.MiscalBps)
	}
	// With every mid at 30% the null is E|30% - k/40| for k ~ Bin(40, 0.3): about 580 bps.
	if r.NullBps < 400 || r.NullBps > 760 {
		t.Errorf("null %.0f bps, want about 580", r.NullBps)
	}
	if !near(r.ExcessBps, r.MiscalBps-r.NullBps, 1e-9) || r.ExcessBps < 3000 {
		t.Errorf("excess %.0f, want miscal - null and over 3000", r.ExcessBps)
	}
	if r.LoBps < 2500 || r.HiBps > 5500 || r.LoBps >= r.MiscalBps || r.HiBps <= r.MiscalBps {
		t.Errorf("interval [%.0f, %.0f], want it to bracket 4000", r.LoBps, r.HiBps)
	}
}

// The bootstrap resamples events, not rungs: with one event there is
// nothing to resample, however many rungs it has.
func TestBootstrapResamplesEvents(t *testing.T) {
	var rows []OpenRow
	for i := range 30 {
		result := "no"
		if i < 10 {
			result = "yes"
		}
		r := quoted("ONE", "ONE-E", "ONE-"+strconv.Itoa(i), repOpen, 300*(i+1), 100, result)
		r.StrikeType, r.Floor = "greater", strconv.Itoa(100-i)
		rows = append(rows, r)
	}
	r := Report(rows, calOpts)[0]
	if r.Settled != 30 || r.CalEvents != 1 {
		t.Fatalf("settled %d events %d", r.Settled, r.CalEvents)
	}
	if r.LoBps != r.MiscalBps || r.HiBps != r.MiscalBps {
		t.Errorf("one event: interval [%v, %v] around %v, want it collapsed (resampling rungs would spread it)", r.LoBps, r.HiBps, r.MiscalBps)
	}
}

// One draw per event decides every rung, consistently: thresholds
// resolve as a nested set, buckets one at a time, and each rung still
// resolves yes with exactly the probability its mid states.
func TestArcsPreserveEventStructure(t *testing.T) {
	mk := func(name, kind, floor, capStrike string, midBps int) OpenRow {
		r := quoted("W", "W-E", name, repOpen, midBps, 100, "no")
		r.StrikeType, r.Floor, r.Cap = kind, floor, capStrike
		return r
	}
	// A threshold ladder: P(above k) falls as k rises.
	ladder := arcs([]OpenRow{
		mk("T50", "greater", "50", "", 9000), mk("T52", "greater", "52", "", 6000),
		mk("T54", "greater", "54", "", 2500), mk("T56", "greater", "56", "", 500),
	})
	for i := 0; i < 1000; i++ {
		u := (float64(i) + 0.5) / 1000
		// Sorted from the highest strike down: once a rung is yes,
		// every lower strike must be too.
		seenYes := false
		for _, g := range ladder {
			if seenYes && !g.in(u) {
				t.Fatalf("u=%.4f: a higher strike resolved yes and a lower one no", u)
			}
			seenYes = seenYes || g.in(u)
		}
	}

	// A weather event: tails and buckets, mids summing to exactly 1.
	weather := arcs([]OpenRow{
		mk("T50", "less", "", "50", 1000),
		mk("B50", "between", "50", "51", 2000), mk("B52", "between", "52", "53", 4000), mk("B54", "between", "54", "55", 2500),
		mk("T55", "greater", "55", "", 500),
	})
	counts := make([]int, len(weather))
	const n = 20000
	for i := 0; i < n; i++ {
		u := (float64(i) + 0.5) / n
		yes := 0
		for j, g := range weather {
			if g.in(u) {
				yes++
				counts[j]++
			}
		}
		if yes != 1 {
			t.Fatalf("u=%.5f: %d rungs resolved yes, want exactly one", u, yes)
		}
	}
	for j, g := range weather {
		if got := float64(counts[j]) / n; math.Abs(got-g.p) > 1e-3 {
			t.Errorf("rung %d resolved yes %.4f of the time, its mid says %.4f", j, got, g.p)
		}
	}

	// Mids summing past 1 wrap around: marginals stay exact.
	over := arcs([]OpenRow{mk("A", "custom", "", "", 6000), mk("B", "custom", "", "", 7000)})
	for j, g := range over {
		c := 0
		for i := 0; i < n; i++ {
			if g.in((float64(i) + 0.5) / n) {
				c++
			}
		}
		if got := float64(c) / n; math.Abs(got-g.p) > 1e-3 {
			t.Errorf("overround rung %d: yes %.4f of the time, mid %.4f", j, got, g.p)
		}
	}
}

// The same rows and seed give the same numbers, whatever other series
// are in the report; another seed gives different ones.
func TestReportIsReproducible(t *testing.T) {
	rng := rand.New(rand.NewPCG(3, 4))
	a, b := calibratedRows("A", 40, rng), calibratedRows("B", 25, rng)
	find := func(reps []SeriesReport, s string) SeriesReport {
		for _, r := range reps {
			if r.Series == s {
				return r
			}
		}
		t.Fatalf("no series %s", s)
		return SeriesReport{}
	}
	both := Report(append(append([]OpenRow{}, a...), b...), calOpts)
	alone := Report(a, calOpts)
	again := Report(append(append([]OpenRow{}, b...), a...), calOpts)
	x, y, z := find(both, "A"), find(alone, "A"), find(again, "A")
	if x.NullBps != y.NullBps || x.LoBps != y.LoBps || x.HiBps != y.HiBps || x.NullBps != z.NullBps || x.HiBps != z.HiBps {
		t.Errorf("series A differs between runs: null %v/%v/%v, hi %v/%v/%v", x.NullBps, y.NullBps, z.NullBps, x.HiBps, y.HiBps, z.HiBps)
	}
	other := calOpts
	other.Seed = 8
	if w := find(Report(a, other), "A"); w.NullBps == x.NullBps {
		t.Errorf("a different seed gave the same null miscal %v", w.NullBps)
	}
}

// Calibration read off candle closes is flagged as a lower bound, in
// the struct, the text and the CSV.
func TestCandleCalibrationIsFlagged(t *testing.T) {
	rng := rand.New(rand.NewPCG(5, 6))
	candle := calibratedRows("CANDLE", 20, rng)
	capture := calibratedRows("CAPT", 20, rng)
	for i := range capture {
		capture[i].Source = SourceCapture
	}
	mixed := calibratedRows("MIXED", 20, rng)
	mixed[0].Source = SourceCapture

	reps := Report(append(append(candle, capture...), mixed...), calOpts)
	flags := map[string]SeriesReport{}
	for _, r := range reps {
		flags[r.Series] = r
	}
	if r := flags["CANDLE"]; !r.MiscalLowerBound || r.CandleCal != 20 {
		t.Errorf("CANDLE: lower bound %v, %d candle rows", r.MiscalLowerBound, r.CandleCal)
	}
	if r := flags["CAPT"]; r.MiscalLowerBound || r.CandleCal != 0 {
		t.Errorf("CAPT: lower bound %v, %d candle rows", r.MiscalLowerBound, r.CandleCal)
	}
	if r := flags["MIXED"]; !r.MiscalLowerBound || r.CandleCal != 19 {
		t.Errorf("MIXED: lower bound %v, %d candle rows", r.MiscalLowerBound, r.CandleCal)
	}

	var text bytes.Buffer
	if err := WriteText(&text, reps, true); err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(text.String(), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		switch fields[1] {
		case "CANDLE", "MIXED":
			if strings.Count(line, "≥") != 2 {
				t.Errorf("row %s should mark miscal and excess with ≥: %s", fields[1], line)
			}
		case "CAPT":
			if strings.Contains(line, "≥") {
				t.Errorf("row CAPT is marked as a lower bound: %s", line)
			}
		}
	}
	if !strings.Contains(text.String(), "20 from candle closes, so gaps are lower bounds") ||
		!strings.Contains(text.String(), "CAPT: opening mid vs realized frequency (20 settled opens in 20 events)") {
		t.Errorf("decile headings:\n%s", text.String())
	}
}
