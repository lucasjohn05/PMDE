package opens

import (
	"math/rand/v2"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/lucasjohn05/pmde/internal/metrics"
	"github.com/lucasjohn05/pmde/internal/storage"
)

var bmNow = time.Date(2026, 12, 15, 12, 0, 0, 0, time.UTC)

// bmRows is a series quoting every day of the 30 before bmNow: perDay
// single-rung events, captured, each quoted at mid with size(i)
// contracts a side 400bps wide, resolving yes when yes(i).
func bmRows(series string, perDay, mid int, size func(i int) int64, yes func(i int) bool) []OpenRow {
	var rows []OpenRow
	i := 0
	for d := 1; d <= 30; d++ {
		for k := range perDay {
			open := bmNow.Add(-time.Duration(d)*24*time.Hour + time.Duration(k)*time.Hour).UnixMilli()
			result := "no"
			if yes(i) {
				result = "yes"
			}
			r := quoted(series, series+"-E"+strconv.Itoa(i), series+"-"+strconv.Itoa(i), open, mid, 200, result)
			r.Source, r.CaptureSeen = SourceCapture, true
			r.BidSz, r.AskSz = int64p(size(i)*100), int64p(size(i)*100)
			rows = append(rows, r)
			i++
		}
	}
	return rows
}

func bmOpts() Options {
	return Options{Seed: 3, Resamples: 400, Rule: DefaultQuoteRule(), Fees: &Fees{TakerRatePPM: 70000, Series: map[string]SeriesFee{
		"BAD": {"quadratic", 1_000_000}, "FAIR": {"quadratic", 1_000_000}, "MIXED": {"quadratic", 1_000_000}, "SMALL": {"quadratic", 1_000_000},
	}}}
}

func TestEvaluateBadMakers(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	// BAD quotes 30c for what happens 7 times in 10, 150 a side, one
	// quote: a candidate.
	rows := bmRows("BAD", 2, 3000, func(int) int64 { return 150 }, func(i int) bool { return i%10 < 7 })
	// FAIR is the same maker but calibrated: no edge.
	rows = append(rows, bmRows("FAIR", 2, 3000, func(int) int64 { return 150 }, func(int) bool { return rng.Float64() < 0.3 })...)
	// MIXED is wrong, but its sizes vary: not one script.
	rows = append(rows, bmRows("MIXED", 2, 3000, func(i int) int64 { return 150 + int64(i%3)*10 }, func(i int) bool { return i%10 < 7 })...)
	// SMALL is wrong and scripted but small: 2 rungs x 15 x 2 sides = 60 a day.
	rows = append(rows, bmRows("SMALL", 2, 3000, func(int) int64 { return 15 }, func(i int) bool { return i%10 < 7 })...)
	// A row older than the window does not count.
	old := bmRows("BAD", 1, 3000, func(int) int64 { return 150 }, func(int) bool { return true })[0]
	old.Market, old.OpenMs = "BAD-OLD", bmNow.Add(-40*24*time.Hour).UnixMilli()
	rows = append(rows, old)

	rule := DefaultBadMakerRule()
	b := EvaluateBadMakers(rows, bmNow, rule, bmOpts(), map[string]string{"BAD": "Bad maker"}, BadMakers{})
	by := map[string]BadMakerEval{}
	for _, e := range b.Series {
		by[e.Series] = e
	}
	bad := by["BAD"]
	if !bad.Candidate || bad.Title != "Bad maker" || bad.Opens != 60 || bad.QuotedShare != 1 || bad.MedianCapacity != 600 ||
		bad.ComboShare != 1 || bad.ComboBidSz != 15000 || bad.ComboSpreadBps != 400 || bad.SettledEvents != 60 ||
		!bad.EdgeOK || bad.NetEdgeLoBps <= 0 || bad.NotifiedMs != bmNow.UnixMilli() ||
		bad.AlertUntilMs != bmNow.Add(6*24*time.Hour).UnixMilli() {
		t.Errorf("BAD: %+v", bad)
	}
	if e := by["FAIR"]; e.Candidate || e.EdgeOK || !e.QuotedOK || !e.CapacityOK || !e.OneMakerOK || !e.EventsOK {
		t.Errorf("FAIR: %+v", e)
	}
	if e := by["MIXED"]; e.Candidate || e.OneMakerOK || !e.EdgeOK {
		t.Errorf("MIXED: %+v", e)
	}
	if e := by["SMALL"]; e.Candidate || e.CapacityOK || e.MedianCapacity != 60 {
		t.Errorf("SMALL: %+v", e)
	}
	if b.Series[0].Series != "BAD" || len(b.Candidates()) != 1 {
		t.Errorf("candidates first: %v", b.Series[0].Series)
	}

	// The next day it is still a candidate but not notified again; a
	// week on, it is.
	next := EvaluateBadMakers(rows, bmNow.Add(24*time.Hour), rule, bmOpts(), nil, b)
	if e := next.Candidates(); len(e) != 1 || e[0].NotifiedMs != bmNow.UnixMilli() || e[0].AlertUntilMs != bmNow.Add(6*24*time.Hour).UnixMilli() {
		t.Errorf("a day later: %+v", e)
	}
	week := EvaluateBadMakers(rows, bmNow.Add(7*24*time.Hour), rule, bmOpts(), nil, next)
	if e := week.Candidates(); len(e) != 1 || e[0].NotifiedMs != bmNow.Add(7*24*time.Hour).UnixMilli() {
		t.Errorf("a week later: %+v", e)
	}

	// Every threshold is the rule's: with 61 events needed, none.
	strict := rule
	strict.MinEvents = 61
	if c := EvaluateBadMakers(rows, bmNow, strict, bmOpts(), nil, BadMakers{}).Candidates(); len(c) != 0 {
		t.Errorf("61 events: %v", c)
	}
}

// Wing rungs that settle against their opening price more often than
// it says are flagged; calibrated ones are not.
func TestWingCalibration(t *testing.T) {
	rng := rand.New(rand.NewPCG(5, 6))
	var fair, off []OpenRow
	for i := range 200 {
		// 5c rungs: fair ones resolve yes 5% of the time, off ones 20%.
		r := quoted("W", "W-E"+strconv.Itoa(i), "W-"+strconv.Itoa(i), repOpen, 500, 100, "no")
		f := r
		if rng.Float64() < 0.05 {
			f.Result = "yes"
		}
		if i%5 == 0 {
			r.Result = "yes"
		}
		fair, off = append(fair, f), append(off, r)
	}
	w := wingCalibrate("W", fair, 1, 2000, 0.9)
	if w.LowN != 200 || w.Events != 200 || w.HighN != 0 || !near(w.Expected, 10, 1e-9) || float64(w.Against) > w.Hi || w.Lo > 10 || w.Hi < 10 {
		t.Errorf("fair wings: %+v", w)
	}
	w = wingCalibrate("W", off, 1, 2000, 0.9)
	if w.Against != 40 || float64(w.Against) <= w.Hi || !near(w.LowYes, 0.2, 1e-9) || !near(w.LowMeanBps, 500, 1e-9) {
		t.Errorf("off wings: %+v", w)
	}
	// A 95c rung settling no is against it too.
	hi := quoted("W", "W-H", "W-H", repOpen, 9500, 100, "no")
	if w := wingCalibrate("W", []OpenRow{hi}, 1, 100, 0.9); w.HighN != 1 || w.Against != 1 || !near(w.Expected, 0.05, 1e-9) {
		t.Errorf("high wing: %+v", w)
	}
}

func TestBadMakersFileAndMetrics(t *testing.T) {
	dir := t.TempDir()
	rule := DefaultBadMakerRule()
	b := BadMakers{EvaluatedMs: bmNow.UnixMilli(), Rule: rule, Quote: DefaultQuoteRule(), Series: []BadMakerEval{TestCandidate(bmNow, rule), {Series: "OK"}}}
	if err := SaveBadMakers(dir, b); err != nil {
		t.Fatal(err)
	}
	got, err := LoadBadMakers(dir)
	if err != nil || len(got.Series) != 2 || got.Series[0].Series != "TEST-BAD-MAKER" || !got.Series[0].Test {
		t.Fatalf("load %+v, %v", got, err)
	}
	flags, err := storage.ReadJSONL[BadMakerEval](filepath.Join(dir, badFlagsFile))
	if err != nil || len(flags) != 1 || flags[0].Series != "TEST-BAD-MAKER" {
		t.Errorf("flags %+v, %v", flags, err)
	}
	// The test candidate is not carried into the next evaluation.
	if next := EvaluateBadMakers(nil, bmNow, rule, bmOpts(), nil, got); len(next.Series) != 0 {
		t.Errorf("test candidate carried over: %+v", next.Series)
	}

	reg := prometheus.NewRegistry()
	m := metrics.NewOpens(reg)
	m.SetBadMakers(got.Metrics())
	mfs, _ := reg.Gather()
	n := map[string]int{}
	for _, mf := range mfs {
		n[mf.GetName()] = len(mf.GetMetric())
		if mf.GetName() == "opens_bad_maker_alert_until_timestamp_seconds" {
			if v := mf.GetMetric()[0].GetGauge().GetValue(); v != float64(bmNow.Add(6*24*time.Hour).Unix()) {
				t.Errorf("until %v", v)
			}
		}
	}
	if n["opens_bad_maker_alert_until_timestamp_seconds"] != 1 || n["opens_bad_maker_stat"] != 15 {
		t.Errorf("metric counts %v", n)
	}
	m.SetBadMakers(nil)
	mfs, _ = reg.Gather()
	for _, mf := range mfs {
		if mf.GetName() == "opens_bad_maker_stat" && len(mf.GetMetric()) > 0 {
			t.Errorf("stats left after clearing")
		}
	}
}

func TestParseConfigBadMaker(t *testing.T) {
	c, err := ParseConfig([]byte("series: [A]\nbad_maker:\n  window_days: 14\n  min_quoted_share: 0.5\n  min_median_capacity: 50\n  min_events: 10\n  notify_every_days: 3\n"))
	if err != nil {
		t.Fatal(err)
	}
	r := c.BadMaker
	if r.WindowDays != 14 || r.MinQuotedShare != 0.5 || r.MinMedianCapacity != 50 || r.MinEvents != 10 || r.NotifyEveryDays != 3 ||
		r.MinModalShare != 0.7 || r.WingInterval != 0.9 {
		t.Errorf("rule %+v", r)
	}
	for _, bad := range []string{
		"series: [A]\nbad_maker:\n  min_quoted_share: 1.5\n",
		"series: [A]\nbad_maker:\n  notify_every_days: 1\n",
		"series: [A]\nbad_maker:\n  window_days: 0\n",
		"series: [A]\nbad_maker:\n  wing_interval: 1\n",
		"series: [A]\nbad_maker:\n  events: 3\n",
	} {
		if _, err := ParseConfig([]byte(bad)); err == nil {
			t.Errorf("no error for %q", bad)
		}
	}
}
