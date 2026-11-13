package opens

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lucasjohn05/pmde/internal/kalshi"
	"github.com/lucasjohn05/pmde/internal/storage"
)

func TestWeekly(t *testing.T) {
	data := t.TempDir()
	dir := filepath.Join(data, "opens")
	now := bmNow
	day := func(d int) int64 { return now.Add(-time.Duration(d) * 24 * time.Hour).UnixMilli() }

	// BIG and SMALL quote this week; WING has a settled 1c rung that
	// resolved yes, three weeks ago.
	var rows []OpenRow
	for d := 1; d <= 3; d++ {
		big := capRow("BIG", d, 0, 4000, 5000, 300)
		big.OpenMs = day(d)
		small := capRow("SMALL", d, 0, 4000, 5000, 20)
		small.OpenMs = day(d)
		rows = append(rows, big, small)
	}
	w := quoted("WING", "WING-E", "WING-1", day(21), 100, 50, "yes")
	rows = append(rows, w)
	if err := storage.WriteJSONL(filepath.Join(dir, backfillFile), rows); err != nil {
		t.Fatal(err)
	}
	storage.WriteJSONL(filepath.Join(dir, seriesDir, "2026-12-15.jsonl"), []kalshi.Series{{Ticker: "BIG", Title: "Big ladder"}})
	storage.WriteJSONL(filepath.Join(dir, newSeriesFile), []NewSeriesRecord{
		{T: day(20), Ticker: "OLDNEW", Title: "Before the week"},
		{T: day(2), Ticker: "KXQUOTED", Title: "Quoted one", Category: "Economics", Frequency: "daily"},
		{T: day(1), Ticker: "KXQUIET", Title: "Quiet one", Category: "Sports", Frequency: "one_off"},
	})
	storage.WriteJSONL(filepath.Join(dir, newQuotesFile), []NewSeriesQuote{
		{T: day(1), Series: "KXQUOTED", Market: "KXQUOTED-1", Bid: 3600, Ask: 4400, BidSz: 2500, AskSz: 2500},
	})
	storage.WriteJSONL(filepath.Join(dir, capturesFile), []CaptureRecord{
		{Market: "KXQUOTED-1", Series: "KXQUOTED", Event: "E", OpenMs: day(1), StartMs: 1, Complete: true},
		{Market: "KXQUOTED-2", Series: "KXQUOTED", Event: "E", OpenMs: day(1), StartMs: 1, Resyncs: 1},
		{Market: "OLD-1", Series: "OLD", Event: "O", OpenMs: day(9), StartMs: 2, Complete: true},
	})
	storage.WriteJSONL(filepath.Join(dir, skipsFile), []SkipRecord{
		{T: day(1), Series: "S", Event: "S-E", Markets: 6, Reason: "concurrency"},
		{T: day(10), Series: "S", Event: "S-F", Markets: 4, Reason: "late"},
	})
	if err := SaveBadMakers(dir, BadMakers{EvaluatedMs: now.UnixMilli(), Rule: DefaultBadMakerRule()}); err != nil {
		t.Fatal(err)
	}

	wk, err := LoadWeekly(data, dir, now, Options{Seed: 1, Resamples: 200})
	if err != nil {
		t.Fatal(err)
	}
	var b strings.Builder
	if err := WriteWeekly(&b, wk); err != nil {
		t.Fatal(err)
	}
	out := b.String()
	for _, want := range []string{
		"# Open scanner, week to 2026-12-15",
		"## Bad-maker candidates", "None.",
		// BIG ranks first: 300 a side on one rung a day = 600.
		"| 1 | BIG | Big ladder | 3 | 600 (600 / 0) |",
		"| 2 | SMALL |",
		"| KXQUOTED | Quoted one | Economics | daily |",
		"| 2 | yes, 12-14 12:00: 36c x 25 / 44c x 25, 800 bps |",
		"| KXQUIET | Quiet one | Sports | one_off |",
		"| 0 | no |",
		// The 1c rung that settled yes is against its price.
		"| WING | 1 (1) | 1.0c / 100.0% | 0 (0) | - | 1 / 0.0 **above** |",
		"| Captures (connections) | 1 |", "| Markets captured | 2 |", "| Captured incompletely | 1 |", "| Resyncs during captures | 1 |",
		"| Skipped opens: concurrency | 6 |", "| Skipped opens: late | 0 |",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("summary lacks %q", want)
		}
	}
	if strings.Contains(out, "OLDNEW") {
		t.Error("a series detected before the week is listed")
	}
	if t.Failed() {
		t.Log(out)
	}

	// A candidate is listed by name.
	SaveBadMakers(dir, BadMakers{EvaluatedMs: now.UnixMilli(), Rule: DefaultBadMakerRule(), Series: []BadMakerEval{TestCandidate(now, DefaultBadMakerRule())}})
	wk, _ = LoadWeekly(data, dir, now, Options{Seed: 1, Resamples: 200})
	b.Reset()
	WriteWeekly(&b, wk)
	if !strings.Contains(b.String(), "| TEST-BAD-MAKER (delivery test) |") {
		t.Errorf("candidate not listed:\n%s", b.String())
	}
}
