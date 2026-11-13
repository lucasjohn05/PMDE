package opens

import (
	"fmt"
	"strconv"
	"testing"
)

// capRow is a captured open with a quote of bid/ask and size contracts
// a side, day days after repOpen.
func capRow(series string, i, day, bid, ask int, size int64) OpenRow {
	r := quoted(series, series+"-E"+strconv.Itoa(day), series+"-"+strconv.Itoa(i), repOpen+int64(day)*dayMs, (bid+ask)/2, (ask-bid)/2, "")
	r.Source, r.CaptureSeen = SourceCapture, true
	r.BidSz, r.AskSz = int64p(size*100), int64p(size*100)
	return r
}

func TestCapacity(t *testing.T) {
	rows := []OpenRow{
		// Day 0: a 40/50c rung, 100 a side (ntm), and a 2/6c rung, 50 a
		// side (wing).
		capRow("S", 0, 0, 4000, 5000, 100),
		capRow("S", 1, 0, 200, 600, 50),
		// Day 1: one ntm rung, 20 a side, and one captured rung whose
		// quote was too small: it counts the day, adds nothing.
		capRow("S", 2, 1, 4000, 5000, 20),
		capRow("S", 3, 1, 4000, 5000, 5),
		// Day 2: captured, nothing qualified.
		capRow("S", 4, 2, 100, 9900, 1000),
		// A candle-sourced open adds no day.
		quoted("S", "S-E9", "S-9", repOpen+9*dayMs, 4500, 300, ""),
	}
	r := Report(rows, Options{Seed: 1, Resamples: 10})[0]
	// Contracts per day: 300 (200 ntm + 100 wing), 40 (ntm), 0.
	if r.CapDays != 3 || r.Contracts != 40 || r.NTMContracts != 40 || r.WingContracts != 0 {
		t.Errorf("contracts: %+v", r.Capacity)
	}
	// Dollars at risk on day 0: ntm 0.40x100 + 0.50x100 = $90; wing
	// 0.02x50 + 0.94x50 = $48. Day 1: 0.40x20 + 0.50x20 = $18.
	day0 := capacity(rows[:2])
	if day0.Contracts != 300 || day0.NTMContracts != 200 || day0.WingContracts != 100 ||
		!near(day0.Dollars, 138, 1e-9) || !near(day0.NTMDollars, 90, 1e-9) || !near(day0.WingDollars, 48, 1e-9) {
		t.Errorf("day 0: %+v", day0)
	}
	if !near(r.Dollars, 18, 1e-9) || !near(r.NTMDollars, 18, 1e-9) || r.WingDollars != 0 {
		t.Errorf("median dollars: %+v", r.Capacity)
	}
}

// Series without a score rank by capacity.
func TestRankByCapacityWithoutScore(t *testing.T) {
	var rows []OpenRow
	rows = append(rows, capRow("SMALL", 0, 0, 4000, 5000, 10))
	rows = append(rows, capRow("BIG", 1, 0, 4000, 5000, 500))
	rows = append(rows, quoted("NONE", "NONE-E", "NONE-0", repOpen, 4500, 300, ""))
	reps := Report(rows, Options{Seed: 1, Resamples: 10})
	var got []string
	for _, r := range reps {
		if r.HasScore {
			t.Errorf("%s has a score", r.Series)
		}
		got = append(got, r.Series)
	}
	if want := "[BIG SMALL NONE]"; fmt.Sprint(got) != want {
		t.Errorf("order %v, want %s", got, want)
	}
}
