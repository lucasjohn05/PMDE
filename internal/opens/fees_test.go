package opens

import (
	"encoding/json"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/lucasjohn05/pmde/internal/kalshi"
	"github.com/lucasjohn05/pmde/internal/storage"
)

func testFees() *Fees {
	return &Fees{TakerRatePPM: 70000, Series: map[string]SeriesFee{
		"STD":   {Type: "quadratic", MultiplierPPM: 1_000_000},
		"MAKER": {Type: "quadratic_with_maker_fees", MultiplierPPM: 1_000_000},
		"HALF":  {Type: "quadratic", MultiplierPPM: 500_000},
		"FREE":  {Type: "quadratic", MultiplierPPM: 0},
		"FLAT":  {Type: "flat", MultiplierPPM: 1_000_000},
	}}
}

// The taker fee as a function of price and size, per contract in bps.
// Expected values are roundup(rate x C x P x (1-P)) to the next cent,
// worked by hand.
func TestTakerFeeBps(t *testing.T) {
	f := testFees()
	for _, c := range []struct {
		series    string
		contracts int64
		priceBps  int
		want      float64 // bps per contract
	}{
		{"STD", 100, 5000, 175},   // 0.07 x 100 x 0.25 = $1.75
		{"STD", 100, 1000, 63},    // 0.07 x 100 x 0.09 = $0.63
		{"STD", 100, 9000, 63},    // symmetric in P and 1-P
		{"STD", 250, 4400, 172.8}, // 0.07 x 250 x 0.2464 = $4.312 -> $4.32
		{"STD", 1, 5000, 200},     // $0.0175 -> 2c: the round-up dominates a 1-lot
		{"STD", 100, 100, 7},      // 0.07 x 100 x 0.0099 = $0.0693 -> 7c
		{"MAKER", 100, 5000, 175}, // the maker-fee types share the taker fee
		{"HALF", 100, 5000, 88},   // x0.5: $0.875 -> $0.88
		{"FREE", 100, 5000, 0},    // fee_multiplier 0
		{"STD", 100, 0, 0},
		{"STD", 100, 10000, 0},
	} {
		got, ok := f.TakerFeeBps(c.series, c.contracts*100, c.priceBps)
		if !ok || !near(got, c.want, 1e-9) {
			t.Errorf("%s, %d contracts at %d bps: fee %v bps/contract (ok %v), want %v", c.series, c.contracts, c.priceBps, got, ok, c.want)
		}
	}
	// No formula for these: say so instead of guessing.
	for _, series := range []string{"FLAT", "UNKNOWN"} {
		if _, ok := f.TakerFeeBps(series, 10000, 5000); ok {
			t.Errorf("%s: a fee was returned", series)
		}
	}
	if _, ok := (*Fees)(nil).TakerFeeBps("STD", 10000, 5000); ok {
		t.Errorf("nil Fees returned a fee")
	}
	if _, ok := f.TakerFeeBps("STD", 0, 5000); ok {
		t.Errorf("a fee for a zero-size order")
	}
}

// The rate comes from fees.yaml and the per-series fields from the
// newest series snapshot.
func TestLoadFees(t *testing.T) {
	dir := t.TempDir()
	snap := func(day string, series ...kalshi.Series) {
		if err := storage.WriteJSONL(filepath.Join(dir, "series", day+".jsonl"), series); err != nil {
			t.Fatal(err)
		}
	}
	// An old snapshot, from before the fee fields were kept.
	snap("2026-11-12", kalshi.Series{Ticker: "KXHIGHNY"})
	f, err := LoadFees("../../fees.yaml", dir)
	if err != nil {
		t.Fatal(err)
	}
	if f.TakerRatePPM != 70000 || f.Checked == "" || f.SnapshotDay != "2026-11-12" || len(f.Series) != 0 {
		t.Errorf("fees from an old snapshot: %+v", f)
	}
	snap("2026-11-13",
		kalshi.Series{Ticker: "KXHIGHNY", FeeType: "quadratic", FeeMultiplier: json.Number("1")},
		kalshi.Series{Ticker: "KXHALF", FeeType: "quadratic_with_maker_fees", FeeMultiplier: json.Number("0.5")},
		kalshi.Series{Ticker: "KXNOFEE"})
	if f, err = LoadFees("../../fees.yaml", dir); err != nil {
		t.Fatal(err)
	}
	if f.SnapshotDay != "2026-11-13" || len(f.Series) != 2 ||
		f.Series["KXHIGHNY"] != (SeriesFee{"quadratic", 1_000_000}) || f.Series["KXHALF"] != (SeriesFee{"quadratic_with_maker_fees", 500_000}) {
		t.Errorf("fees: %+v", f)
	}
	if bps, ok := f.TakerFeeBps("KXHALF", 10000, 5000); !ok || bps != 88 {
		t.Errorf("KXHALF fee %v %v", bps, ok)
	}

	// No snapshot at all is not an error; a missing fees file is.
	if f, err := LoadFees("../../fees.yaml", t.TempDir()); err != nil || len(f.Series) != 0 {
		t.Errorf("no snapshot: %+v, %v", f, err)
	}
	if _, err := FeesOrNil(filepath.Join(dir, "nope.yaml"), dir); err == nil {
		t.Errorf("no error for a missing fees file")
	}
	if f, err := FeesOrNil("", dir); f != nil || err != nil {
		t.Errorf("empty path: %v, %v", f, err)
	}
}

// offRows is a series quoting mid for things that happen 7 times in
// 10, with 250 contracts a side at half-spread half.
func offRows(series string, n, midBps, half int, sized bool) []OpenRow {
	var rows []OpenRow
	for i := range n {
		result := "no"
		if i%10 < 7 {
			result = "yes"
		}
		r := quoted(series, series+"-E"+strconv.Itoa(i), series+"-"+strconv.Itoa(i), repOpen, midBps, half, result)
		if sized {
			r.Source = SourceCapture
			r.BidSz, r.AskSz = int64p(25000), int64p(25000)
		}
		rows = append(rows, r)
	}
	return rows
}

func TestNetEdgeAndScore(t *testing.T) {
	opts := calOpts
	opts.Fees = testFees()

	// 30% quoted, 70% realized: miscal 4000 bps. 400 bps wide. 250 a
	// side, 40 opens in one day.
	r := Report(offRows("STD", 40, 3000, 200, true), opts)[0]
	// Fee at the median mid of 30c for 250 contracts:
	// 0.07 x 250 x 0.21 = $3.675 -> $3.68 -> 1.472c a contract.
	if !r.HasFee || r.FeeSizeCenti != 25000 || !near(r.FeeBps, 147.2, 1e-9) || r.MedianMidBps != 3000 {
		t.Fatalf("fee %v bps at size %d, median mid %v (has %v)", r.FeeBps, r.FeeSizeCenti, r.MedianMidBps, r.HasFee)
	}
	wantNet := r.ExcessBps - 200 - 147.2
	if !r.HasNet || !near(r.NetEdgeBps, wantNet, 1e-9) || r.NetEdgeBps < 2500 {
		t.Errorf("net edge %v, want excess %v - 200 - 147.2 = %v", r.NetEdgeBps, r.ExcessBps, wantNet)
	}
	if !r.HasScore || !near(r.Score, wantNet/10000*250*40, 1e-6) {
		t.Errorf("score %v, want %v", r.Score, wantNet/10000*250*40)
	}
	// The gross figure is the old score: raw miscal, nothing taken off.
	if !r.HasGross || !near(r.GrossScore, 0.4*250*40, 1e-6) || r.GrossScore <= r.Score {
		t.Errorf("gross %v, want %v and above the net score %v", r.GrossScore, 0.4*250*40, r.Score)
	}
	if r.MiscalLowerBound {
		t.Errorf("captured opens flagged as a lower bound")
	}

	// A spread wider than the edge: floored at zero, score zero, gross
	// unchanged.
	wide := Report(offRows("STD", 40, 3000, 4500, true), opts)[0]
	if !wide.HasNet || wide.NetEdgeBps != 0 || !wide.HasScore || wide.Score != 0 || !near(wide.GrossScore, 0.4*250*40, 1e-6) {
		t.Errorf("wide spread: net %v score %v gross %v", wide.NetEdgeBps, wide.Score, wide.GrossScore)
	}

	// No captured sizes: the fee is priced at 100 contracts and there is
	// a net edge, but no score of either kind.
	candle := Report(offRows("STD", 40, 3000, 200, false), opts)[0]
	if !candle.HasNet || candle.FeeSizeCenti != 10000 || !near(candle.FeeBps, 147, 1e-9) || candle.HasScore || candle.HasGross {
		t.Errorf("no sizes: fee %v at %d, net %v, score %v gross %v", candle.FeeBps, candle.FeeSizeCenti, candle.HasNet, candle.HasScore, candle.HasGross)
	}

	// A fee this code cannot price, and no fees at all: no net edge and
	// no score, but the gross figure is still there.
	for name, rows := range map[string][]OpenRow{"FLAT": offRows("FLAT", 40, 3000, 200, true), "UNKNOWN": offRows("UNKNOWN", 40, 3000, 200, true)} {
		if x := Report(rows, opts)[0]; x.HasFee || x.HasNet || x.HasScore || !x.HasGross {
			t.Errorf("%s: fee %v net %v score %v gross %v", name, x.HasFee, x.HasNet, x.HasScore, x.HasGross)
		}
	}
	if x := Report(offRows("STD", 40, 3000, 200, true), calOpts)[0]; x.HasFee || x.HasNet || x.HasScore || !x.HasGross {
		t.Errorf("no Fees: fee %v net %v score %v gross %v", x.HasFee, x.HasNet, x.HasScore, x.HasGross)
	}

	// Ranking is by the net score: the series with the bigger gross
	// figure but a spread that eats the edge ranks below.
	rows := append(offRows("TIGHT", 20, 3000, 200, true), offRows("WIDE", 40, 3000, 4500, true)...)
	o := opts
	o.Fees = &Fees{TakerRatePPM: 70000, Series: map[string]SeriesFee{"TIGHT": {"quadratic", 1_000_000}, "WIDE": {"quadratic", 1_000_000}}}
	reps := Report(rows, o)
	if reps[0].Series != "TIGHT" || reps[1].GrossScore <= reps[0].GrossScore {
		t.Errorf("ranking: %s (score %v, gross %v) then %s (score %v, gross %v)",
			reps[0].Series, reps[0].Score, reps[0].GrossScore, reps[1].Series, reps[1].Score, reps[1].GrossScore)
	}
}
