package opens

import (
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"

	"github.com/lucasjohn05/pmde/internal/analysis"
	"github.com/lucasjohn05/pmde/internal/kalshi"
	"github.com/lucasjohn05/pmde/internal/storage"
)

// Fees is what the report needs to price a taker order on any series:
// Kalshi's general taker rate, and each series' own fee type and
// multiplier.
//
// Nothing about the schedule is written into this package. The rate
// comes from fees.yaml (the same file pmde-analyze uses), which records
// where it was read and when; the per-series fee_type and
// fee_multiplier come from Kalshi's series list, as saved in the series
// snapshot; and the formula and its rounding are analysis.KalshiFee,
// which fees.yaml describes: roundup(rate x C x P x (1-P)) to the next
// cent, per order.
type Fees struct {
	// TakerRatePPM is the general taker rate in parts per million
	// (fees.yaml's kalshi.taker_rate_ppm).
	TakerRatePPM int64

	// Series is each series' fee, by ticker.
	Series map[string]SeriesFee

	// Checked and SnapshotDay say how old the two inputs are: the date
	// fees.yaml's rate was verified, and the day of the series snapshot.
	Checked     string
	SnapshotDay string
}

// SeriesFee is a series' fee_type and fee_multiplier from Kalshi's
// series list.
type SeriesFee struct {
	// Type is quadratic, quadratic_with_maker_fees,
	// quadratic_with_combo_maker_fees or flat. The first three share one
	// taker fee (they differ in what makers pay); flat is a different
	// table of the fee schedule, which this code does not have.
	Type string

	// MultiplierPPM is fee_multiplier in parts per million: Kalshi
	// documents it as "a floating point multiplier applied to the fee
	// calculations". 1 for most series; 0.5 and 0 exist.
	MultiplierPPM int64
}

// feeDefaultSize is the order size, in hundredths of a contract, the
// fee is worked out at when a series has no captured opening sizes: 100
// contracts. The size matters because the fee is rounded up to the next
// cent per order, which is a large share of a small order's fee.
const feeDefaultSize = 100 * 100

// LoadFees reads the taker rate from the fees file and the series' fee
// fields from the latest series snapshot under dir (<data-dir>/opens).
// A snapshot written before the fee fields were kept, or no snapshot at
// all, is not an error: the series then have no fee, and the report
// leaves their net figures blank.
func LoadFees(feesPath, dir string) (*Fees, error) {
	af, err := analysis.LoadFees(feesPath)
	if err != nil {
		return nil, err
	}
	f := &Fees{TakerRatePPM: af.Kalshi.TakerRatePPM, Checked: af.Kalshi.Checked, Series: map[string]SeriesFee{}}
	path, day, err := LatestSnapshot(dir)
	if err != nil {
		return nil, err
	}
	if path == "" {
		return f, nil
	}
	snap, err := storage.ReadJSONL[kalshi.Series](path)
	if err != nil {
		return nil, fmt.Errorf("opens: %w", err)
	}
	f.SnapshotDay = day
	for _, s := range snap {
		if s.FeeType == "" {
			continue
		}
		mult, err := strconv.ParseFloat(s.FeeMultiplier.String(), 64)
		if err != nil || mult < 0 {
			continue
		}
		f.Series[s.Ticker] = SeriesFee{Type: s.FeeType, MultiplierPPM: int64(math.Round(mult * 1e6))}
	}
	return f, nil
}

// LatestSnapshot finds the newest series snapshot under dir. path is ""
// if there is none.
func LatestSnapshot(dir string) (path, day string, err error) {
	names, err := filepath.Glob(filepath.Join(dir, seriesDir, "*.jsonl"))
	if err != nil {
		return "", "", fmt.Errorf("opens: %w", err)
	}
	if len(names) == 0 {
		return "", "", nil
	}
	slices.Sort(names) // YYYY-MM-DD sorts by date
	path = names[len(names)-1]
	return path, filepath.Base(path)[:len(filepath.Base(path))-len(".jsonl")], nil
}

// TakerFeeBps is the taker fee on one order of sizeCenti hundredths of
// a contract at priceBps on series, per contract, in basis points of
// $1. ok is false if the fee is not known: the series is not in the
// snapshot, or its fee type is one this code has no formula for.
//
// It is the same whether the taker buys yes at P or no at 1-P.
func (f *Fees) TakerFeeBps(series string, sizeCenti int64, priceBps int) (bps float64, ok bool) {
	if f == nil || sizeCenti <= 0 || priceBps < 0 || priceBps > 10000 {
		return 0, false
	}
	sf, known := f.Series[series]
	if !known {
		return 0, false
	}
	switch sf.Type {
	case "quadratic", "quadratic_with_maker_fees", "quadratic_with_combo_maker_fees":
	default:
		return 0, false // "flat", or a type added since
	}
	ratePPM := f.TakerRatePPM * sf.MultiplierPPM / 1_000_000
	microDollars := analysis.KalshiFee(ratePPM, sizeCenti, priceBps)
	// Per contract, and 1 bp of $1 is 100 micro-dollars.
	return float64(microDollars) / (float64(sizeCenti) / 100) / 100, true
}

// errNoFees is what the report command says when asked for net figures
// with no fees file.
var errNoFees = errors.New("no fees file")

// FeesOrNil loads the fees for a report. An empty feesPath means the
// caller asked for no net figures: nil, nil. A missing file is an
// error, since a silently blank net column reads as "no edge".
func FeesOrNil(feesPath, dir string) (*Fees, error) {
	if feesPath == "" {
		return nil, nil
	}
	if _, err := os.Stat(feesPath); err != nil {
		return nil, fmt.Errorf("%w %s: pass -fees with the path to fees.yaml, or -fees \"\" to report without net figures", errNoFees, feesPath)
	}
	return LoadFees(feesPath, dir)
}
