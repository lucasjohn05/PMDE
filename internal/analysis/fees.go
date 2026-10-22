package analysis

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math/big"
	"os"

	"go.yaml.in/yaml/v3"
)

// Fees are the rates the executable gap is netted against, with where
// they came from. The formulas and rounding are KalshiFee and
// PolymarketFee; see fees.yaml.
type Fees struct {
	Kalshi     VenueFees `yaml:"kalshi" json:"kalshi"`
	Polymarket VenueFees `yaml:"polymarket" json:"polymarket"`
}

// VenueFees is one venue's entry in fees.yaml.
type VenueFees struct {
	Venue        string `yaml:"venue" json:"venue"`
	TakerRatePPM int64  `yaml:"taker_rate_ppm" json:"taker_rate_ppm"`
	// StressTakerRatePPM, if set, is a second rate the gap is also netted
	// against (Polymarket only).
	StressTakerRatePPM int64    `yaml:"stress_taker_rate_ppm,omitempty" json:"stress_taker_rate_ppm,omitempty"`
	Checked            string   `yaml:"checked" json:"checked"`
	Sources            []string `yaml:"sources" json:"sources"`
	Notes              string   `yaml:"notes" json:"notes,omitempty"`
}

// KalshiFormula and PolymarketFormula describe KalshiFee and
// PolymarketFee for the report.
const (
	KalshiFormula     = "roundup(rate x C x P x (1-P)) to the next cent, per order"
	PolymarketFormula = "C x rate x p x (1-p), rounded to 5 decimals (min 0.00001 if rate > 0), taker only"
)

// LoadFees reads and validates a fees file.
func LoadFees(path string) (Fees, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Fees{}, fmt.Errorf("reading fees: %w", err)
	}
	f, err := ParseFees(data)
	if err != nil {
		return Fees{}, fmt.Errorf("%s: %w", path, err)
	}
	return f, nil
}

// ParseFees parses a fees file. Unknown keys are errors, and every venue
// needs a source and a checked date: a rate nobody can trace back isn't
// worth reporting a net gap against.
func ParseFees(data []byte) (Fees, error) {
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	var f Fees
	if err := dec.Decode(&f); err != nil {
		if errors.Is(err, io.EOF) {
			return Fees{}, errors.New("empty fees file")
		}
		return Fees{}, fmt.Errorf("parsing fees: %w", err)
	}
	for _, v := range []struct {
		name string
		f    VenueFees
	}{{"kalshi", f.Kalshi}, {"polymarket", f.Polymarket}} {
		switch {
		case v.f.Venue == "":
			return Fees{}, fmt.Errorf("%s: venue is required", v.name)
		case v.f.TakerRatePPM < 0 || v.f.TakerRatePPM > 1_000_000 ||
			v.f.StressTakerRatePPM < 0 || v.f.StressTakerRatePPM > 1_000_000:
			return Fees{}, fmt.Errorf("%s: rates must be 0-1000000 ppm", v.name)
		case len(v.f.Sources) == 0:
			return Fees{}, fmt.Errorf("%s: at least one source is required", v.name)
		case v.f.Checked == "":
			return Fees{}, fmt.Errorf("%s: checked (date the rate was verified) is required", v.name)
		}
	}
	if f.Kalshi.StressTakerRatePPM != 0 {
		return Fees{}, errors.New("kalshi: stress_taker_rate_ppm is only supported for polymarket")
	}
	return f, nil
}

// Money is in micro-dollars (USD for Kalshi, USDC for Polymarket; the
// report treats them as equal). Sizes are in hundredths of a contract or
// share, as stored; prices in bps.
//
// feeNumerator is rate x C x P x (1-P) scaled so that one dollar is
// 1e16: ratePPM (1e6) x sizeCenti (1e2) x P bps x (10000-P) bps (1e8).
// Big ints, because the product can pass 2^63 for large sizes.
func feeNumerator(ratePPM, sizeCenti int64, priceBps int) *big.Int {
	n := big.NewInt(ratePPM)
	n.Mul(n, big.NewInt(sizeCenti))
	n.Mul(n, big.NewInt(int64(priceBps)))
	n.Mul(n, big.NewInt(int64(10000-priceBps)))
	return n
}

// KalshiFee is Kalshi's fee in micro-dollars for one order of sizeCenti
// hundredths of a contract at priceBps: rounded up to the next cent. The
// formula is symmetric in P and 1-P, so selling yes at P (buying no at
// 1-P) costs the same as buying yes at P.
func KalshiFee(ratePPM, sizeCenti int64, priceBps int) int64 {
	n := feeNumerator(ratePPM, sizeCenti, priceBps)
	const perCent = 1e14 // 1e16 per dollar
	q, r := new(big.Int).QuoRem(n, big.NewInt(perCent), new(big.Int))
	if r.Sign() > 0 {
		q.Add(q, big.NewInt(1))
	}
	return q.Int64() * 10_000 // cents to micro-dollars
}

// PolymarketFee is Polymarket's taker fee in micro-dollars for sizeCenti
// hundredths of a share at priceBps: rounded to 5 decimals (half up), and
// at least 0.00001 when the rate isn't 0.
func PolymarketFee(ratePPM, sizeCenti int64, priceBps int) int64 {
	n := feeNumerator(ratePPM, sizeCenti, priceBps)
	if n.Sign() == 0 {
		return 0
	}
	const perUnit = 1e11 // 1e16 per dollar, 1e5 units of 0.00001 per dollar
	q, r := new(big.Int).QuoRem(n, big.NewInt(perUnit), new(big.Int))
	if r.Cmp(big.NewInt(perUnit/2)) >= 0 {
		q.Add(q, big.NewInt(1))
	}
	units := max(q.Int64(), 1)
	return units * 10 // 0.00001 = 10 micro-dollars
}
