package analysis

import (
	"strings"
	"testing"
)

func TestKalshiFee(t *testing.T) {
	tests := []struct {
		name      string
		sizeCenti int64
		price     int
		want      int64 // micro-dollars
	}{
		// 0.07 x 100 x 0.5 x 0.5 = $1.75 exactly.
		{"100 at 50c", 10000, 5000, 1_750_000},
		// 0.07 x 1 x 0.5 x 0.5 = $0.0175 -> rounds up to $0.02.
		{"1 at 50c", 100, 5000, 20_000},
		// 0.07 x 1 x 0.21 x 0.79 = $0.011613 -> $0.02.
		{"1 at 21c", 100, 2100, 20_000},
		// 0.07 x 10 x 0.21 x 0.79 = $0.11613 -> $0.12.
		{"10 at 21c", 1000, 2100, 120_000},
		// Symmetric: 79c costs the same as 21c.
		{"10 at 79c", 1000, 7900, 120_000},
		// 0.07 x 1000 x 0.01 x 0.99 = $0.693 -> $0.70.
		{"1000 at 1c", 100000, 100, 700_000},
		{"at 0", 10000, 0, 0},
		{"at 100", 10000, 10000, 0},
		// Fractional contracts (sizes are stored in hundredths):
		// 0.07 x 0.5 x 0.5 x 0.5 = $0.00875 -> $0.01.
		{"0.5 at 50c", 50, 5000, 10_000},
	}
	for _, tt := range tests {
		if got := KalshiFee(70000, tt.sizeCenti, tt.price); got != tt.want {
			t.Errorf("%s: KalshiFee = %d, want %d", tt.name, got, tt.want)
		}
	}
	// No overflow at absurd sizes: 1e9 contracts at 50c = $17.5M.
	if got, want := KalshiFee(70000, 100_000_000_000, 5000), int64(17_500_000_000_000); got != want {
		t.Errorf("huge: KalshiFee = %d, want %d", got, want)
	}
}

func TestPolymarketFee(t *testing.T) {
	tests := []struct {
		name      string
		rate      int64
		sizeCenti int64
		price     int
		want      int64
	}{
		{"rate 0", 0, 10000, 5000, 0},
		// 100 x 0.05 x 0.5 x 0.5 = 1.25 USDC.
		{"100 at 50c", 50000, 10000, 5000, 1_250_000},
		// 1 x 0.05 x 0.19 x 0.81 = 0.007695 -> 0.0077 (5 decimals, half up).
		{"1 at 19c", 50000, 100, 1900, 7_700},
		// 0.01 x 0.05 x 0.01 x 0.99 = 0.000004950 -> 0.00000, raised to the 0.00001 minimum.
		{"tiny", 50000, 1, 100, 10},
		// Half up: 0.07 x 0.01 x 0.5 x 0.5 = 0.000175 -> 0.00018.
		{"half up", 70000, 1, 5000, 180},
	}
	for _, tt := range tests {
		if got := PolymarketFee(tt.rate, tt.sizeCenti, tt.price); got != tt.want {
			t.Errorf("%s: PolymarketFee = %d, want %d", tt.name, got, tt.want)
		}
	}
}

func TestParseFees(t *testing.T) {
	if _, err := LoadFees("../../fees.yaml"); err != nil {
		t.Fatalf("repo fees.yaml: %v", err)
	}
	venue := func(name, extra string) string {
		return name + ":\n  venue: V\n  taker_rate_ppm: 70000\n  checked: 2026-10-22\n  sources: [https://x]\n" + extra
	}
	tests := []struct{ name, yaml, wantErr string }{
		{"empty", "", "empty"},
		{"unknown key", venue("kalshi", "  taker_rate: 0.07\n") + venue("polymarket", ""), "taker_rate"},
		{"no source", venue("kalshi", "") + "polymarket:\n  venue: V\n  checked: x\n", "source"},
		{"no checked", venue("kalshi", "") + "polymarket:\n  venue: V\n  sources: [a]\n", "checked"},
		{"rate too big", venue("kalshi", "") + venue("polymarket", "  stress_taker_rate_ppm: 2000000\n"), "0-1000000"},
		{"kalshi stress", venue("kalshi", "  stress_taker_rate_ppm: 1\n") + venue("polymarket", ""), "only supported"},
	}
	for _, tt := range tests {
		_, err := ParseFees([]byte(tt.yaml))
		if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
			t.Errorf("%s: err = %v, want one containing %q", tt.name, err, tt.wantErr)
		}
	}
}
