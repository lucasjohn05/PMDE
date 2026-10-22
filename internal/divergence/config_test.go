package divergence

import (
	"strings"
	"testing"
	"time"
)

const assetA = "111061902544814266207267295505639408607400625795891618462682726460921782993748"
const assetB = "55159722761418013044126414276680602270318000841690689684819994448621694923050"

func TestParsePairs(t *testing.T) {
	got, err := ParsePairs([]byte(`
# comment
pairs:
  - name: hold
    kalshi: KX-H0
    polymarket: "` + assetA + `"
  - name: hike
    kalshi: KX-H25 # trailing comment
    polymarket: ` + assetB + ` # unquoted keeps every digit
    notes: |
      Rules differ on rounding.
      Second line.
    kalshi_last_trade: 2026-10-28T13:55:00-04:00
  - name: hold-again-other-asset
    kalshi: KX-H0
    polymarket: "` + assetB + `"
`))
	if err != nil {
		t.Fatal(err)
	}
	want := []Pair{
		{Name: "hold", Kalshi: "KX-H0", Polymarket: assetA},
		{Name: "hike", Kalshi: "KX-H25", Polymarket: assetB, Notes: "Rules differ on rounding.\nSecond line.\n", KalshiLastTrade: "2026-10-28T13:55:00-04:00"},
		{Name: "hold-again-other-asset", Kalshi: "KX-H0", Polymarket: assetB},
	}
	if len(got) != len(want) {
		t.Fatalf("got %d pairs, want %d: %+v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("pair %d = %+v, want %+v", i, got[i], want[i])
		}
	}
	if _, ok := got[0].LastTrade(); ok {
		t.Error("pair without kalshi_last_trade has a LastTrade")
	}
	lt, ok := got[1].LastTrade()
	if want := time.Date(2026, 10, 28, 17, 55, 0, 0, time.UTC); !ok || !lt.Equal(want) {
		t.Errorf("LastTrade = %v, %v; want %v", lt, ok, want)
	}
}

func TestParsePairsErrors(t *testing.T) {
	pair := func(name, kalshi, poly string) string {
		return "  - name: " + name + "\n    kalshi: " + kalshi + "\n    polymarket: \"" + poly + "\"\n"
	}
	tests := []struct {
		name, yaml, wantErr string
	}{
		{"empty file", "", "no pairs"},
		{"empty list", "pairs: []\n", "no pairs"},
		{"typo in key", "pairs:\n  - name: a\n    kalshi: K\n    polymarkt: \"1\"\n", "polymarkt"},
		{"unknown top-level key", "pair:\n" + pair("a", "K", assetA), "pair"},
		{"missing name", "pairs:\n" + pair("", "K", assetA), "name is required"},
		{"missing kalshi", "pairs:\n" + pair("a", "", assetA), "kalshi ticker is required"},
		{"missing polymarket", "pairs:\n" + pair("a", "K", ""), "asset ID is required"},
		{"condition ID instead of token", "pairs:\n" + pair("a", "K", "0x876506d8b2bd7a0d3fa4fe18c024eee6e1dd81ee24c26795dadd6cfe4a7b5d0d"), "not an asset (token) ID"},
		{"duplicate name", "pairs:\n" + pair("a", "K1", assetA) + pair("a", "K2", assetB), "duplicate name"},
		{"duplicate markets", "pairs:\n" + pair("a", "K", assetA) + pair("b", "K", assetA), "same markets as pair \"a\""},
		{"not yaml", "pairs: [", "parsing pair config"},
		{"last trade: bare date", "pairs:\n" + pair("a", "K", assetA) + "    kalshi_last_trade: 2026-10-28\n", "explicit offset"},
		{"last trade: UTC Z", "pairs:\n" + pair("a", "K", assetA) + "    kalshi_last_trade: 2026-10-28T17:55:00Z\n", "explicit offset"},
		{"last trade: no seconds", "pairs:\n" + pair("a", "K", assetA) + "    kalshi_last_trade: 2026-10-28T13:55-04:00\n", "explicit offset"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParsePairs([]byte(tt.yaml))
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("err = %v, want one containing %q", err, tt.wantErr)
			}
		})
	}
}

// TestRepoPairsFile checks that the committed pairs.yaml is valid.
func TestRepoPairsFile(t *testing.T) {
	pairs, err := LoadPairs("../../pairs.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if len(pairs) == 0 {
		t.Fatal("pairs.yaml has no pairs")
	}
}
