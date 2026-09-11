package kalshi

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
)

func TestBook(t *testing.T) {
	cases := []struct {
		name     string
		snapshot string
		deltas   []string
		check    func(t *testing.T, b *Book, errs []error)
	}{
		{
			name: "snapshot then deltas increments size",
			snapshot: `{"type":"orderbook_snapshot","sid":1,"seq":1,"msg":{"market_ticker":"TEST-MARKET",` +
				`"yes_dollars_fp":[["0.4300","10.00"]],"no_dollars_fp":[["0.5000","5.00"]]}}`,
			deltas: []string{
				`{"type":"orderbook_delta","sid":1,"seq":2,"msg":{"market_ticker":"TEST-MARKET",` +
					`"price_dollars":"0.4300","delta_fp":"5.00","side":"yes"}}`,
				`{"type":"orderbook_delta","sid":1,"seq":3,"msg":{"market_ticker":"TEST-MARKET",` +
					`"price_dollars":"0.5000","delta_fp":"-2.00","side":"no"}}`,
			},
			check: func(t *testing.T, b *Book, errs []error) {
				for i, err := range errs {
					if err != nil {
						t.Fatalf("delta %d: unexpected error: %v", i, err)
					}
				}
				if size, ok := b.YesSize(4300); !ok || size != 1500 {
					t.Errorf("YesSize(4300) = (%d, %v), want (1500, true)", size, ok)
				}
				if size, ok := b.NoSize(5000); !ok || size != 300 {
					t.Errorf("NoSize(5000) = (%d, %v), want (300, true)", size, ok)
				}
			},
		},
		{
			name: "level removed when size reaches zero",
			snapshot: `{"type":"orderbook_snapshot","sid":1,"seq":1,"msg":{"market_ticker":"TEST-MARKET",` +
				`"yes_dollars_fp":[["0.2000","3.00"]],"no_dollars_fp":[["0.8000","1.00"]]}}`,
			deltas: []string{
				`{"type":"orderbook_delta","sid":1,"seq":2,"msg":{"market_ticker":"TEST-MARKET",` +
					`"price_dollars":"0.2000","delta_fp":"-3.00","side":"yes"}}`,
			},
			check: func(t *testing.T, b *Book, errs []error) {
				if errs[0] != nil {
					t.Fatalf("unexpected error: %v", errs[0])
				}
				if size, ok := b.YesSize(2000); ok {
					t.Errorf("YesSize(2000) = (%d, true), want level removed (ok=false)", size)
				}
				if _, _, ok := b.BestYes(); ok {
					t.Errorf("BestYes ok = true with an empty yes side, want false")
				}
			},
		},
		{
			name: "yes/ask inversion arithmetic",
			snapshot: `{"type":"orderbook_snapshot","sid":1,"seq":1,"msg":{"market_ticker":"TEST-MARKET",` +
				`"yes_dollars_fp":[["0.3000","1.00"],["0.4500","2.00"]],` +
				`"no_dollars_fp":[["0.1000","1.00"],["0.4200","3.00"]]}}`,
			check: func(t *testing.T, b *Book, errs []error) {
				bid, ask, ok := b.BestYes()
				if !ok {
					t.Fatalf("BestYes ok = false, want true")
				}
				if bid != 4500 {
					t.Errorf("bid = %d, want 4500 (highest yes level)", bid)
				}
				if ask != 5800 {
					t.Errorf("ask = %d, want 5800 (10000 - highest no level 4200)", ask)
				}
			},
		},
		{
			name: "sequence gap rejected without applying",
			snapshot: `{"type":"orderbook_snapshot","sid":1,"seq":1,"msg":{"market_ticker":"TEST-MARKET",` +
				`"yes_dollars_fp":[["0.5000","1.00"]],"no_dollars_fp":[["0.5000","1.00"]]}}`,
			deltas: []string{
				`{"type":"orderbook_delta","sid":1,"seq":2,"msg":{"market_ticker":"TEST-MARKET",` +
					`"price_dollars":"0.5000","delta_fp":"1.00","side":"yes"}}`,
				// seq jumps from 2 to 4, skipping 3: a gap.
				`{"type":"orderbook_delta","sid":1,"seq":4,"msg":{"market_ticker":"TEST-MARKET",` +
					`"price_dollars":"0.5000","delta_fp":"1.00","side":"yes"}}`,
			},
			check: func(t *testing.T, b *Book, errs []error) {
				if errs[0] != nil {
					t.Fatalf("delta 0 (seq 2): unexpected error: %v", errs[0])
				}
				if errs[1] == nil {
					t.Fatalf("delta 1 (seq 4, a gap): expected an error, got nil")
				}
				// The gapped delta must not have been applied: size should
				// reflect only the first delta (1.00 + 1.00 = 2.00), and
				// lastSeq must not have advanced past the last good delta.
				if size, ok := b.YesSize(5000); !ok || size != 200 {
					t.Errorf("YesSize(5000) = (%d, %v), want (200, true) — gapped delta must not apply", size, ok)
				}
				if got := b.LastSeq(); got != 2 {
					t.Errorf("LastSeq() = %d, want 2 (unchanged by the rejected delta)", got)
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := &Book{}
			if err := b.ApplySnapshot([]byte(tc.snapshot)); err != nil {
				t.Fatalf("ApplySnapshot: unexpected error: %v", err)
			}
			errs := make([]error, len(tc.deltas))
			for i, d := range tc.deltas {
				errs[i] = b.ApplyDelta([]byte(d))
			}
			tc.check(t, b, errs)
		})
	}
}

func TestParseFixedPoint(t *testing.T) {
	cases := []struct {
		name     string
		in       string
		decimals int
		want     int64
		wantErr  bool
	}{
		{name: "price with 4 decimals", in: "0.4300", decimals: 4, want: 4300},
		{name: "size with 2 decimals", in: "47.00", decimals: 2, want: 4700},
		{name: "negative delta", in: "-94.69", decimals: 2, want: -9469},
		{name: "large integer size", in: "357122.00", decimals: 2, want: 35712200},
		{name: "whole dollar price", in: "1.0000", decimals: 4, want: 10000},
		{name: "too many decimal places", in: "0.43001", decimals: 4, wantErr: true},
		{name: "not a number", in: "abc", decimals: 4, wantErr: true},
		{name: "empty string", in: "", decimals: 4, wantErr: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseFixedPoint(tc.in, tc.decimals)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("parseFixedPoint(%q, %d) = %d, want error", tc.in, tc.decimals, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseFixedPoint(%q, %d): unexpected error: %v", tc.in, tc.decimals, err)
			}
			if got != tc.want {
				t.Errorf("parseFixedPoint(%q, %d) = %d, want %d", tc.in, tc.decimals, got, tc.want)
			}
		})
	}
}

// TestBestYesOverCapture replays the real capture at
// testdata/kalshi_capture.jsonl (1 subscribed ack, 1 orderbook_snapshot,
// 201 contiguous orderbook_delta messages) end to end and reports the
// resulting BestYes, so it can be eyeballed against Kalshi's site at
// capture time.
func TestBestYesOverCapture(t *testing.T) {
	f, err := os.Open("../../testdata/kalshi_capture.jsonl")
	if err != nil {
		t.Fatalf("opening capture file: %v", err)
	}
	defer f.Close()

	b := &Book{}
	var snapshots, deltas, acks int
	var negativeAttempts, removals int
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for lineNo := 1; scanner.Scan(); lineNo++ {
		line := scanner.Bytes()
		if lineNo == 1 {
			// The capture file starts with a UTF-8 BOM; strip it so the
			// first line still decodes as JSON.
			line = bytes.TrimPrefix(line, []byte("\xef\xbb\xbf"))
		}
		if len(strings.TrimSpace(string(line))) == 0 {
			continue
		}

		var probe struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(line, &probe); err != nil {
			t.Fatalf("line %d: decoding type: %v", lineNo, err)
		}

		switch probe.Type {
		case "subscribed":
			acks++
		case "orderbook_snapshot":
			if err := b.ApplySnapshot(line); err != nil {
				t.Fatalf("line %d: ApplySnapshot: %v", lineNo, err)
			}
			snapshots++
		case "orderbook_delta":
			// Diagnostics: work out what this delta will do to its level
			// before applying it, so we can report whether it ever would
			// have gone negative and whether it removes a level at zero.
			// This duplicates ApplyDelta's own arithmetic deliberately —
			// it's independent instrumentation, not a shortcut around it.
			var env struct {
				Msg deltaMsg `json:"msg"`
			}
			if err := json.Unmarshal(line, &env); err != nil {
				t.Fatalf("line %d: decoding delta for diagnostics: %v", lineNo, err)
			}
			priceBps, err := parseFixedPoint(env.Msg.PriceDollars, 4)
			if err != nil {
				t.Fatalf("line %d: parsing price for diagnostics: %v", lineNo, err)
			}
			delta, err := parseFixedPoint(env.Msg.DeltaFP, 2)
			if err != nil {
				t.Fatalf("line %d: parsing delta for diagnostics: %v", lineNo, err)
			}
			var oldSize int64
			var hadLevel bool
			switch env.Msg.Side {
			case "yes":
				oldSize, hadLevel = b.YesSize(int(priceBps))
			case "no":
				oldSize, hadLevel = b.NoSize(int(priceBps))
			}
			if oldSize+delta < 0 {
				negativeAttempts++
			}

			if err := b.ApplyDelta(line); err != nil {
				t.Fatalf("line %d: ApplyDelta: %v", lineNo, err)
			}
			deltas++

			if hadLevel && oldSize+delta == 0 {
				removals++
			}
		default:
			t.Fatalf("line %d: unexpected message type %q", lineNo, probe.Type)
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scanning capture file: %v", err)
	}

	t.Logf("replayed %d subscribed ack(s), %d snapshot(s), %d delta(s); final lastSeq=%d",
		acks, snapshots, deltas, b.LastSeq())
	t.Logf("negative-size attempts during replay: %d", negativeAttempts)
	t.Logf("levels removed at zero during replay: %d", removals)

	// Raw top-of-book, before the yes/no inversion, straight off the same
	// unexported maps BestYes reads — so 10000-topNo can be checked
	// arithmetically against BestYes's own ask.
	topYes, topYesOK := highestNonZero(b.yes)
	topNo, topNoOK := highestNonZero(b.no)
	if !topYesOK || !topNoOK {
		t.Fatalf("book empty after replay: topYesOK=%v topNoOK=%v", topYesOK, topNoOK)
	}
	computedAsk := 10000 - topNo
	t.Logf("raw top-of-book: topYes=%d bps (%s) topNo=%d bps (%s) 10000-topNo=%d bps (%s)",
		topYes, bpsToDollars(topYes), topNo, bpsToDollars(topNo), computedAsk, bpsToDollars(computedAsk))

	bid, ask, ok := b.BestYes()
	if !ok {
		t.Fatalf("BestYes ok = false after replaying the full capture")
	}
	if bid != topYes || ask != computedAsk {
		t.Errorf("BestYes = (%d, %d), want (%d, %d) matching raw top-of-book", bid, ask, topYes, computedAsk)
	}
	mid := (bid + ask) / 2
	t.Logf("BestYes: bid=%d bps (%s) ask=%d bps (%s) mid=%d bps (%s)",
		bid, bpsToDollars(bid), ask, bpsToDollars(ask), mid, bpsToDollars(mid))

	t.Logf("top 5 yes levels (price, size):")
	for _, lvl := range topLevels(b.yes, 5) {
		t.Logf("  %s (%d bps)  size=%s", bpsToDollars(lvl.priceBps), lvl.priceBps, centiToDollars(lvl.size))
	}
	t.Logf("top 5 no levels (price, size):")
	for _, lvl := range topLevels(b.no, 5) {
		t.Logf("  %s (%d bps)  size=%s", bpsToDollars(lvl.priceBps), lvl.priceBps, centiToDollars(lvl.size))
	}
}

// level is a (price, size) pair used only for reporting top-of-book levels
// in test output.
type level struct {
	priceBps int
	size     int64
}

// topLevels returns up to n levels from a price->size map, sorted by price
// descending.
func topLevels(levels map[int]int64, n int) []level {
	out := make([]level, 0, len(levels))
	for p, s := range levels {
		out = append(out, level{priceBps: p, size: s})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].priceBps > out[j].priceBps })
	if len(out) > n {
		out = out[:n]
	}
	return out
}

// centiToDollars formats a centi-unit size (size*100) as a decimal string,
// purely through integer division/modulo.
func centiToDollars(n int64) string {
	if n < 0 {
		return "-" + centiToDollars(-n)
	}
	return fmt.Sprintf("%d.%02d", n/100, n%100)
}

// bpsToDollars formats basis points as a dollar string purely through
// integer division/modulo, for human-readable logging only — no float
// ever holds the price.
func bpsToDollars(bps int) string {
	return fmt.Sprintf("%d.%04d", bps/10000, bps%10000)
}
