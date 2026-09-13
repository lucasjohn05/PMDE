package polymarket

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

// applyStep is one message applied to a Book during a test case, tagged
// with which method it should go through.
type applyStep struct {
	kind string // "snapshot" or "price_change"
	json string
}

func TestBook(t *testing.T) {
	cases := []struct {
		name    string
		assetID string
		steps   []applyStep
		check   func(t *testing.T, b *Book, errs []error)
	}{
		{
			name:    "price_change assigns absolute size, not increment",
			assetID: "ASSET-A",
			steps: []applyStep{
				{kind: "price_change", json: `{"event_type":"price_change","price_changes":[` +
					`{"asset_id":"ASSET-A","price":"0.5000","size":"100.00","side":"BUY"}]}`},
				{kind: "price_change", json: `{"event_type":"price_change","price_changes":[` +
					`{"asset_id":"ASSET-A","price":"0.5000","size":"40.00","side":"BUY"}]}`},
			},
			check: func(t *testing.T, b *Book, errs []error) {
				for i, err := range errs {
					if err != nil {
						t.Fatalf("step %d: unexpected error: %v", i, err)
					}
				}
				// If this were Kalshi's delta_fp semantics, this would be
				// 140.00 (14000). Polymarket's size is absolute: the
				// second message must replace, not add to, the first.
				if size, ok := b.BidSize(5000); !ok || size != 4000 {
					t.Errorf("BidSize(5000) = (%d, %v), want (4000, true)", size, ok)
				}
			},
		},
		{
			name:    "book event mid-stream fully replaces state",
			assetID: "ASSET-A",
			steps: []applyStep{
				{kind: "price_change", json: `{"event_type":"price_change","price_changes":[` +
					`{"asset_id":"ASSET-A","price":"0.3000","size":"10.00","side":"BUY"},` +
					`{"asset_id":"ASSET-A","price":"0.7000","size":"5.00","side":"SELL"}]}`},
				{kind: "snapshot", json: `{"event_type":"book","asset_id":"ASSET-A",` +
					`"bids":[{"price":"0.1000","size":"1.00"}],` +
					`"asks":[{"price":"0.9000","size":"2.00"}]}`},
			},
			check: func(t *testing.T, b *Book, errs []error) {
				for i, err := range errs {
					if err != nil {
						t.Fatalf("step %d: unexpected error: %v", i, err)
					}
				}
				// The old levels from the price_change must be gone
				// entirely, not merged with the snapshot's.
				if _, ok := b.BidSize(3000); ok {
					t.Errorf("BidSize(3000) still present after a replacing book event")
				}
				if _, ok := b.AskSize(7000); ok {
					t.Errorf("AskSize(7000) still present after a replacing book event")
				}
				if size, ok := b.BidSize(1000); !ok || size != 100 {
					t.Errorf("BidSize(1000) = (%d, %v), want (100, true)", size, ok)
				}
				if size, ok := b.AskSize(9000); !ok || size != 200 {
					t.Errorf("AskSize(9000) = (%d, %v), want (200, true)", size, ok)
				}
			},
		},
		{
			name:    "size zero removes the level",
			assetID: "ASSET-A",
			steps: []applyStep{
				{kind: "price_change", json: `{"event_type":"price_change","price_changes":[` +
					`{"asset_id":"ASSET-A","price":"0.4000","size":"20.00","side":"BUY"}]}`},
				{kind: "price_change", json: `{"event_type":"price_change","price_changes":[` +
					`{"asset_id":"ASSET-A","price":"0.4000","size":"0","side":"BUY"}]}`},
			},
			check: func(t *testing.T, b *Book, errs []error) {
				for i, err := range errs {
					if err != nil {
						t.Fatalf("step %d: unexpected error: %v", i, err)
					}
				}
				if size, ok := b.BidSize(4000); ok {
					t.Errorf("BidSize(4000) = (%d, true), want level removed (ok=false)", size)
				}
			},
		},
		{
			name:    "complement asset entries are filtered out",
			assetID: "ASSET-A",
			steps: []applyStep{
				{kind: "price_change", json: `{"event_type":"price_change","price_changes":[` +
					`{"asset_id":"ASSET-A","price":"0.6000","size":"5.00","side":"BUY","best_bid":"0.60","best_ask":"0.65"},` +
					`{"asset_id":"ASSET-B","price":"0.4000","size":"5.00","side":"SELL","best_bid":"0.35","best_ask":"0.40"}]}`},
			},
			check: func(t *testing.T, b *Book, errs []error) {
				if errs[0] != nil {
					t.Fatalf("unexpected error: %v", errs[0])
				}
				if size, ok := b.BidSize(6000); !ok || size != 500 {
					t.Errorf("BidSize(6000) = (%d, %v), want (500, true) — our own entry", size, ok)
				}
				if _, ok := b.AskSize(4000); ok {
					t.Errorf("AskSize(4000) present — the complement's entry must not be applied")
				}
				bid, ask, ok := b.LastReportedBest()
				if !ok {
					t.Fatalf("LastReportedBest ok = false")
				}
				if bid != 6000 || ask != 6500 {
					t.Errorf("LastReportedBest = (%d, %d), want (6000, 6500) — our own entry's best, not the complement's", bid, ask)
				}
			},
		},
		{
			name:    "Best is highest bid and lowest ask",
			assetID: "ASSET-A",
			steps: []applyStep{
				{kind: "snapshot", json: `{"event_type":"book","asset_id":"ASSET-A",` +
					`"bids":[{"price":"0.1000","size":"1.00"},{"price":"0.3000","size":"1.00"},{"price":"0.2500","size":"1.00"}],` +
					`"asks":[{"price":"0.9000","size":"1.00"},{"price":"0.7000","size":"1.00"},{"price":"0.8000","size":"1.00"}]}`},
			},
			check: func(t *testing.T, b *Book, errs []error) {
				if errs[0] != nil {
					t.Fatalf("unexpected error: %v", errs[0])
				}
				bid, ask, ok := b.Best()
				if !ok {
					t.Fatalf("Best ok = false")
				}
				if bid != 3000 {
					t.Errorf("bid = %d, want 3000 (highest bid)", bid)
				}
				if ask != 7000 {
					t.Errorf("ask = %d, want 7000 (lowest ask)", ask)
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := NewBook(tc.assetID)
			errs := make([]error, len(tc.steps))
			for i, s := range tc.steps {
				switch s.kind {
				case "snapshot":
					errs[i] = b.ApplySnapshot([]byte(s.json))
				case "price_change":
					errs[i] = b.ApplyPriceChange([]byte(s.json))
				default:
					t.Fatalf("step %d: unknown kind %q", i, s.kind)
				}
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
		{name: "price with 2 decimals scaled to bps", in: "0.81", decimals: 4, want: 8100},
		{name: "price with 1 decimal scaled to bps", in: "0.1", decimals: 4, want: 1000},
		{name: "size with 2 decimals", in: "13121.73", decimals: 2, want: 1312173},
		{name: "whole-number size", in: "60500", decimals: 2, want: 6050000},
		{name: "size with 1 decimal", in: "3406.6", decimals: 2, want: 340660},
		{name: "zero size, no decimal point", in: "0", decimals: 2, want: 0},
		{name: "too many decimal places", in: "0.811", decimals: 2, wantErr: true},
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

// TestBestOverCapture replays the real capture at
// testdata/polymarket_capture.jsonl (127 lines: 2 book events, the rest
// price_change) for the asset the fixture's price_change entries mostly
// quote, and reports Best() alongside the stream's own last-reported
// best_bid/best_ask, so the two can be compared instead of reconciled.
func TestBestOverCapture(t *testing.T) {
	const assetID = "63842529068710005716169325380315470359047749786610778647370693404952498013178"

	f, err := os.Open("../../testdata/polymarket_capture.jsonl")
	if err != nil {
		t.Fatalf("opening capture file: %v", err)
	}
	defer f.Close()

	b := NewBook(assetID)
	var books, priceChanges, lastTradePrices int
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for lineNo := 1; scanner.Scan(); lineNo++ {
		line := scanner.Bytes()
		if lineNo == 1 {
			line = bytes.TrimPrefix(line, []byte("\xef\xbb\xbf"))
		}
		if len(strings.TrimSpace(string(line))) == 0 {
			continue
		}

		var probe struct {
			EventType string `json:"event_type"`
		}
		if err := json.Unmarshal(line, &probe); err != nil {
			t.Fatalf("line %d: decoding event_type: %v", lineNo, err)
		}

		switch probe.EventType {
		case "book":
			if err := b.ApplySnapshot(line); err != nil {
				t.Fatalf("line %d: ApplySnapshot: %v", lineNo, err)
			}
			books++
		case "price_change":
			if err := b.ApplyPriceChange(line); err != nil {
				t.Fatalf("line %d: ApplyPriceChange: %v", lineNo, err)
			}
			priceChanges++
		case "last_trade_price":
			// A trade print, not a book-state message; this Book type has
			// no use for it. Present in the fixture but not called out in
			// the "2 book events and the rest price_change" description —
			// skipped rather than silently miscounted as a price_change.
			lastTradePrices++
		default:
			t.Fatalf("line %d: unexpected event_type %q", lineNo, probe.EventType)
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scanning capture file: %v", err)
	}

	t.Logf("replayed %d book event(s), %d price_change event(s), %d last_trade_price event(s) (ignored)",
		books, priceChanges, lastTradePrices)

	bid, ask, ok := b.Best()
	if !ok {
		t.Fatalf("Best ok = false after replaying the full capture")
	}
	t.Logf("derived Best(): bid=%d bps (%s) ask=%d bps (%s)", bid, bpsToDollars(bid), ask, bpsToDollars(ask))

	repBid, repAsk, repOK := b.LastReportedBest()
	if !repOK {
		t.Fatalf("LastReportedBest ok = false after replaying the full capture")
	}
	t.Logf("stream-reported best: best_bid=%d bps (%s) best_ask=%d bps (%s)",
		repBid, bpsToDollars(repBid), repAsk, bpsToDollars(repAsk))

	if bid != repBid || ask != repAsk {
		t.Logf("DISAGREEMENT: derived Best() = (%d, %d) but stream-reported best = (%d, %d)",
			bid, ask, repBid, repAsk)
	} else {
		t.Logf("derived Best() matches the stream-reported best exactly")
	}

	t.Logf("top 5 bid levels (price, size):")
	for _, lvl := range topLevels(b.bids, 5, false) {
		t.Logf("  %s (%d bps)  size=%s", bpsToDollars(lvl.priceBps), lvl.priceBps, centiToDollars(lvl.size))
	}
	t.Logf("top 5 ask levels (price, size):")
	for _, lvl := range topLevels(b.asks, 5, true) {
		t.Logf("  %s (%d bps)  size=%s", bpsToDollars(lvl.priceBps), lvl.priceBps, centiToDollars(lvl.size))
	}
}

// level is a (price, size) pair used only for reporting top-of-book levels
// in test output.
type level struct {
	priceBps int
	size     int64
}

// topLevels returns up to n levels from a price->size map, sorted by
// price ascending if asc is true (for asks, best = lowest first) or
// descending otherwise (for bids, best = highest first).
func topLevels(levels map[int]int64, n int, asc bool) []level {
	out := make([]level, 0, len(levels))
	for p, s := range levels {
		out = append(out, level{priceBps: p, size: s})
	}
	sort.Slice(out, func(i, j int) bool {
		if asc {
			return out[i].priceBps < out[j].priceBps
		}
		return out[i].priceBps > out[j].priceBps
	})
	if len(out) > n {
		out = out[:n]
	}
	return out
}

// bpsToDollars formats basis points as a dollar string purely through
// integer division/modulo, for human-readable logging only — no float
// ever holds the price.
func bpsToDollars(bps int) string {
	return fmt.Sprintf("%d.%04d", bps/10000, bps%10000)
}

// centiToDollars formats a centi-unit size (size*100) as a decimal
// string, purely through integer division/modulo.
func centiToDollars(n int64) string {
	if n < 0 {
		return "-" + centiToDollars(-n)
	}
	return fmt.Sprintf("%d.%02d", n/100, n%100)
}
