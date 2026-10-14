package polymarket

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"testing"
)

const captureAssetID = "63842529068710005716169325380315470359047749786610778647370693404952498013178"

// captureLines returns the non-blank lines of
// testdata/polymarket_capture.jsonl, BOM stripped.
func captureLines(t *testing.T) [][]byte {
	t.Helper()
	data, err := os.ReadFile("../../testdata/polymarket_capture.jsonl")
	if err != nil {
		t.Fatalf("reading capture file: %v", err)
	}
	data = bytes.TrimPrefix(data, []byte("\xef\xbb\xbf"))
	var lines [][]byte
	scanner := bufio.NewScanner(bytes.NewReader(data))
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) > 0 {
			lines = append(lines, append([]byte(nil), line...))
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scanning capture file: %v", err)
	}
	return lines
}

func eventType(t *testing.T, line []byte) string {
	t.Helper()
	var probe struct {
		EventType string `json:"event_type"`
	}
	if err := json.Unmarshal(line, &probe); err != nil {
		t.Fatalf("decoding event_type: %v", err)
	}
	return probe.EventType
}

func TestFeedReplay(t *testing.T) {
	lines := captureLines(t)

	f := NewFeed(captureAssetID)
	var changes int
	sawBook := false
	for i, line := range lines {
		changed, err := f.Handle(line)
		if err != nil {
			t.Fatalf("line %d: Handle: %v", i+1, err)
		}
		if eventType(t, line) == "book" {
			sawBook = true
		}
		if !sawBook && (changed || !f.Stale()) {
			t.Fatalf("line %d: before any book event: changed=%v stale=%v, want false, true",
				i+1, changed, f.Stale())
		}
		if changed {
			changes++
		}
	}

	if f.Stale() {
		t.Fatalf("Stale() = true after replay")
	}
	bid, ask, ok := f.Best()
	if !ok {
		t.Fatalf("Best() ok = false after replay")
	}
	// The capture's derived book matches Polymarket's own reported best;
	// the feed should end up there too.
	repBid, repAsk, _ := f.Book().LastReportedBest()
	if bid != repBid || ask != repAsk {
		t.Fatalf("Best() = (%d, %d), want venue-reported (%d, %d)", bid, ask, repBid, repAsk)
	}
	// Both book events in the capture carry the same top of book, and no
	// price_change after the first one moves it, so the first book event
	// is the only change; TestFeedReportsOnlyTopChanges covers movement.
	if changes != 1 {
		t.Fatalf("%d top-of-book changes over the capture, want 1 (the first book event)", changes)
	}
	t.Logf("%d messages, %d top-of-book changes, final best bid=%d ask=%d", len(lines), changes, bid, ask)
}

func TestFeedArrayFraming(t *testing.T) {
	var book []byte
	for _, line := range captureLines(t) {
		if eventType(t, line) == "book" {
			book = line
			break
		}
	}
	if book == nil {
		t.Fatalf("no book event in capture")
	}

	f := NewFeed(captureAssetID)
	framed := append(append([]byte("["), book...), ']')
	changed, err := f.Handle(framed)
	if err != nil {
		t.Fatalf("Handle(array-framed book): %v", err)
	}
	if !changed || f.Stale() {
		t.Fatalf("array-framed book: changed=%v stale=%v, want true, false", changed, f.Stale())
	}
}

func TestFeedIgnoresOtherAssetBook(t *testing.T) {
	var book []byte
	for _, line := range captureLines(t) {
		if eventType(t, line) == "book" {
			book = line
			break
		}
	}

	f := NewFeed("some-other-asset")
	changed, err := f.Handle(book)
	if err != nil || changed || !f.Stale() {
		t.Fatalf("book for another asset: changed=%v err=%v stale=%v, want false, nil, true",
			changed, err, f.Stale())
	}
}

func TestFeedReportsOnlyTopChanges(t *testing.T) {
	f := NewFeed(captureAssetID)
	for i, line := range captureLines(t) {
		if _, err := f.Handle(line); err != nil {
			t.Fatalf("line %d: Handle: %v", i+1, err)
		}
	}
	bid, ask, _ := f.Best()

	change := func(price, size string) []byte {
		return []byte(fmt.Sprintf(
			`{"price_changes":[{"asset_id":%q,"price":%q,"size":%q,"side":"BUY"}],"event_type":"price_change"}`,
			captureAssetID, price, size))
	}

	steps := []struct {
		name        string
		price, size string
		wantChanged bool
		wantBid     int
	}{
		{"deep level, top unchanged", "0.02", "100", false, bid},
		{"new best bid inside the spread", "0.815", "10", true, 8150},
		{"resize the new best, price unchanged", "0.815", "20", false, 8150},
		{"remove the new best", "0.815", "0", true, bid},
	}
	for _, s := range steps {
		changed, err := f.Handle(change(s.price, s.size))
		if err != nil {
			t.Fatalf("%s: Handle: %v", s.name, err)
		}
		gotBid, gotAsk, _ := f.Best()
		if changed != s.wantChanged || gotBid != s.wantBid || gotAsk != ask {
			t.Fatalf("%s: changed=%v best=(%d, %d), want changed=%v best=(%d, %d)",
				s.name, changed, gotBid, gotAsk, s.wantChanged, s.wantBid, ask)
		}
	}
}
