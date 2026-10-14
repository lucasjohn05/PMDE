package kalshi

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"testing"
)

// captureLines returns the non-blank lines of testdata/kalshi_capture.jsonl,
// BOM stripped.
func captureLines(t *testing.T) [][]byte {
	t.Helper()
	data, err := os.ReadFile("../../testdata/kalshi_capture.jsonl")
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

func messageType(t *testing.T, line []byte) string {
	t.Helper()
	var probe struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(line, &probe); err != nil {
		t.Fatalf("decoding type: %v", err)
	}
	return probe.Type
}

func TestFeedReplay(t *testing.T) {
	lines := captureLines(t)

	var f Feed
	if !f.Stale() {
		t.Fatalf("new Feed: Stale() = false, want true before any snapshot")
	}

	var changes int
	for i, line := range lines {
		changed, err := f.Handle(line)
		if err != nil {
			t.Fatalf("line %d: Handle: %v", i+1, err)
		}
		if changed {
			changes++
		}
	}

	if f.Stale() {
		t.Fatalf("Stale() = true after a clean replay")
	}
	bid, ask, ok := f.Best()
	wantBid, wantAsk, wantOK := f.Book().BestYes()
	if bid != wantBid || ask != wantAsk || ok != wantOK {
		t.Fatalf("Best() = (%d, %d, %v), want Book().BestYes() = (%d, %d, %v)",
			bid, ask, ok, wantBid, wantAsk, wantOK)
	}
	// The capture's top of book never moves after the snapshot, so this
	// is the snapshot's change alone; TestFeedReportsOnlyTopChanges
	// covers movement.
	if changes != 1 {
		t.Fatalf("%d top-of-book changes over the capture, want 1 (the snapshot)", changes)
	}
	t.Logf("%d messages, %d top-of-book changes, final best bid=%d ask=%d", len(lines), changes, bid, ask)
}

func TestFeedSeqGapGoesStale(t *testing.T) {
	lines := captureLines(t)

	// Drop one delta mid-stream to simulate a missed message.
	skip := -1
	for i := len(lines) / 2; i < len(lines); i++ {
		if messageType(t, lines[i]) == "orderbook_delta" {
			skip = i
			break
		}
	}
	if skip < 0 {
		t.Fatalf("no delta found in second half of capture")
	}

	var f Feed
	for i := 0; i < skip; i++ {
		if _, err := f.Handle(lines[i]); err != nil {
			t.Fatalf("line %d: Handle: %v", i+1, err)
		}
	}
	seqBefore := f.Book().LastSeq()

	// The delta right after the skipped one is the gap.
	if _, err := f.Handle(lines[skip+1]); err == nil {
		t.Fatalf("Handle after skipped delta: err = nil, want a seq gap error")
	}
	if !f.Stale() {
		t.Fatalf("Stale() = false after a seq gap")
	}
	if _, _, ok := f.Best(); ok {
		t.Fatalf("Best() ok = true on a stale book")
	}

	// Every later delta is ignored, quietly: no error, no change, no seq
	// movement.
	for i := skip + 2; i < len(lines); i++ {
		changed, err := f.Handle(lines[i])
		if err != nil || changed {
			t.Fatalf("line %d on stale book: changed=%v err=%v, want false, nil", i+1, changed, err)
		}
	}
	if got := f.Book().LastSeq(); got != seqBefore {
		t.Fatalf("LastSeq() = %d after stale replay, want unchanged %d", got, seqBefore)
	}

	// A fresh snapshot is the only way back.
	for _, line := range lines {
		if messageType(t, line) == "orderbook_snapshot" {
			if _, err := f.Handle(line); err != nil {
				t.Fatalf("re-applying snapshot: %v", err)
			}
			break
		}
	}
	if f.Stale() {
		t.Fatalf("Stale() = true after a fresh snapshot")
	}
}

func TestFeedIgnoresDeltaBeforeSnapshot(t *testing.T) {
	lines := captureLines(t)

	var f Feed
	for _, line := range lines {
		if messageType(t, line) != "orderbook_delta" {
			continue
		}
		changed, err := f.Handle(line)
		if err != nil || changed {
			t.Fatalf("delta before snapshot: changed=%v err=%v, want false, nil", changed, err)
		}
		break
	}
	if !f.Stale() {
		t.Fatalf("Stale() = false with no snapshot applied")
	}
}

func TestFeedReportsOnlyTopChanges(t *testing.T) {
	lines := captureLines(t)

	var f Feed
	for i, line := range lines {
		if _, err := f.Handle(line); err != nil {
			t.Fatalf("line %d: Handle: %v", i+1, err)
		}
	}
	bid, ask, _ := f.Best()

	delta := func(price, size string) []byte {
		return []byte(fmt.Sprintf(
			`{"type":"orderbook_delta","sid":1,"seq":%d,"msg":{"market_ticker":%q,"price_dollars":%q,"delta_fp":%q,"side":"yes"}}`,
			f.Book().LastSeq()+1, f.Book().MarketTicker, price, size))
	}

	steps := []struct {
		name        string
		price, size string
		wantChanged bool
		wantBid     int
	}{
		{"deep level, top unchanged", "0.0100", "5.00", false, bid},
		{"new best bid inside the spread", "0.8050", "10.00", true, 8050},
		{"add size at the new best, price unchanged", "0.8050", "1.00", false, 8050},
		{"remove the new best", "0.8050", "-11.00", true, bid},
	}
	for _, s := range steps {
		changed, err := f.Handle(delta(s.price, s.size))
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
