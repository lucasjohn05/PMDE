package kalshi

import (
	"encoding/json"
	"testing"
	"time"
)

// TestFeedLag checks Lag against ts_ms on the capture's messages: deltas
// carry it, snapshots don't, and a delta counts whether or not it was
// applied to the book.
func TestFeedLag(t *testing.T) {
	var snapshot []byte
	var deltas [][]byte
	for _, line := range captureLines(t) {
		switch messageType(t, line) {
		case "orderbook_snapshot":
			snapshot = line
		case "orderbook_delta":
			deltas = append(deltas, line)
		}
	}
	tsOf := func(raw []byte) time.Time {
		var m struct {
			Msg struct {
				TsMs int64 `json:"ts_ms"`
			} `json:"msg"`
		}
		if err := json.Unmarshal(raw, &m); err != nil || m.Msg.TsMs == 0 {
			t.Fatalf("no ts_ms in %s", raw)
		}
		return time.UnixMilli(m.Msg.TsMs)
	}

	var f Feed

	// A delta before any snapshot isn't applied, but still has a lag.
	recv := tsOf(deltas[0]).Add(250 * time.Millisecond)
	f.HandleQueued(Queued{Raw: deltas[0], Recv: recv})
	if lag, ok := f.Lag(); !ok || lag != 250*time.Millisecond {
		t.Fatalf("unapplied delta: Lag() = %v, %v; want 250ms, true", lag, ok)
	}

	// Snapshots carry no ts_ms.
	f.HandleQueued(Queued{Raw: snapshot, Recv: recv})
	if lag, ok := f.Lag(); ok {
		t.Fatalf("snapshot: Lag() = %v, true; want no lag", lag)
	}

	// Clock skew can make it negative; it's reported as is.
	f.HandleQueued(Queued{Raw: deltas[0], Recv: tsOf(deltas[0]).Add(-30 * time.Millisecond)})
	if lag, ok := f.Lag(); !ok || lag != -30*time.Millisecond {
		t.Fatalf("skewed delta: Lag() = %v, %v; want -30ms, true", lag, ok)
	}

	// No receive time, no lag.
	f.HandleQueued(Queued{Raw: deltas[1]})
	if lag, ok := f.Lag(); ok {
		t.Fatalf("no Recv: Lag() = %v, true; want no lag", lag)
	}

	// Through the queue, Offer stamps the receive time.
	q := NewQueue(4)
	before := time.Now()
	q.Offer(deltas[2])
	after := time.Now()
	nextWithin(t, &f, q, time.Second)
	lag, ok := f.Lag()
	ts := tsOf(deltas[2])
	if !ok || lag < before.Sub(ts) || lag > after.Sub(ts) {
		t.Fatalf("via Next: Lag() = %v, %v; want between %v and %v", lag, ok, before.Sub(ts), after.Sub(ts))
	}

	// A Next step that handles no message clears it, so a client
	// observing Lag after every step never counts a message twice.
	q.Close()
	nextWithin(t, &f, q, time.Second)
	if lag, ok := f.Lag(); ok {
		t.Fatalf("after a step with no message: Lag() = %v, true; want no lag", lag)
	}
}

// TestFeedStampAndSizes: Stamp reports the handled message's receive and
// venue times (none for a snapshot, none after a step with no message),
// and TopSizes the sizes behind Best, the ask's being the "no" size at
// 10000-ask.
func TestFeedStampAndSizes(t *testing.T) {
	var snapshot, delta []byte
	for _, line := range captureLines(t) {
		switch messageType(t, line) {
		case "orderbook_snapshot":
			snapshot = line
		case "orderbook_delta":
			if delta == nil {
				delta = line
			}
		}
	}

	var f Feed
	if bid, ask := f.TopSizes(); bid != 0 || ask != 0 {
		t.Fatalf("TopSizes before any snapshot = %d/%d, want 0/0", bid, ask)
	}

	recv := time.Date(2026, 9, 11, 15, 0, 0, 0, time.UTC)
	f.HandleQueued(Queued{Raw: snapshot, Recv: recv})
	if r, ms := f.Stamp(); !r.Equal(recv) || ms != 0 {
		t.Fatalf("snapshot: Stamp() = %v, %d; want %v, 0", r, ms, recv)
	}
	bid, ask, ok := f.Best()
	bidSz, askSz := f.TopSizes()
	wantBid, _ := f.book.YesSize(bid)
	wantAsk, _ := f.book.NoSize(10000 - ask)
	if !ok || bidSz != wantBid || askSz != wantAsk || bidSz == 0 || askSz == 0 {
		t.Fatalf("TopSizes() = %d/%d at %d/%d, want %d/%d (nonzero)", bidSz, askSz, bid, ask, wantBid, wantAsk)
	}

	f.HandleQueued(Queued{Raw: delta, Recv: recv.Add(time.Second)})
	if r, ms := f.Stamp(); !r.Equal(recv.Add(time.Second)) || ms != tsOfDelta(t, delta) {
		t.Fatalf("delta: Stamp() = %v, %d; want %v, %d", r, ms, recv.Add(time.Second), tsOfDelta(t, delta))
	}

	q := NewQueue(1)
	q.Close()
	nextWithin(t, &f, q, time.Second)
	if r, ms := f.Stamp(); !r.IsZero() || ms != 0 {
		t.Fatalf("after a step with no message: Stamp() = %v, %d; want zero", r, ms)
	}
}

func tsOfDelta(t *testing.T, raw []byte) int64 {
	t.Helper()
	var m struct {
		Msg struct {
			TsMs int64 `json:"ts_ms"`
		} `json:"msg"`
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	return m.Msg.TsMs
}
