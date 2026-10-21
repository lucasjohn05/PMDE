package polymarket

import (
	"encoding/json"
	"strconv"
	"testing"
	"time"
)

// TestFeedLag checks Lag against the "timestamp" field: price_change and
// last_trade_price events count, whether or not they were applied to the
// book. Book events carry a timestamp too, but it's when the book last
// changed, so they don't.
func TestFeedLag(t *testing.T) {
	lines := captureLines(t)
	tsOf := func(raw []byte) time.Time {
		var m struct {
			Timestamp string `json:"timestamp"`
		}
		json.Unmarshal(raw, &m)
		ms, err := strconv.ParseInt(m.Timestamp, 10, 64)
		if err != nil {
			t.Fatalf("no timestamp in %s", raw)
		}
		return time.UnixMilli(ms)
	}
	first := func(kind string) []byte {
		for _, line := range lines {
			if eventType(t, line) == kind {
				return line
			}
		}
		t.Fatalf("no %s event in capture", kind)
		return nil
	}

	f := NewFeed(captureAssetID)
	for _, et := range []string{"price_change", "last_trade_price"} {
		raw := first(et)
		f.HandleQueued(Queued{Raw: raw, Recv: tsOf(raw).Add(120 * time.Millisecond)})
		if lag, ok := f.Lag(); !ok || lag != 120*time.Millisecond {
			t.Fatalf("%s: Lag() = %v, %v; want 120ms, true", et, lag, ok)
		}
	}

	book := first("book")
	f.HandleQueued(Queued{Raw: book, Recv: tsOf(book).Add(120 * time.Millisecond)})
	if lag, ok := f.Lag(); ok {
		t.Fatalf("book: Lag() = %v, true; want no lag", lag)
	}

	// No timestamp, no lag (priceChange builds one without).
	f.HandleQueued(Queued{Raw: priceChange("0.50", "10"), Recv: time.Now()})
	if lag, ok := f.Lag(); ok {
		t.Fatalf("no timestamp: Lag() = %v, true; want no lag", lag)
	}

	// An array of events is one message: the lag is measured against the
	// newest timestamp in it.
	arr := []byte(`[{"event_type":"tick_size_change","timestamp":"1000"},{"event_type":"tick_size_change","timestamp":"3000"},{"event_type":"tick_size_change","timestamp":"2000"}]`)
	f.HandleQueued(Queued{Raw: arr, Recv: time.UnixMilli(3500)})
	if lag, ok := f.Lag(); !ok || lag != 500*time.Millisecond {
		t.Fatalf("array: Lag() = %v, %v; want 500ms, true", lag, ok)
	}

	// No receive time, no lag.
	f.HandleQueued(Queued{Raw: first("price_change")})
	if lag, ok := f.Lag(); ok {
		t.Fatalf("no Recv: Lag() = %v, true; want no lag", lag)
	}

	// Through the queue, Offer stamps the receive time.
	q := NewQueue(4)
	raw := first("price_change")
	before := time.Now()
	q.Offer(raw)
	after := time.Now()
	nextWithin(t, f, q, time.Second)
	lag, ok := f.Lag()
	ts := tsOf(raw)
	if !ok || lag < before.Sub(ts) || lag > after.Sub(ts) {
		t.Fatalf("via Next: Lag() = %v, %v; want between %v and %v", lag, ok, before.Sub(ts), after.Sub(ts))
	}

	// A Next step that handles no message clears it.
	q.Close()
	nextWithin(t, f, q, time.Second)
	if lag, ok := f.Lag(); ok {
		t.Fatalf("after a step with no message: Lag() = %v, true; want no lag", lag)
	}
}

// TestFeedStampAndSizes: unlike Lag, Stamp's venue time includes book
// events (their timestamp is when the book last changed, which is the
// right time for the state they produce). TopSizes gives the sizes behind
// Best.
func TestFeedStampAndSizes(t *testing.T) {
	book := firstBookEvent(t)
	var m struct {
		Timestamp string `json:"timestamp"`
	}
	json.Unmarshal(book, &m)
	bookMs, err := strconv.ParseInt(m.Timestamp, 10, 64)
	if err != nil {
		t.Fatal(err)
	}

	f := NewFeed(captureAssetID)
	recv := time.UnixMilli(bookMs).Add(5 * time.Second)
	f.HandleQueued(Queued{Raw: book, Recv: recv})
	if r, ms := f.Stamp(); !r.Equal(recv) || ms != bookMs {
		t.Fatalf("book: Stamp() = %v, %d; want %v, %d", r, ms, recv, bookMs)
	}
	if lag, ok := f.Lag(); ok {
		t.Fatalf("book: Lag() = %v, true; want none", lag)
	}

	bid, ask, ok := f.Best()
	bidSz, askSz := f.TopSizes()
	wantBid, _ := f.book.BidSize(bid)
	wantAsk, _ := f.book.AskSize(ask)
	if !ok || bidSz != wantBid || askSz != wantAsk || bidSz == 0 || askSz == 0 {
		t.Fatalf("TopSizes() = %d/%d at %d/%d, want %d/%d (nonzero)", bidSz, askSz, bid, ask, wantBid, wantAsk)
	}

	f.HandleQueued(Queued{Raw: priceChange("0.50", "10")}) // no timestamp, no Recv
	if r, ms := f.Stamp(); !r.IsZero() || ms != 0 {
		t.Fatalf("no timestamp: Stamp() = %v, %d; want zero", r, ms)
	}
}
