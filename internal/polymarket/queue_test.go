package polymarket

import (
	"fmt"
	"strings"
	"testing"
)

// TestQueueDropMarksBookStale replays the capture through a tiny queue
// with a consumer that drains one message for every three the reader
// offers, so the reader is forced to drop. Whenever a message arrives
// after a drop, the book must go stale before that message is applied,
// unless the message is itself a book event, which replaces the book and
// so supersedes whatever was dropped.
func TestQueueDropMarksBookStale(t *testing.T) {
	lines := captureLines(t)
	q := NewQueue(2)
	f := NewFeed(captureAssetID)

	var seenDrops uint64
	var handled, dropErrs, staleMarks int
	drain := func() {
		m := <-q.Messages()
		handled++
		_, err := f.HandleQueued(m)
		if err != nil {
			if !strings.Contains(err.Error(), "dropped") {
				t.Fatalf("message %d: unexpected error: %v", handled, err)
			}
			dropErrs++
		}
		if m.Drops > seenDrops {
			seenDrops = m.Drops
			if eventType(t, m.Raw) != "book" {
				if !f.Stale() {
					t.Fatalf("message %d arrived after %d drop(s) but the book is not stale", handled, m.Drops)
				}
				staleMarks++
			}
		}
		if f.Stale() {
			if _, _, ok := f.Best(); ok {
				t.Fatalf("message %d: Best() ok = true on a stale book", handled)
			}
		}
	}

	for i, line := range lines {
		q.Offer(line) // never blocks; returns false on a drop
		if i%3 == 2 {
			drain()
		}
	}
	q.Close()
	for len(q.Messages()) > 0 {
		drain()
	}

	if q.Drops() == 0 {
		t.Fatalf("no drops with a queue of 2 and a consumer at a third of the offer rate")
	}
	if staleMarks == 0 {
		t.Fatalf("drops happened but the book never went stale")
	}
	t.Logf("offered %d, handled %d, dropped %d; %d drop report(s), %d stale mark(s)",
		len(lines), handled, q.Drops(), dropErrs, staleMarks)
}

// TestQueueDropMarksStaleAtTheRightPoint checks that the stale mark lands
// at the drop's position in the stream, not whenever the book goroutine
// happens to notice. Here a book event was queued *before* the drop: if
// the book goroutine learned of the drop from a shared flag instead, it
// could see the flag, then apply that earlier book event, conclude the
// book was resynced, and go on to apply post-drop price_changes onto a
// book missing an update. Polymarket has no seq to catch that.
func TestQueueDropMarksStaleAtTheRightPoint(t *testing.T) {
	var book []byte
	for _, line := range captureLines(t) {
		if eventType(t, line) == "book" {
			book = line
			break
		}
	}
	change := func(price, size string) []byte {
		return []byte(fmt.Sprintf(
			`{"price_changes":[{"asset_id":%q,"price":%q,"size":%q,"side":"BUY"}],"event_type":"price_change"}`,
			captureAssetID, price, size))
	}

	q := NewQueue(2)
	f := NewFeed(captureAssetID)

	mustOffer := func(raw []byte, want bool) {
		t.Helper()
		if got := q.Offer(raw); got != want {
			t.Fatalf("Offer = %v, want %v", got, want)
		}
	}
	handle := func() error {
		t.Helper()
		_, err := f.HandleQueued(<-q.Messages())
		return err
	}

	mustOffer(book, true)
	mustOffer(change("0.815", "10"), true)
	mustOffer(change("0.02", "1"), false) // full: dropped

	// Both of these were queued before the drop; they apply normally.
	if err := handle(); err != nil || f.Stale() {
		t.Fatalf("book event: err=%v stale=%v, want nil, false", err, f.Stale())
	}
	if err := handle(); err != nil || f.Stale() {
		t.Fatalf("price_change queued before the drop: err=%v stale=%v, want nil, false", err, f.Stale())
	}
	if bid, _, _ := f.Best(); bid != 8150 {
		t.Fatalf("best bid = %d after pre-drop price_change, want 8150", bid)
	}

	// Queued after the drop: the book goes stale and the update is not
	// applied.
	mustOffer(change("0.816", "5"), true)
	err := handle()
	if err == nil || !strings.Contains(err.Error(), "dropped") {
		t.Fatalf("price_change queued after the drop: err=%v, want a drop report", err)
	}
	if !f.Stale() {
		t.Fatalf("price_change queued after the drop: book not stale")
	}
	if got := f.StaleReason(); got != ReasonDropped {
		t.Fatalf("StaleReason() = %q, want %q", got, ReasonDropped)
	}
	if _, ok := f.Book().BidSize(8160); ok {
		t.Fatalf("post-drop price_change was applied to the book")
	}

	// A fresh book event recovers.
	mustOffer(book, true)
	if err := handle(); err != nil || f.Stale() {
		t.Fatalf("fresh book event: err=%v stale=%v, want nil, false", err, f.Stale())
	}
}
