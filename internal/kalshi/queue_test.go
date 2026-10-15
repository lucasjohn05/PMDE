package kalshi

import (
	"strings"
	"testing"
)

// TestQueueDropMarksBookStale replays the capture through a tiny queue
// with a consumer that drains one message for every three the reader
// offers, so the reader is forced to drop. The book must go stale at the
// first message that arrives after a drop, and stay stale: the capture's
// only snapshot is its second line, long before any drop.
func TestQueueDropMarksBookStale(t *testing.T) {
	lines := captureLines(t)
	q := NewQueue(2)
	var f Feed

	var seenDrops uint64
	var handled, dropErrs int
	drain := func() {
		m := <-q.Messages()
		handled++
		_, err := f.HandleQueued(m)
		if err != nil {
			// Deltas after a drop are ignored because the book is already
			// stale, so they never reach the seq check; a seq-gap error
			// here would mean the drop went unnoticed and seq caught it
			// instead, with the wrong reason.
			if !strings.Contains(err.Error(), "dropped") || strings.Contains(err.Error(), "sequence gap") {
				t.Fatalf("message %d: want a drop report only, got: %v", handled, err)
			}
			dropErrs++
		}
		if m.Drops > seenDrops {
			seenDrops = m.Drops
			if !f.Stale() {
				t.Fatalf("message %d arrived after %d drop(s) but the book is not stale", handled, m.Drops)
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
	if dropErrs == 0 {
		t.Fatalf("drops happened but no message reported one")
	}
	if !f.Stale() {
		t.Fatalf("book not stale at the end, despite drops and no snapshot after them")
	}
	t.Logf("offered %d, handled %d, dropped %d; %d drop report(s)", len(lines), handled, q.Drops(), dropErrs)
}

// TestQueueDropMarksStaleAtTheRightPoint checks that the stale mark lands
// at the drop's position in the stream, not whenever the book goroutine
// happens to notice. Messages already queued before the drop were
// received in order and are still valid, so they apply normally; the
// first message queued after the drop is where the book goes stale.
func TestQueueDropMarksStaleAtTheRightPoint(t *testing.T) {
	lines := captureLines(t)
	var snapshot []byte
	var deltas [][]byte
	for _, line := range lines {
		switch messageType(t, line) {
		case "orderbook_snapshot":
			snapshot = line
		case "orderbook_delta":
			deltas = append(deltas, line)
		}
	}

	q := NewQueue(2)
	var f Feed

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

	mustOffer(snapshot, true)
	mustOffer(deltas[0], true)
	mustOffer(deltas[1], false) // full: dropped

	// Both of these were queued before the drop; they apply normally.
	if err := handle(); err != nil || f.Stale() {
		t.Fatalf("snapshot: err=%v stale=%v, want nil, false", err, f.Stale())
	}
	if err := handle(); err != nil || f.Stale() {
		t.Fatalf("delta queued before the drop: err=%v stale=%v, want nil, false", err, f.Stale())
	}
	seqBefore := f.Book().LastSeq()

	// Queued after the drop: the book goes stale and the delta is not
	// applied.
	mustOffer(deltas[2], true)
	// Kalshi's seq check would also catch this delta as a gap; the point
	// is that the drop marks the book stale first, so it's reported as a
	// drop and never reaches the seq check.
	err := handle()
	if err == nil || !strings.Contains(err.Error(), "dropped") || strings.Contains(err.Error(), "sequence gap") {
		t.Fatalf("delta queued after the drop: err=%v, want a drop report only", err)
	}
	if !f.Stale() {
		t.Fatalf("delta queued after the drop: book not stale")
	}
	if got := f.Book().LastSeq(); got != seqBefore {
		t.Fatalf("LastSeq() = %d, want %d: the post-drop delta was applied", got, seqBefore)
	}

	// A fresh snapshot recovers.
	mustOffer(snapshot, true)
	if err := handle(); err != nil || f.Stale() {
		t.Fatalf("fresh snapshot: err=%v stale=%v, want nil, false", err, f.Stale())
	}
}
