package polymarket

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func priceChange(price, size string) []byte {
	return []byte(fmt.Sprintf(
		`{"price_changes":[{"asset_id":%q,"price":%q,"size":%q,"side":"BUY"}],"event_type":"price_change"}`,
		captureAssetID, price, size))
}

func firstBookEvent(t *testing.T) []byte {
	t.Helper()
	for _, line := range captureLines(t) {
		if eventType(t, line) == "book" {
			return line
		}
	}
	t.Fatalf("no book event in capture")
	return nil
}

// TestNextIdleDropMarksStale covers a drop followed by silence: a burst
// fills the queue, the next message is dropped, and then nothing else
// arrives. Without an idle check the drop would only be noticed when a
// later message got through, so the book would look valid indefinitely.
func TestNextIdleDropMarksStale(t *testing.T) {
	book := firstBookEvent(t)
	q := NewQueue(4)
	f := NewFeed(captureAssetID)

	// The burst: four messages fill the queue, the fifth is dropped. The
	// dropped one would have moved the best bid, so a book left looking
	// valid would be quoting the wrong top.
	burst := [][]byte{book, priceChange("0.02", "1"), priceChange("0.03", "1"), priceChange("0.04", "1")}
	for i, raw := range burst {
		if !q.Offer(raw) {
			t.Fatalf("burst message %d dropped, want it queued", i)
		}
	}
	if q.Offer(priceChange("0.815", "10")) {
		t.Fatalf("Offer on a full queue succeeded, want a drop")
	}

	// Everything queued before the drop applies normally.
	for i := range burst {
		r := nextWithin(t, f, q, time.Second)
		if r.err != nil || !r.ok || f.Stale() {
			t.Fatalf("pre-drop message %d: ok=%v err=%v stale=%v, want true, nil, false", i, r.ok, r.err, f.Stale())
		}
	}

	// Silence. The queue is empty, so Next must notice the drop on its
	// own rather than block waiting for a message that isn't coming.
	r := nextWithin(t, f, q, time.Second)
	if !r.ok || r.err == nil || !strings.Contains(r.err.Error(), "dropped") {
		t.Fatalf("idle Next: ok=%v err=%v, want true and a drop report", r.ok, r.err)
	}
	if !f.Stale() {
		t.Fatalf("idle Next: book not stale after a drop")
	}
	if !r.changed {
		t.Fatalf("idle Next: changed = false, want true (best goes from a quote to none)")
	}
	if _, _, ok := f.Best(); ok {
		t.Fatalf("idle Next: Best() ok = true on a stale book")
	}

	// With no new drops, Next goes back to blocking until a message
	// arrives, and the drop isn't reported twice.
	done := make(chan nextResult, 1)
	go func() {
		changed, ok, err := f.Next(q)
		done <- nextResult{changed, ok, err}
	}()
	select {
	case r := <-done:
		t.Fatalf("Next returned (%+v) with nothing queued and no new drops, want it to block", r)
	case <-time.After(50 * time.Millisecond):
	}
	q.Offer(book)
	select {
	case r := <-done:
		if r.err != nil || f.Stale() {
			t.Fatalf("fresh book event: err=%v stale=%v, want nil, false", r.err, f.Stale())
		}
	case <-time.After(time.Second):
		t.Fatalf("Next did not return after a message was queued")
	}
}

// TestNextDropAtClose covers a drop right before the reader shuts down:
// the queue closes with nothing after the drop, and the book must still
// end up stale.
func TestNextDropAtClose(t *testing.T) {
	q := NewQueue(1)
	f := NewFeed(captureAssetID)
	q.Offer(firstBookEvent(t))
	q.Offer(priceChange("0.815", "10")) // full: dropped
	q.Close()

	if r := nextWithin(t, f, q, time.Second); !r.ok || r.err != nil || f.Stale() {
		t.Fatalf("book event: ok=%v err=%v stale=%v, want true, nil, false", r.ok, r.err, f.Stale())
	}
	r := nextWithin(t, f, q, time.Second)
	if r.ok || r.err == nil || !strings.Contains(r.err.Error(), "dropped") {
		t.Fatalf("closed queue: ok=%v err=%v, want false and a drop report", r.ok, r.err)
	}
	if !f.Stale() {
		t.Fatalf("closed queue: book not stale after a final drop")
	}
}

// TestNextConcurrentReaderAndBook runs a real reader goroutine against a
// real book goroutine through queues of several sizes, so whether and
// where drops happen is up to the scheduler. Whatever happens, the book
// must never claim to be valid while wrong: if it ends up not stale (a
// book event got through after the last drop), it must match a clean
// replay. Run under -race in CI.
func TestNextConcurrentReaderAndBook(t *testing.T) {
	lines := captureLines(t)

	clean := NewFeed(captureAssetID)
	for _, line := range lines {
		if _, err := clean.Handle(line); err != nil {
			t.Fatalf("clean replay: %v", err)
		}
	}
	cleanBid, cleanAsk, _ := clean.Best()

	var runsWithDrops, staleEnds int
	// From a queue that drops constantly to one that holds the whole
	// capture and never drops, so both outcomes are checked.
	sizes := []int{1, 4, 32, len(lines)}
	for run := 0; run < 50; run++ {
		q := NewQueue(sizes[run%len(sizes)])
		f := NewFeed(captureAssetID)
		done := make(chan struct{})
		go func() {
			defer close(done)
			for {
				if _, more, _ := f.Next(q); !more {
					return
				}
			}
		}()

		for _, line := range lines {
			q.Offer(line)
		}
		q.Close()
		<-done

		if q.Drops() > 0 {
			runsWithDrops++
		} else if f.Stale() {
			t.Fatalf("run %d: no drops but the book is stale", run)
		}
		if f.Stale() {
			staleEnds++
			continue
		}
		if bid, ask, _ := f.Best(); bid != cleanBid || ask != cleanAsk {
			t.Fatalf("run %d: book claims valid with best (%d, %d), clean replay gives (%d, %d)",
				run, bid, ask, cleanBid, cleanAsk)
		}
	}
	t.Logf("%d of 50 runs had drops, %d ended stale", runsWithDrops, staleEnds)
}

type nextResult struct {
	changed, ok bool
	err         error
}

// nextWithin runs f.Next on its own goroutine and fails the test if it
// doesn't return within d.
func nextWithin(t *testing.T, f *Feed, q *Queue, d time.Duration) nextResult {
	t.Helper()
	done := make(chan nextResult, 1)
	go func() {
		changed, ok, err := f.Next(q)
		done <- nextResult{changed, ok, err}
	}()
	select {
	case r := <-done:
		return r
	case <-time.After(d):
		t.Fatalf("Next did not return within %v", d)
		return nextResult{}
	}
}
