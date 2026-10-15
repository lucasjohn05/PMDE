package polymarket

import "sync/atomic"

// QueueSize is the capacity of the channel between the websocket reader
// and the book goroutine.
//
// The book goroutine is not the bottleneck: Feed.Handle costs ~9µs per
// message over the capture (~100k msgs/s), while the busiest stretch of
// testdata/polymarket_capture.jsonl is 19 messages in one second. So the
// queue is not there to absorb a consumer that is steadily slower than the
// feed; it is there to ride out the book goroutine briefly stalling (a GC
// pause, the scheduler, a log write to a slow terminal). 1024 slots covers
// close to a minute of the captured peak rate with the consumer completely
// stopped, or ~0.5s at a hundred times that rate. Captured messages are at
// most ~3.4KB, so a full queue holds on the order of 3.5MB.
//
// Bigger isn't safer past that point. A consumer stalled long enough to
// fill this queue is serving a book seconds out of date; dropping, going
// stale and resyncing from a fresh book event gets back to a current book
// sooner than draining a deep backlog of old updates would.
//
// (Queue mirrors internal/kalshi's Queue. Duplicated rather than shared,
// per CLAUDE.md's no-venue-abstraction rule, as parseFixedPoint is.)
const QueueSize = 1024

// Queued is one raw message on its way from the reader to the book
// goroutine.
type Queued struct {
	Raw []byte

	// Drops is how many messages the reader had dropped, in total, when
	// it enqueued this one. If it is higher than the previous message's,
	// at least one message was lost between the two — so the drop is
	// reported in-band, at exactly the point in the stream where it
	// happened.
	Drops uint64
}

// Queue is the bounded hand-off from the websocket reader to the book
// goroutine. The reader never blocks on it: when it is full, the message
// is dropped and counted, and the next message that does get through
// carries the new count so the book goroutine marks the book stale
// before applying it (see Feed.HandleQueued).
//
// Offer and Close must only be called from the reader goroutine.
type Queue struct {
	ch    chan Queued
	drops atomic.Uint64
}

// NewQueue returns a Queue with room for size messages.
func NewQueue(size int) *Queue {
	return &Queue{ch: make(chan Queued, size)}
}

// Offer enqueues raw without blocking. If the queue is full, raw is
// dropped, the drop is counted, and Offer returns false.
//
// raw must not be modified afterwards; gorilla/websocket's ReadMessage
// returns a fresh slice per message, so passing it straight through is
// safe.
func (q *Queue) Offer(raw []byte) bool {
	select {
	case q.ch <- Queued{Raw: raw, Drops: q.drops.Load()}:
		return true
	default:
		q.drops.Add(1)
		return false
	}
}

// Close signals that the reader is done; the book goroutine's range over
// Messages ends once the queue drains.
func (q *Queue) Close() {
	close(q.ch)
}

// Messages is the receiving end, for the book goroutine.
func (q *Queue) Messages() <-chan Queued {
	return q.ch
}

// Drops returns the total number of messages dropped so far. Safe to call
// from any goroutine.
func (q *Queue) Drops() uint64 {
	return q.drops.Load()
}
