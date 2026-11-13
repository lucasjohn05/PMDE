package kalshi

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// MultiFeed drives the Books of several markets from one orderbook_delta
// subscription. It is the multi-market counterpart of Feed, built
// alongside it for the open scanner; Feed and Client are unchanged.
//
// Kalshi puts every market named in a subscribe on one subscription
// (one sid), and numbers every message on it with one seq: the
// snapshots first, one per market, then the deltas of all the markets
// interleaved. (Observed in November 2026; the docs say only that seq
// "counts messages on one subscription".) So a gap in seq cannot be
// pinned on a market: the missing message could have been for any of
// them. One gap, one drop by the reader, or one message that fails to
// apply therefore breaks every book on the connection at once, and
// they stay untrusted until the connection is replaced and each market
// gets a fresh snapshot.
type MultiFeed struct {
	books map[string]*Book // every subscribed market, by ticker
	have  map[string]bool  // a snapshot has been applied this session

	lastSeq int64
	started bool // a seq has been seen

	broken bool
	reason string // why, a Reason constant

	drops uint64 // the reader's drop count as of the last message handled
}

// NewMultiFeed returns a MultiFeed for a subscription to tickers.
func NewMultiFeed(tickers []string) *MultiFeed {
	f := &MultiFeed{books: make(map[string]*Book, len(tickers)), have: make(map[string]bool, len(tickers))}
	for _, t := range tickers {
		f.books[t] = &Book{}
	}
	return f
}

// Broken reports whether the subscription's books are lost (see the
// type's comment), and why. Once broken, a MultiFeed stays broken.
func (f *MultiFeed) Broken() (broken bool, reason string) {
	return f.broken, f.reason
}

// Synced reports whether every market has its snapshot and nothing has
// broken since.
func (f *MultiFeed) Synced() bool {
	return !f.broken && len(f.have) == len(f.books)
}

// AnySynced reports whether at least one market has had a snapshot this
// session: whether there was any trusted book to lose.
func (f *MultiFeed) AnySynced() bool {
	return len(f.have) > 0
}

// Book returns ticker's book, nil if it is not on the subscription.
func (f *MultiFeed) Book(ticker string) *Book {
	return f.books[ticker]
}

func (f *MultiFeed) fail(reason string) {
	if !f.broken {
		f.broken, f.reason = true, reason
	}
}

// HandleQueued processes one message from the reader. ok is true if it
// was an orderbook snapshot or delta, and ev then describes it and the
// book of its market afterwards; ev.Stale is set if that book is not
// trusted (the subscription is broken, or the market has no snapshot
// yet). A non-nil err means the message was rejected or reports drops.
func (f *MultiFeed) HandleQueued(m Queued) (ev BookEvent, ok bool, err error) {
	dropErr := f.noteDrops(m.Drops)
	ev, ok, err = f.handle(m.Raw)
	if ok {
		ev.Recv = m.Recv
	}
	return ev, ok, errors.Join(dropErr, err)
}

func (f *MultiFeed) handle(raw []byte) (ev BookEvent, ok bool, err error) {
	var env envelope
	if err := json.Unmarshal(raw, &env); err != nil {
		f.fail(ReasonInvalid)
		return BookEvent{}, false, fmt.Errorf("kalshi: decoding message: %w", err)
	}
	if env.Type == "error" {
		return BookEvent{}, false, fmt.Errorf("kalshi: venue error: %s", env.Msg)
	}
	if ev, ok = decodeBookEvent(env); !ok {
		return BookEvent{}, false, nil // "subscribed" acks and the like
	}

	// One seq runs across every market: check it before anything else.
	if f.started && env.Seq != f.lastSeq+1 && !f.broken {
		f.fail(ReasonSeqGap)
		err = fmt.Errorf("kalshi: %w on a %d-market subscription: have seq %d, got seq %d (every book on it is stale, resync required)",
			ErrSeqGap, len(f.books), f.lastSeq, env.Seq)
	}
	f.started, f.lastSeq = true, env.Seq

	book := f.books[ev.Market]
	switch {
	case f.broken:
		// Nothing is applied on top of a broken subscription.
	case book == nil:
		f.fail(ReasonInvalid)
		err = fmt.Errorf("kalshi: %s for %q, which is not on this subscription", env.Type, ev.Market)
	case ev.Type == EventSnapshot:
		if aerr := book.ApplySnapshot(raw); aerr != nil {
			f.fail(ReasonInvalid)
			err = aerr
		} else {
			f.have[ev.Market] = true
		}
	case !f.have[ev.Market]:
		// A delta for a market whose snapshot hasn't come: nothing to
		// apply it to. The market reads as unsynced until it does.
	default:
		if aerr := book.applyDelta(env); aerr != nil {
			f.fail(ReasonInvalid)
			err = aerr
		}
	}

	if !f.broken && book != nil && f.have[ev.Market] {
		ev.setTop(book)
	} else {
		ev.Stale, ev.StaleReason = true, f.reason
	}
	return ev, true, err
}

// Next is one step of the book goroutine's loop: it handles the next
// queued message, blocking until one arrives. got is true if ev
// describes a book message; more is false once the reader has closed
// the queue and it has drained.
//
// The drop accounting is Feed.Next's, for the same reasons (see there):
// the drop total is read before looking at the queue, and an empty
// queue with new drops breaks the subscription on the spot, so a feed
// that goes quiet right after a drop is not left looking valid.
func (f *MultiFeed) Next(q *Queue) (ev BookEvent, got, more bool, err error) {
	total := q.Drops()
	select {
	case m, open := <-q.Messages():
		if !open {
			return BookEvent{}, false, false, f.noteDrops(q.Drops())
		}
		ev, got, err = f.HandleQueued(m)
		return ev, got, true, err
	default:
	}
	if err := f.noteDrops(total); err != nil {
		return BookEvent{}, false, true, err
	}
	m, open := <-q.Messages()
	if !open {
		return BookEvent{}, false, false, f.noteDrops(q.Drops())
	}
	ev, got, err = f.HandleQueued(m)
	return ev, got, true, err
}

func (f *MultiFeed) noteDrops(total uint64) error {
	if total <= f.drops {
		return nil
	}
	err := fmt.Errorf("kalshi: reader queue full, %d message(s) dropped (every book on the subscription is stale, resync required)",
		total-f.drops)
	f.drops = total
	f.fail(ReasonDropped)
	return err
}

// staleEvent is the EventStale for one market of a subscription.
func staleEvent(market, reason string) BookEvent {
	return BookEvent{Type: EventStale, Market: market, Recv: time.Now(), Stale: true, StaleReason: reason}
}
