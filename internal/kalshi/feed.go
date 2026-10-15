package kalshi

import (
	"encoding/json"
	"errors"
	"fmt"
)

// Feed drives a Book from the raw messages on the orderbook_delta channel:
// it dispatches each message by type, tracks whether the book can be
// trusted, and reports when the top of book changes.
//
// The book is untrusted ("stale") until the first orderbook_snapshot, and
// becomes stale again the moment any delta fails to apply — a seq gap, or
// a delta that can't be parsed or would drive a level negative, or the
// reader dropping a message because the queue was full (HandleQueued).
// Either way the book no longer matches Kalshi's, so every later delta is
// ignored rather than applied on top of a wrong base. Only a new snapshot
// clears staleness.
type Feed struct {
	book   Book
	synced bool

	// drops is the reader's drop count as of the last queued message
	// handled; see HandleQueued.
	drops uint64

	// Top of book as of the last time Handle reported a change.
	bidBps, askBps int
	bestOK         bool
}

// Handle processes one raw channel message. changed reports whether the
// top of book (as returned by Best) differs from what it was before this
// message. A non-nil err means the message was rejected; if the book was
// trusted before, it is now stale (see Stale).
func (f *Feed) Handle(raw []byte) (changed bool, err error) {
	var probe struct {
		Type string          `json:"type"`
		Msg  json.RawMessage `json:"msg"`
	}
	if err := json.Unmarshal(raw, &probe); err != nil {
		return false, fmt.Errorf("kalshi: decoding message type: %w", err)
	}

	switch probe.Type {
	case "orderbook_snapshot":
		if err := f.book.ApplySnapshot(raw); err != nil {
			f.synced = false
			return f.updateBest(), err
		}
		f.synced = true
	case "orderbook_delta":
		if !f.synced {
			// Before the first snapshot there's nothing to apply onto;
			// after a gap, applying would build on a wrong base.
			return f.updateBest(), nil
		}
		if err := f.book.ApplyDelta(raw); err != nil {
			f.synced = false
			return f.updateBest(), err
		}
	case "error":
		return false, fmt.Errorf("kalshi: venue error: %s", probe.Msg)
	default:
		// "subscribed" acks and anything else carry no book state.
	}
	return f.updateBest(), nil
}

// HandleQueued processes one message handed over by the reader goroutine.
// If m.Drops shows the reader dropped messages since the last one handled,
// the book is marked stale first, then m is handled as usual: a delta is
// ignored, a snapshot rebuilds the book (it supersedes whatever was
// dropped before it).
//
// Only the book goroutine touches the Feed, so the drop is detected by
// comparing counts carried in-band on the messages themselves, not by
// reading shared state.
func (f *Feed) HandleQueued(m Queued) (changed bool, err error) {
	var dropErr error
	if m.Drops > f.drops {
		dropErr = fmt.Errorf("kalshi: reader queue full, %d message(s) dropped (book is stale, resync required)",
			m.Drops-f.drops)
		f.drops = m.Drops
		f.synced = false
	}
	changed, err = f.Handle(m.Raw)
	return changed, errors.Join(dropErr, err)
}

// Best returns the top of book in basis points. ok is false while the book
// is stale or either side is empty.
func (f *Feed) Best() (bidBps, askBps int, ok bool) {
	return f.bidBps, f.askBps, f.bestOK
}

// Stale reports whether the book is untrusted: no snapshot yet, or a delta
// has failed to apply or been dropped since the last snapshot.
func (f *Feed) Stale() bool {
	return !f.synced
}

// Book returns the underlying book, for inspection.
func (f *Feed) Book() *Book {
	return &f.book
}

// updateBest recomputes the top of book and reports whether it changed.
// A stale book has no trustworthy best, so it reports ok=false.
func (f *Feed) updateBest() bool {
	var bid, ask int
	var ok bool
	if f.synced {
		bid, ask, ok = f.book.BestYes()
	}
	if !ok {
		bid, ask = 0, 0
	}
	if bid == f.bidBps && ask == f.askBps && ok == f.bestOK {
		return false
	}
	f.bidBps, f.askBps, f.bestOK = bid, ask, ok
	return true
}
