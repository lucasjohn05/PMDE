package polymarket

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
)

// Feed drives a Book from the raw messages on the CLOB market channel: it
// dispatches each event by event_type, tracks whether the book can be
// trusted, and reports when the top of book changes.
//
// Polymarket has no sequence numbers, so there is no gap to detect. The
// book is untrusted until the first "book" event; a price_change that
// arrives before then is ignored, since applying absolute sizes to an
// empty book would produce a book with only the levels that happened to
// change. A price_change that fails to apply, or a message the reader
// dropped because the queue was full (HandleQueued), also leaves the book
// untrusted until the next "book" event replaces it wholesale.
type Feed struct {
	book   *Book
	synced bool

	// drops is the reader's drop count as of the last queued message
	// handled; see HandleQueued.
	drops uint64

	// Top of book as of the last time Handle reported a change.
	bidBps, askBps int
	bestOK         bool
}

// NewFeed returns a Feed tracking assetID.
func NewFeed(assetID string) *Feed {
	return &Feed{book: NewBook(assetID)}
}

// Handle processes one raw channel message. changed reports whether the
// top of book (as returned by Best) differs from what it was before this
// message. A non-nil err means an event was rejected.
//
// The channel sometimes frames events as a JSON array (the initial book
// snapshot on subscribe, in particular) and sometimes as a bare object;
// Handle accepts both.
func (f *Feed) Handle(raw []byte) (changed bool, err error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) > 0 && trimmed[0] == '[' {
		var events []json.RawMessage
		if err := json.Unmarshal(trimmed, &events); err != nil {
			return false, fmt.Errorf("polymarket: decoding event array: %w", err)
		}
		for _, ev := range events {
			if err := f.handleEvent(ev); err != nil {
				return f.updateBest(), err
			}
		}
		return f.updateBest(), nil
	}
	if err := f.handleEvent(trimmed); err != nil {
		return f.updateBest(), err
	}
	return f.updateBest(), nil
}

// HandleQueued processes one message handed over by the reader goroutine.
// If m.Drops shows the reader dropped messages since the last one handled,
// the book is marked stale first, then m is handled as usual: a
// price_change is ignored, a book event rebuilds the book (it supersedes
// whatever was dropped before it).
//
// Only the book goroutine touches the Feed, so the drop is detected by
// comparing counts carried in-band on the messages themselves, not by
// reading shared state. That matters more here than for Kalshi: with no
// sequence numbers, nothing downstream would catch a missed price_change
// if the stale mark landed at the wrong point in the stream.
func (f *Feed) HandleQueued(m Queued) (changed bool, err error) {
	var dropErr error
	if m.Drops > f.drops {
		dropErr = fmt.Errorf("polymarket: reader queue full, %d message(s) dropped (book is stale, resync required)",
			m.Drops-f.drops)
		f.drops = m.Drops
		f.synced = false
	}
	changed, err = f.Handle(m.Raw)
	return changed, errors.Join(dropErr, err)
}

func (f *Feed) handleEvent(raw []byte) error {
	var probe struct {
		EventType string `json:"event_type"`
		AssetID   string `json:"asset_id"`
	}
	if err := json.Unmarshal(raw, &probe); err != nil {
		return fmt.Errorf("polymarket: decoding event_type: %w", err)
	}

	switch probe.EventType {
	case "book":
		if probe.AssetID != f.book.AssetID {
			return nil
		}
		if err := f.book.ApplySnapshot(raw); err != nil {
			f.synced = false
			return err
		}
		f.synced = true
	case "price_change":
		if !f.synced {
			return nil
		}
		if err := f.book.ApplyPriceChange(raw); err != nil {
			f.synced = false
			return err
		}
	default:
		// last_trade_price, tick_size_change and the like carry no
		// book-level state.
	}
	return nil
}

// Best returns the top of book in basis points. ok is false while the book
// is untrusted or either side is empty.
func (f *Feed) Best() (bidBps, askBps int, ok bool) {
	return f.bidBps, f.askBps, f.bestOK
}

// Stale reports whether the book is untrusted: no book event yet, or a
// price_change has failed to apply or been dropped since the last one.
func (f *Feed) Stale() bool {
	return !f.synced
}

// Book returns the underlying book, for inspection.
func (f *Feed) Book() *Book {
	return f.book
}

// updateBest recomputes the top of book and reports whether it changed.
func (f *Feed) updateBest() bool {
	var bid, ask int
	var ok bool
	if f.synced {
		bid, ask, ok = f.book.Best()
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
