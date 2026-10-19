package polymarket

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"
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
	reason string // see StaleReason

	// drops is the reader's drop count as of the last queued message
	// handled; see HandleQueued.
	drops uint64

	// venueMs is the venue's timestamp (Unix ms) on the message Handle
	// last processed (the latest, if it framed several events), or 0 if
	// it had none. lag/hasLag hold the ingest lag
	// of the message the last HandleQueued or Next call handled; see Lag.
	venueMs int64
	lag     time.Duration
	hasLag  bool

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
	f.venueMs = 0
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
	dropErr := f.noteDrops(m.Drops)
	changed, err = f.Handle(m.Raw)
	f.lag, f.hasLag = 0, false
	if f.venueMs > 0 && !m.Recv.IsZero() {
		f.lag, f.hasLag = m.Recv.Sub(time.UnixMilli(f.venueMs)), true
	}
	return changed, errors.Join(dropErr, err)
}

// Lag reports the ingest lag of the message the last HandleQueued or Next
// call handled: when the reader received it minus the venue's timestamp
// on it. ok is false if that call handled no message, or the message had
// no timestamp. The lag includes any clock skew between the venue and
// this host, so it can come out negative.
//
// Every message with a timestamp counts, applied to the book or not: lag
// measures the transport, not the book.
func (f *Feed) Lag() (lag time.Duration, ok bool) {
	return f.lag, f.hasLag
}

// Next is one step of the book goroutine's loop: it handles the next
// queued message, blocking until one arrives. ok is false once the reader
// has closed the queue and it has drained; any drops not yet accounted for
// still mark the book stale then, since the reader is done offering.
//
// Before blocking on an empty queue, Next checks whether the reader has
// dropped anything since the last message handled. Otherwise a drop is
// only noticed when the next message gets through, so a feed that goes
// quiet right after one would leave the book looking valid while missing
// an update. An empty queue means everything enqueued before the drop has
// already been applied, so the book can be marked stale right there.
//
// The drop total is read before checking whether the queue is empty, not
// after. Read after, the queue could fill up and drop in between, and the
// book would be marked stale while messages from before that drop were
// still queued. If one of those were a snapshot it would clear the mark,
// and the drop after it would never be noticed: the message following it
// carries a count the book has already seen.
func (f *Feed) Next(q *Queue) (changed, ok bool, err error) {
	f.lag, f.hasLag = 0, false
	total := q.Drops()
	select {
	case m, open := <-q.Messages():
		if !open {
			return f.closed(q)
		}
		changed, err = f.HandleQueued(m)
		return changed, true, err
	default:
	}

	if err := f.noteDrops(total); err != nil {
		return f.updateBest(), true, err
	}

	m, open := <-q.Messages()
	if !open {
		return f.closed(q)
	}
	changed, err = f.HandleQueued(m)
	return changed, true, err
}

// closed accounts for any final drops once the queue is closed and
// drained. The reader's last Offer happened before Close, so the total is
// final by now.
func (f *Feed) closed(q *Queue) (changed, ok bool, err error) {
	err = f.noteDrops(q.Drops())
	return f.updateBest(), false, err
}

// noteDrops marks the book stale if the reader's drop total has risen past
// what this Feed has already accounted for, and reports the drop.
func (f *Feed) noteDrops(total uint64) error {
	if total <= f.drops {
		return nil
	}
	err := fmt.Errorf("polymarket: reader queue full, %d message(s) dropped (book is stale, resync required)",
		total-f.drops)
	f.drops = total
	f.markStale(ReasonDropped)
	return err
}

func (f *Feed) handleEvent(raw []byte) error {
	var probe struct {
		EventType string `json:"event_type"`
		AssetID   string `json:"asset_id"`
		Timestamp string `json:"timestamp"` // Unix ms, as a string
	}
	if err := json.Unmarshal(raw, &probe); err != nil {
		return fmt.Errorf("polymarket: decoding event_type: %w", err)
	}
	// A book event's timestamp is when the book last changed, not when
	// the event was sent: the snapshot on subscribe can be seconds or
	// minutes old on a quiet market. That measures the market, not our
	// ingest, so book events don't count toward lag.
	if probe.EventType != "book" {
		if ms, err := strconv.ParseInt(probe.Timestamp, 10, 64); err == nil && ms > f.venueMs {
			f.venueMs = ms
		}
	}

	switch probe.EventType {
	case "book":
		if probe.AssetID != f.book.AssetID {
			return nil
		}
		if err := f.book.ApplySnapshot(raw); err != nil {
			f.markStale(ReasonInvalid)
			return err
		}
		f.synced = true
		f.reason = ""
	case "price_change":
		if !f.synced {
			return nil
		}
		if err := f.book.ApplyPriceChange(raw); err != nil {
			f.markStale(ReasonInvalid)
			return err
		}
	default:
		// last_trade_price, tick_size_change and the like carry no
		// book-level state.
	}
	return nil
}

// Reasons a trusted book goes stale, as reported by StaleReason. There's
// no seq gap: Polymarket has no sequence numbers.
const (
	ReasonDropped = "dropped"        // the reader dropped a message (queue full)
	ReasonInvalid = "invalid_update" // a message couldn't be parsed or applied
)

// markStale marks the book untrusted. The reason is kept only for the
// transition from trusted, so it names what broke the book, not whatever
// else went wrong after.
func (f *Feed) markStale(reason string) {
	if f.synced {
		f.reason = reason
	}
	f.synced = false
}

// StaleReason reports why the book last went from trusted to stale (one
// of the Reason constants), or "" if it is trusted or has never been.
func (f *Feed) StaleReason() string {
	return f.reason
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
