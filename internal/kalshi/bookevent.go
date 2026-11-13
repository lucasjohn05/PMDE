package kalshi

import (
	"cmp"
	"encoding/json"
	"slices"
	"time"
)

// BookEvent types.
const (
	EventSnapshot = "snapshot" // an orderbook_snapshot
	EventDelta    = "delta"    // an orderbook_delta
	EventStale    = "stale"    // the book was lost with no message behind it
)

// Level is one resting price level: a price in basis points and the
// size there in hundredths of a contract.
type Level struct {
	PriceBps  int
	SizeCenti int64
}

// BookEvent is one book message as the Feed handled it, with the top of
// book it left behind. Client.OnEvent receives one per orderbook
// snapshot and delta, applied or not, and an EventStale one when the
// book is lost without a message: a drop noticed while the feed was
// idle, or the connection going away.
type BookEvent struct {
	Type string // EventSnapshot, EventDelta or EventStale

	Seq     int64     // the message's seq; 0 for EventStale
	Recv    time.Time // when the reader read it off the socket
	VenueMs int64     // the venue's ts_ms; 0 if the message had none (snapshots)

	// For EventDelta: the level changed, and by how much.
	Side       string // "yes" or "no"
	PriceBps   int
	DeltaCenti int64

	// For EventSnapshot: the whole book, each side sorted by price. Both
	// are empty for a market that has not opened yet.
	Yes, No []Level

	// The book after this event. Each side is reported on its own, so a
	// book with only bids or only asks is visible; the ask is the yes
	// ask, 10000 minus the best no bid. All zero while Stale.
	BidBps, AskBps   int
	BidSize, AskSize int64
	HasBid, HasAsk   bool

	// Stale is true if the book is untrusted after this event, and
	// StaleReason then says what broke it (a Reason constant), or is ""
	// if it has not synced yet.
	Stale       bool
	StaleReason string
}

// Event describes the message the last HandleQueued or Next call
// handled. ok is false if that call handled no message, or one that was
// not an orderbook snapshot or delta.
func (f *Feed) Event() (ev BookEvent, ok bool) {
	if f.raw == nil {
		return BookEvent{}, false
	}
	var env envelope
	if json.Unmarshal(f.raw, &env) != nil {
		return BookEvent{}, false
	}
	switch env.Type {
	case "orderbook_snapshot":
		ev.Type = EventSnapshot
		var msg snapshotMsg
		if json.Unmarshal(env.Msg, &msg) == nil {
			ev.Yes, ev.No = sortedLevels(msg.YesDollarsFP), sortedLevels(msg.NoDollarsFP)
		}
	case "orderbook_delta":
		ev.Type = EventDelta
		var msg deltaMsg
		if json.Unmarshal(env.Msg, &msg) == nil {
			ev.Side = msg.Side
			if p, ok := parseBps(msg.PriceDollars); ok {
				ev.PriceBps = p
			}
			ev.DeltaCenti, _ = parseFixedPoint(msg.DeltaFP, 2)
		}
	default:
		return BookEvent{}, false
	}
	ev.Seq, ev.Recv, ev.VenueMs = env.Seq, f.recv, f.stampMs
	if f.synced {
		ev.BidBps, ev.BidSize, ev.HasBid = f.book.BestBid()
		ev.AskBps, ev.AskSize, ev.HasAsk = f.book.BestAsk()
	} else {
		ev.Stale, ev.StaleReason = true, f.reason
	}
	return ev, true
}

// sortedLevels parses a snapshot side into levels sorted by price. A
// side that does not parse comes back empty: the Feed has already
// rejected the snapshot and marked the book stale.
func sortedLevels(pairs [][]string) []Level {
	m, err := parseLevels(pairs)
	if err != nil || len(m) == 0 {
		return nil
	}
	levels := make([]Level, 0, len(m))
	for p, size := range m {
		levels = append(levels, Level{PriceBps: p, SizeCenti: size})
	}
	slices.SortFunc(levels, func(a, b Level) int { return cmp.Compare(a.PriceBps, b.PriceBps) })
	return levels
}
