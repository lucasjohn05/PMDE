// Package tick defines Tick, the one shape both venue clients report the
// top of book in. Everything downstream of the books (divergence,
// storage) works on Ticks and never sees venue-specific data.
package tick

import "time"

// Venue names, as used in Tick.Venue and the metrics' venue label.
const (
	Kalshi     = "kalshi"
	Polymarket = "polymarket"
)

// Tick is one venue's top of book for one market, reported whenever it
// changes: a best price, a size at a best price, or whether the book is
// trusted.
type Tick struct {
	Venue string // Kalshi or Polymarket

	// Market is the venue's own ID: a Kalshi market ticker, or a
	// Polymarket asset (token) ID.
	Market string

	// BidBps and AskBps are the best bid and ask for the market's "yes"
	// outcome (Kalshi's yes side; the Polymarket token itself), in basis
	// points of $1.
	BidBps, AskBps int

	// BidSize and AskSize are the sizes resting at BidBps and AskBps, in
	// hundredths of the venue's unit, so they stay exact integers ("47.00"
	// is 4700). Kalshi: contracts; the ask size is the "no" size resting
	// at 10000-AskBps, which is what makes that ask. Polymarket: shares.
	// Either way, one unit pays $1 if the outcome happens.
	BidSize, AskSize int64

	// OK is false when there is no trusted top of book: the book is
	// stale, a session is reconnecting, or one side is empty. Prices and
	// sizes are 0 then.
	OK bool

	// Time is when this host received the message that produced the Tick
	// (the reader stamps it before queueing), or when the client produced
	// it if no message did (a session ending, a drop noticed while idle).
	// Local clock, so Ticks from both venues are on one clock.
	Time time.Time

	// VenueTime is the venue's own timestamp on that message, or zero if
	// it had none (Kalshi snapshots; the ends of sessions). The venue's
	// clock, so not comparable across venues without allowing for skew.
	VenueTime time.Time
}
