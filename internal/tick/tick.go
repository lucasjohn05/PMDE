// Package tick defines Tick, the one shape both venue clients report the
// top of book in. Everything downstream of the books (divergence, and
// later storage) works on Ticks and never sees venue-specific data.
package tick

import "time"

// Venue names, as used in Tick.Venue and the metrics' venue label.
const (
	Kalshi     = "kalshi"
	Polymarket = "polymarket"
)

// Tick is one venue's top of book for one market, reported whenever it
// changes.
type Tick struct {
	Venue string // Kalshi or Polymarket

	// Market is the venue's own ID: a Kalshi market ticker, or a
	// Polymarket asset (token) ID.
	Market string

	// BidBps and AskBps are the best bid and ask for the market's "yes"
	// outcome (Kalshi's yes side; the Polymarket token itself), in basis
	// points of $1.
	BidBps, AskBps int

	// OK is false when there is no trusted top of book: the book is
	// stale, a session is reconnecting, or one side is empty. BidBps and
	// AskBps are 0 then.
	OK bool

	// Time is when the client produced the Tick (local clock).
	Time time.Time
}
