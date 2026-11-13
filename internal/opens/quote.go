package opens

import "fmt"

// QuoteRule is what counts as a two-sided quote everywhere in the
// scanner: triage, backfill, report, the new-series alert and the
// bad-maker evaluation. A quote is a best bid and a best ask, each with
// at least MinSizeCenti resting at the price, no more than MaxSpreadBps
// apart.
//
// Price levels with less than the minimum size are ignored rather than
// disqualifying the book: a one-contract order in front of a maker's
// quote does not hide the quote, and does not count as one. A 1c bid
// and a 99c ask (a "stub" book) fail the spread cap.
//
// Candlesticks have no sizes, so a quote read from them is held to the
// spread cap only (MeetsSpread).
type QuoteRule struct {
	MaxSpreadBps int   // ask - bid at most this, bps
	MinSizeCenti int64 // resting size on each side at least this, hundredths of a contract
}

// DefaultQuoteRule: 10c wide at most, 10 contracts a side at least.
func DefaultQuoteRule() QuoteRule {
	return QuoteRule{MaxSpreadBps: 1000, MinSizeCenti: 1000}
}

func (q QuoteRule) String() string {
	return fmt.Sprintf("spread <= %d bps, size >= %s contracts a side", q.MaxSpreadBps, centi(q.MinSizeCenti))
}

// MeetsSpread reports whether a bid and an ask make a two-sided quote
// as far as prices go: both inside (0, $1), crossed by neither, and no
// more than MaxSpreadBps apart.
func (q QuoteRule) MeetsSpread(bid, ask int) bool {
	return bid > 0 && ask < 10000 && bid < ask && ask-bid <= q.MaxSpreadBps
}

// Meets reports whether a quote with these sizes qualifies.
func (q QuoteRule) Meets(bid, ask int, bidSz, askSz int64) bool {
	return q.MeetsSpread(bid, ask) && bidSz >= q.MinSizeCenti && askSz >= q.MinSizeCenti
}

// quoteBook rebuilds one market's full book from its captured events:
// a snapshot replaces it, a delta changes one level. Kalshi's book is
// two bid books: yes bids, and no bids, a no bid at p being a yes ask
// at 10000-p.
type quoteBook struct {
	yes, no map[int]int64 // price bps -> resting size, hundredths
}

func newQuoteBook() *quoteBook {
	return &quoteBook{yes: map[int]int64{}, no: map[int]int64{}}
}

func (b *quoteBook) apply(r EventRecord) {
	switch r.Type {
	case "snapshot":
		clear(b.yes)
		clear(b.no)
		for _, l := range r.Yes {
			b.yes[int(l[0])] = l[1]
		}
		for _, l := range r.No {
			b.no[int(l[0])] = l[1]
		}
	case "delta":
		if r.Price == nil || r.Delta == nil {
			return
		}
		side := b.yes
		if r.Side == "no" {
			side = b.no
		}
		if n := side[*r.Price] + *r.Delta; n > 0 {
			side[*r.Price] = n
		} else {
			delete(side, *r.Price)
		}
	}
}

// best returns the best bid and ask with at least minSize resting at
// the level; ok is false if either side has none.
func (b *quoteBook) best(minSize int64) (bid, ask int, bidSz, askSz int64, ok bool) {
	bid, noBid := -1, -1
	for p, n := range b.yes {
		if n >= minSize && p > bid {
			bid, bidSz = p, n
		}
	}
	for p, n := range b.no {
		if n >= minSize && p > noBid {
			noBid, askSz = p, n
		}
	}
	if bid < 0 || noBid < 0 {
		return 0, 0, 0, 0, false
	}
	return bid, 10000 - noBid, bidSz, askSz, true
}

// openingFromEvents replays a capture and finds its opening quote: the
// first moment the book has a bid and an ask that meet rule. ok is
// false if there is none, or if the capture lost its book before
// reaching one, since the first quote seen after a hole need not be the
// first there was. Records must be in file order.
func openingFromEvents(events []EventRecord, rule QuoteRule) (q quote, ok bool) {
	b := newQuoteBook()
	for _, r := range events {
		if r.Stale != "" && r.Stale != "unsynced" {
			return quote{}, false
		}
		b.apply(r)
		if r.Stale != "" {
			continue
		}
		if bid, ask, bidSz, askSz, have := b.best(rule.MinSizeCenti); have && rule.Meets(bid, ask, bidSz, askSz) {
			return quote{ms: eventMs(r), bid: bid, ask: ask, bidSz: bidSz, askSz: askSz}, true
		}
	}
	return quote{}, false
}
