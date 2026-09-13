// Package polymarket holds order book state for Polymarket assets, built
// from the "book" and "price_change" events on the CLOB market channel.
package polymarket

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// Book holds Polymarket's order book state for a single asset.
//
// Unlike Kalshi, Polymarket quotes real bids and asks directly — there is
// no yes/no inversion to perform here. The two fixture event types drive
// the book very differently, though:
//
//   - "book" (ApplySnapshot) is a full replacement of all state. It can
//     arrive at any time, including mid-stream; that is normal, not an
//     error, and there is no sequence number to validate it against.
//   - "price_change" (ApplyPriceChange) carries, per entry, the new
//     absolute size resting at a price level — not an increment like
//     Kalshi's delta_fp. ApplyPriceChange assigns, it never adds.
//
// Because of negRisk, a single price_change message bundles entries for
// both an asset and its complement. ApplyPriceChange filters to AssetID
// and ignores every other entry in the message.
//
// Prices are stored as integer basis points (price * 10000, so "0.8100"
// -> 8100), per the no-floats-for-price rule. Sizes are stored as integer
// centi-units (size * 100, so "13121.73" -> 1312173), for the same reason
// Kalshi's sizes are: exact, integer-only arithmetic.
type Book struct {
	// AssetID is the asset this book tracks. price_change entries for any
	// other asset ID (i.e. the negRisk complement) are ignored.
	AssetID string

	bids map[int]int64 // price bps -> size (centi-units)
	asks map[int]int64 // price bps -> size (centi-units)

	// bestBid/bestAsk are Polymarket's own reported top-of-book, taken
	// verbatim from a price_change entry's best_bid/best_ask fields. They
	// are kept separate from the bids/asks derived above, on purpose:
	// LastReportedBest lets a caller compare the two instead of this type
	// silently reconciling them.
	bestBid  int
	bestAsk  int
	haveBest bool
}

// NewBook returns a Book that tracks assetID, with no resting levels and
// no reported best yet.
func NewBook(assetID string) *Book {
	return &Book{
		AssetID: assetID,
		bids:    make(map[int]int64),
		asks:    make(map[int]int64),
	}
}

// priceLevel is a single {"price":"...", "size":"..."} entry in a book
// event's bids/asks arrays.
type priceLevel struct {
	Price string `json:"price"`
	Size  string `json:"size"`
}

// bookMsg is a "book" event: a flat envelope (no msg wrapper, unlike
// Kalshi), scoped to a single asset_id, carrying the full resting book.
type bookMsg struct {
	AssetID   string       `json:"asset_id"`
	Bids      []priceLevel `json:"bids"`
	Asks      []priceLevel `json:"asks"`
	EventType string       `json:"event_type"`
}

// priceChangeEntry is one element of a "price_change" event's
// price_changes array. size is the new absolute size resting at price on
// side — not an increment.
type priceChangeEntry struct {
	AssetID string `json:"asset_id"`
	Price   string `json:"price"`
	Size    string `json:"size"`
	Side    string `json:"side"` // "BUY" or "SELL"
	BestBid string `json:"best_bid"`
	BestAsk string `json:"best_ask"`
}

// priceChangeMsg is a "price_change" event: a flat envelope wrapping one
// or more priceChangeEntry values, which — because of negRisk — can cover
// more than one asset in a single message.
type priceChangeMsg struct {
	PriceChanges []priceChangeEntry `json:"price_changes"`
	EventType    string             `json:"event_type"`
}

// ApplySnapshot replaces all book state from a "book" event, discarding
// whatever state (if any) the Book held before. A book event is a full
// replacement whenever it arrives, mid-stream or not — there is no
// sequence number and no staleness to detect, unlike Kalshi's snapshots.
//
// best_bid/best_ask are not carried on book events, so ApplySnapshot does
// not touch what LastReportedBest returns.
func (b *Book) ApplySnapshot(raw []byte) error {
	var msg bookMsg
	if err := json.Unmarshal(raw, &msg); err != nil {
		return fmt.Errorf("polymarket: decoding book event: %w", err)
	}
	if msg.EventType != "book" {
		return fmt.Errorf("polymarket: ApplySnapshot called with event_type %q", msg.EventType)
	}
	if msg.AssetID != b.AssetID {
		return fmt.Errorf("polymarket: book event asset_id %q does not match this book's asset_id %q",
			msg.AssetID, b.AssetID)
	}

	bids, err := parseLevels(msg.Bids)
	if err != nil {
		return fmt.Errorf("polymarket: parsing bids: %w", err)
	}
	asks, err := parseLevels(msg.Asks)
	if err != nil {
		return fmt.Errorf("polymarket: parsing asks: %w", err)
	}

	b.bids = bids
	b.asks = asks
	return nil
}

// ApplyPriceChange applies a "price_change" event. Each entry's size is
// the new absolute size resting at price on side, so ApplyPriceChange
// assigns rather than adds; a size of zero removes the level. Entries
// whose asset_id is not b.AssetID (the negRisk complement) are ignored.
func (b *Book) ApplyPriceChange(raw []byte) error {
	var msg priceChangeMsg
	if err := json.Unmarshal(raw, &msg); err != nil {
		return fmt.Errorf("polymarket: decoding price_change event: %w", err)
	}
	if msg.EventType != "price_change" {
		return fmt.Errorf("polymarket: ApplyPriceChange called with event_type %q", msg.EventType)
	}

	for _, entry := range msg.PriceChanges {
		if entry.AssetID != b.AssetID {
			continue // the negRisk complement asset; not ours to track
		}

		priceBps, err := parseFixedPoint(entry.Price, 4)
		if err != nil {
			return fmt.Errorf("polymarket: parsing price_changes price %q: %w", entry.Price, err)
		}
		if priceBps < 0 || priceBps > 10000 {
			return fmt.Errorf("polymarket: price %d bps out of range [0,10000]", priceBps)
		}
		size, err := parseFixedPoint(entry.Size, 2)
		if err != nil {
			return fmt.Errorf("polymarket: parsing price_changes size %q: %w", entry.Size, err)
		}
		if size < 0 {
			return fmt.Errorf("polymarket: negative size %q at price %q", entry.Size, entry.Price)
		}

		var levels map[int]int64
		switch entry.Side {
		case "BUY":
			levels = b.bids
		case "SELL":
			levels = b.asks
		default:
			return fmt.Errorf("polymarket: price_change has unknown side %q", entry.Side)
		}

		price := int(priceBps)
		if size == 0 {
			delete(levels, price)
		} else {
			levels[price] = size
		}

		if entry.BestBid != "" {
			bestBid, err := parseFixedPoint(entry.BestBid, 4)
			if err != nil {
				return fmt.Errorf("polymarket: parsing best_bid %q: %w", entry.BestBid, err)
			}
			b.bestBid = int(bestBid)
		}
		if entry.BestAsk != "" {
			bestAsk, err := parseFixedPoint(entry.BestAsk, 4)
			if err != nil {
				return fmt.Errorf("polymarket: parsing best_ask %q: %w", entry.BestAsk, err)
			}
			b.bestAsk = int(bestAsk)
		}
		b.haveBest = true
	}

	return nil
}

// Best returns the derived top-of-book in basis points: bidBps is the
// highest bid with resting nonzero size, askBps is the lowest ask with
// resting nonzero size. ok is false if either side is currently empty.
func (b *Book) Best() (bidBps, askBps int, ok bool) {
	bid, bidOK := highestNonZero(b.bids)
	if !bidOK {
		return 0, 0, false
	}
	ask, askOK := lowestNonZero(b.asks)
	if !askOK {
		return 0, 0, false
	}
	return bid, ask, true
}

// LastReportedBest returns the most recent best_bid/best_ask Polymarket
// itself reported for AssetID, verbatim off a price_change entry — as
// opposed to Best, which is derived from the bids/asks this Book
// maintains. ok is false if no price_change for AssetID has been applied
// yet.
func (b *Book) LastReportedBest() (bidBps, askBps int, ok bool) {
	return b.bestBid, b.bestAsk, b.haveBest
}

// BidSize returns the resting size at priceBps on the bid side, and
// whether that level currently exists.
func (b *Book) BidSize(priceBps int) (int64, bool) {
	size, ok := b.bids[priceBps]
	return size, ok
}

// AskSize returns the resting size at priceBps on the ask side, and
// whether that level currently exists.
func (b *Book) AskSize(priceBps int) (int64, bool) {
	size, ok := b.asks[priceBps]
	return size, ok
}

func highestNonZero(levels map[int]int64) (price int, ok bool) {
	best := -1
	for p, size := range levels {
		if size <= 0 {
			continue
		}
		if p > best {
			best = p
		}
	}
	if best < 0 {
		return 0, false
	}
	return best, true
}

func lowestNonZero(levels map[int]int64) (price int, ok bool) {
	best := -1
	for p, size := range levels {
		if size <= 0 {
			continue
		}
		if best == -1 || p < best {
			best = p
		}
	}
	if best == -1 {
		return 0, false
	}
	return best, true
}

// parseLevels converts a book event's [{"price","size"}, ...] entries into
// a price(bps) -> size(centi-units) map.
func parseLevels(raw []priceLevel) (map[int]int64, error) {
	levels := make(map[int]int64, len(raw))
	for _, lvl := range raw {
		priceBps, err := parseFixedPoint(lvl.Price, 4)
		if err != nil {
			return nil, fmt.Errorf("parsing price %q: %w", lvl.Price, err)
		}
		if priceBps < 0 || priceBps > 10000 {
			return nil, fmt.Errorf("price %d bps out of range [0,10000]", priceBps)
		}
		size, err := parseFixedPoint(lvl.Size, 2)
		if err != nil {
			return nil, fmt.Errorf("parsing size %q: %w", lvl.Size, err)
		}
		if size < 0 {
			return nil, fmt.Errorf("negative size %q at price %q", lvl.Size, lvl.Price)
		}
		if size == 0 {
			continue // no such thing as a resting zero-size level
		}
		levels[int(priceBps)] = size
	}
	return levels, nil
}

// parseFixedPoint converts a decimal string such as "0.81" or "13121.73"
// into an integer scaled by 10^decimals (8100 for decimals=4, 1312173 for
// decimals=2), without ever passing the value through a float. It rejects
// strings with more fractional digits than decimals, since silently
// truncating would lose precision the wire format didn't intend to lose.
//
// (This mirrors internal/kalshi's parseFixedPoint exactly. Duplicated
// rather than shared, per CLAUDE.md's no-venue-abstraction rule — each
// venue package is self-contained.)
func parseFixedPoint(s string, decimals int) (int64, error) {
	if s == "" {
		return 0, fmt.Errorf("empty decimal string")
	}

	neg := false
	if s[0] == '-' {
		neg = true
		s = s[1:]
	}

	intPart, fracPart, _ := strings.Cut(s, ".")
	if len(fracPart) > decimals {
		return 0, fmt.Errorf("%q has more than %d decimal places", s, decimals)
	}
	fracPart += strings.Repeat("0", decimals-len(fracPart))
	if intPart == "" {
		intPart = "0"
	}

	combined := intPart + fracPart
	for _, r := range combined {
		if r < '0' || r > '9' {
			return 0, fmt.Errorf("%q is not a valid decimal number", s)
		}
	}

	n, err := strconv.ParseInt(combined, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%q: %w", s, err)
	}
	if neg {
		n = -n
	}
	return n, nil
}
