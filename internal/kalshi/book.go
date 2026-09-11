// Package kalshi holds order book state for Kalshi markets, built from the
// orderbook_snapshot/orderbook_delta messages on the authenticated
// orderbook_delta channel.
package kalshi

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// Book holds Kalshi's order book state for a single market.
//
// Kalshi has no unified bid/ask book: it has two one-sided books, "yes" and
// "no", each a set of resting price levels. A no level at price p is
// economically a yes level at (10000-p) bps, since a yes contract and a no
// contract on the same market always settle to a combined 10000bps
// ($1.00). BestYes performs that inversion.
//
// Prices are stored as integer basis points (price_dollars * 10000, so
// "0.4300" -> 4300), per the no-floats-for-price rule. Sizes are stored as
// integer centi-units (size * 100, so "47.00" -> 4700) — the wire format
// gives sizes as decimal strings with exactly two places, and scaling by
// 100 keeps the same size exact and integer-only rather than reaching for
// floats just because CLAUDE.md's float ban is stated for price alone.
type Book struct {
	// MarketTicker identifies which market this book is for. Set by
	// ApplySnapshot; empty until the first snapshot is applied.
	MarketTicker string

	lastSeq int64
	yes     map[int]int64 // price bps -> size (centi-units)
	no      map[int]int64 // price bps -> size (centi-units)
}

// envelope is the common wrapper around every message on the
// orderbook_delta channel: {"type":..., "sid":..., "seq":..., "msg":{...}}.
// The "subscribed" ack has no seq, but neither ApplySnapshot nor ApplyDelta
// is ever called with one.
type envelope struct {
	Type string          `json:"type"`
	Seq  int64           `json:"seq"`
	Msg  json.RawMessage `json:"msg"`
}

// snapshotMsg is the msg payload of an orderbook_snapshot message.
// yes_dollars_fp/no_dollars_fp are arrays of [price_dollars, size] string
// pairs, e.g. [["0.4300","747.10"], ...].
type snapshotMsg struct {
	MarketTicker string     `json:"market_ticker"`
	YesDollarsFP [][]string `json:"yes_dollars_fp"`
	NoDollarsFP  [][]string `json:"no_dollars_fp"`
}

// deltaMsg is the msg payload of an orderbook_delta message. delta_fp is
// signed and is an increment against whatever size is already resting at
// price_dollars on the given side.
type deltaMsg struct {
	MarketTicker string `json:"market_ticker"`
	PriceDollars string `json:"price_dollars"`
	DeltaFP      string `json:"delta_fp"`
	Side         string `json:"side"` // "yes" or "no"
}

// ApplySnapshot replaces all book state from an orderbook_snapshot message,
// discarding whatever state (if any) the Book held before. It records the
// message's seq as the baseline that the next ApplyDelta must extend.
func (b *Book) ApplySnapshot(raw []byte) error {
	var env envelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return fmt.Errorf("kalshi: decoding snapshot envelope: %w", err)
	}
	if env.Type != "orderbook_snapshot" {
		return fmt.Errorf("kalshi: ApplySnapshot called with message type %q", env.Type)
	}

	var msg snapshotMsg
	if err := json.Unmarshal(env.Msg, &msg); err != nil {
		return fmt.Errorf("kalshi: decoding snapshot msg: %w", err)
	}

	yes, err := parseLevels(msg.YesDollarsFP)
	if err != nil {
		return fmt.Errorf("kalshi: parsing yes_dollars_fp: %w", err)
	}
	no, err := parseLevels(msg.NoDollarsFP)
	if err != nil {
		return fmt.Errorf("kalshi: parsing no_dollars_fp: %w", err)
	}

	b.MarketTicker = msg.MarketTicker
	b.yes = yes
	b.no = no
	b.lastSeq = env.Seq
	return nil
}

// ApplyDelta applies an orderbook_delta message: delta_fp is an increment
// to whatever size is resting at price_dollars on the given side, and the
// level is removed once its size reaches zero.
//
// If the message's seq is not exactly lastSeq+1, the book is stale — a
// message was missed and delta_fp can no longer be trusted as an increment
// against the level's true size. ApplyDelta rejects and reports that case
// rather than applying the delta, and leaves the book's state and lastSeq
// unchanged so the caller knows a resync (a fresh ApplySnapshot) is needed.
func (b *Book) ApplyDelta(raw []byte) error {
	var env envelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return fmt.Errorf("kalshi: decoding delta envelope: %w", err)
	}
	if env.Type != "orderbook_delta" {
		return fmt.Errorf("kalshi: ApplyDelta called with message type %q", env.Type)
	}
	if env.Seq != b.lastSeq+1 {
		return fmt.Errorf("kalshi: sequence gap on %s: have seq %d, got seq %d (book is stale, resync required)",
			b.MarketTicker, b.lastSeq, env.Seq)
	}

	var msg deltaMsg
	if err := json.Unmarshal(env.Msg, &msg); err != nil {
		return fmt.Errorf("kalshi: decoding delta msg: %w", err)
	}

	priceBps, err := parseFixedPoint(msg.PriceDollars, 4)
	if err != nil {
		return fmt.Errorf("kalshi: parsing delta price_dollars: %w", err)
	}
	if priceBps < 0 || priceBps > 10000 {
		return fmt.Errorf("kalshi: delta price %d bps out of range [0,10000]", priceBps)
	}

	delta, err := parseFixedPoint(msg.DeltaFP, 2)
	if err != nil {
		return fmt.Errorf("kalshi: parsing delta_fp: %w", err)
	}

	var levels map[int]int64
	switch msg.Side {
	case "yes":
		levels = b.yes
	case "no":
		levels = b.no
	default:
		return fmt.Errorf("kalshi: delta has unknown side %q", msg.Side)
	}

	price := int(priceBps)
	newSize := levels[price] + delta
	switch {
	case newSize < 0:
		return fmt.Errorf("kalshi: delta on %s side at %d bps would make size negative (%d)", msg.Side, price, newSize)
	case newSize == 0:
		delete(levels, price)
	default:
		levels[price] = newSize
	}

	b.lastSeq = env.Seq
	return nil
}

// BestYes returns Kalshi's yes-side market in basis points. bidBps is the
// highest yes price with resting nonzero size — a standing bid to buy yes.
// askBps is derived from the no book: the highest no price with resting
// nonzero size is a standing bid to buy no, which is economically a
// standing ask to sell yes at 10000-price. ok is false if either side of
// the book is currently empty.
func (b *Book) BestYes() (bidBps, askBps int, ok bool) {
	yesHigh, yesOK := highestNonZero(b.yes)
	if !yesOK {
		return 0, 0, false
	}
	noHigh, noOK := highestNonZero(b.no)
	if !noOK {
		return 0, 0, false
	}
	return yesHigh, 10000 - noHigh, true
}

// LastSeq returns the seq of the last message successfully applied.
func (b *Book) LastSeq() int64 {
	return b.lastSeq
}

// YesSize returns the resting size at priceBps on the yes side, and
// whether that level currently exists.
func (b *Book) YesSize(priceBps int) (int64, bool) {
	size, ok := b.yes[priceBps]
	return size, ok
}

// NoSize returns the resting size at priceBps on the no side, and whether
// that level currently exists.
func (b *Book) NoSize(priceBps int) (int64, bool) {
	size, ok := b.no[priceBps]
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

// parseLevels converts a snapshot side's [price_dollars, size] string pairs
// into a price(bps) -> size(centi-units) map.
func parseLevels(pairs [][]string) (map[int]int64, error) {
	levels := make(map[int]int64, len(pairs))
	for _, pair := range pairs {
		if len(pair) != 2 {
			return nil, fmt.Errorf("level entry has %d elements, want 2: %v", len(pair), pair)
		}
		priceBps, err := parseFixedPoint(pair[0], 4)
		if err != nil {
			return nil, fmt.Errorf("parsing price %q: %w", pair[0], err)
		}
		if priceBps < 0 || priceBps > 10000 {
			return nil, fmt.Errorf("price %d bps out of range [0,10000]", priceBps)
		}
		size, err := parseFixedPoint(pair[1], 2)
		if err != nil {
			return nil, fmt.Errorf("parsing size %q: %w", pair[1], err)
		}
		if size < 0 {
			return nil, fmt.Errorf("negative size %q at price %q", pair[1], pair[0])
		}
		if size == 0 {
			continue // no such thing as a resting zero-size level
		}
		levels[int(priceBps)] = size
	}
	return levels, nil
}

// parseFixedPoint converts a decimal string such as "0.4300" or "-94.69"
// into an integer scaled by 10^decimals (4300 for decimals=4, -9469 for
// decimals=2), without ever passing the value through a float. It rejects
// strings with more fractional digits than decimals, since silently
// truncating would lose precision the wire format didn't intend to lose.
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
