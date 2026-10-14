# pmde

Ingests live order book data from Kalshi and Polymarket over WebSocket and
measures price divergence between the two venues quoting the same event.

Prediction markets on the same real-world event trade independently on each
venue, so their prices can drift apart even though they're supposed to
settle to the same outcome. In the captures gathered so far, on a major
event, the two venues quoted within one tick of each other — the gap is
usually small. What's worth measuring is how fast price discovery
propagates between two venues that share no capital: Polymarket settles in
USDC on Polygon, Kalshi through US banking, so there's no fast arbitrage
path tying their prices together. Measuring it means holding both venues'
order books to a common price representation and comparing them
continuously.

## Status

Both venue clients connect over WebSocket (Kalshi's with a signed
request), build a live order book from the feed, and log the best bid/ask
in basis points whenever it changes — not on every message.

Each venue package has a `Feed` that sits between the raw messages and the
`Book`: it dispatches by message type, decides whether the book can be
trusted, and reports top-of-book changes. Book and feed are tested against
the captured fixtures in `testdata/`, including, for Polymarket,
cross-checking the derived book against the venue's own reported best
bid/ask.

A book is **stale** (untrusted, no best reported) until its first full
snapshot — Kalshi's `orderbook_snapshot`, Polymarket's `book` event — and
becomes stale again if an update fails to apply. For Kalshi that includes a
non-contiguous `seq`: the gap is logged once and every later delta is
ignored rather than applied onto a book known to be wrong. A new snapshot
is the only thing that clears staleness; nothing requests one yet, so in
practice a stale Kalshi book stays stale until the process restarts.

Not built yet: the reader and book still run in one goroutine with no
channel between them; there's no reconnect or resync, no `Tick` struct, no
venue-pair config, no divergence calculation, no storage, and no metrics.

## Venue differences

The two venues don't expose the same shape of data, and the book code for
each reflects that rather than hiding it behind a common interface.

**Book shape.** Kalshi has no bid/ask book. It has two one-sided books,
"yes" and "no", each just a set of resting price levels, and since a yes
and a no contract on the same market settle to $1.00 combined, the
highest resting no bid is economically the lowest yes ask, at `10000 - p`
(the inversion flips the ordering, so it's the highest no bid specifically
that maps to the lowest yes ask). `internal/kalshi`'s `BestYes` does that
inversion. Polymarket gives real bids and asks directly —
`internal/polymarket`'s `Best` just takes the highest bid and the lowest
ask, no inversion needed.

**Delta semantics.** Kalshi's `orderbook_delta` messages carry `delta_fp`,
an increment to be added to whatever size is already resting at that price
level. Polymarket's `price_change` messages carry the new absolute size at
that level — you assign it, you don't add to it. Applying one venue's
update rule to the other's messages would silently corrupt the book.

**Staleness detection.** Kalshi messages carry a `seq`; a gap between the
last-applied `seq` and the next one means a message was missed and the
book can no longer be trusted, so `internal/kalshi`'s `ApplyDelta` rejects
a non-contiguous `seq` outright rather than applying it. Polymarket has no
sequence numbers at all — there's nothing to gap-check. Instead, a `book`
event is a full, unconditional replacement of state, and it can arrive at
any time, mid-stream, as a normal occurrence rather than a resync signal.

**Keep-alive.** The two venues picked opposite mechanisms for the same
problem: Kalshi uses WebSocket protocol-level ping/pong control frames,
while Polymarket ignores those and instead requires an application-level
`PING` text message every 10 seconds. So `cmd/kalshi` sends no heartbeat
of its own — it relies on gorilla/websocket's default handling of
protocol-level ping/pong frames — while `cmd/polymarket` sends the literal
text `PING` every 10 seconds and treats a `PONG` reply as a heartbeat, not
a data message.

**Independent validation.** Polymarket's `price_change` messages carry
`best_bid`/`best_ask` alongside the size update — the venue's own
top-of-book, reported independently of the levels you're maintaining
yourself. That let the derived book be checked against a second, external
number instead of just against its own arithmetic: replaying
`testdata/polymarket_capture.jsonl` end to end, the derived `Best()` and
the last-reported `best_bid`/`best_ask` matched exactly. Kalshi gives no
equivalent independent figure, so its book can only be checked for
internal consistency (snapshot + deltas add up), not against a
venue-reported number.

This is why there's no shared `Exchange` interface between the two
clients: the venues aren't the same shape, and forcing them through one
interface would mean encoding one venue's assumptions as the general case.

## Running it

Kalshi needs an API key:

```
KALSHI_KEY_PATH=/path/to/private_key.pem
KALSHI_KEY_ID=<your key id>
go run ./cmd/kalshi
```

Polymarket needs no credentials — the market channel is public:

```
go run ./cmd/polymarket
```

Both connect, subscribe to one hardcoded market/asset, and log top-of-book
changes and staleness transitions until interrupted or the connection
drops.

## Layout

```
CLAUDE.md                          engineering constraints and working style for this repo
cmd/kalshi/main.go                 connects to Kalshi's authenticated orderbook_delta channel, feeds a kalshi.Feed, logs best bid/ask changes
cmd/polymarket/main.go             connects to Polymarket's public market channel, feeds a polymarket.Feed, logs best bid/ask changes, sends the app-level heartbeat
internal/kalshi/book.go            Kalshi order book state: snapshot/delta application, yes/no inversion, sequence-gap rejection
internal/kalshi/book_test.go       table-driven tests plus a replay of testdata/kalshi_capture.jsonl
internal/kalshi/feed.go            message dispatch, staleness (no snapshot yet / seq gap), top-of-book change detection
internal/kalshi/feed_test.go       fixture replay, injected seq gap, synthetic top-of-book moves
internal/polymarket/book.go        Polymarket order book state: snapshot/absolute price_change application, negRisk asset filtering
internal/polymarket/book_test.go   table-driven tests plus a replay of testdata/polymarket_capture.jsonl
internal/polymarket/feed.go        event dispatch (object or array framing), staleness until first book event, top-of-book change detection
internal/polymarket/feed_test.go   fixture replay, array framing, synthetic top-of-book moves
testdata/kalshi_capture.jsonl      captured orderbook_snapshot/orderbook_delta messages for one Kalshi market
testdata/polymarket_capture.jsonl  captured book/price_change/last_trade_price messages for one Polymarket asset
go.mod, go.sum                     module definition (Go 1.27, gorilla/websocket)
```
