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
becomes stale again if an update fails to apply or is dropped (see
Backpressure below). For Kalshi, failing to apply includes a non-contiguous
`seq`. The cause is logged once, and every later delta is ignored rather
than applied onto a book known to be wrong. Only a new full snapshot clears
staleness, and nothing requests one yet. Polymarket sometimes sends a
`book` event mid-stream on its own, so a stale Polymarket book may recover
by itself. A stale Kalshi book stays stale until the process restarts.

Each client runs two goroutines per venue: a reader that only reads the
socket and hands messages off, and a book goroutine that owns the `Feed`
and applies them. A bounded queue sits between them.

Not built yet: there's no reconnect or resync, no `Tick` struct, no
venue-pair config, no divergence calculation, no storage, and no metrics.
Drops are counted, but the total is only logged at exit.

## Backpressure

The reader never blocks on the book goroutine. If the queue
(`QueueSize`, 1024 messages per venue) is full, the reader drops the
message, counts the drop, and moves on. The book goes stale at that point
in the stream and stays stale until a fresh snapshot rebuilds it.

**Why drop rather than block.** Blocking the reader would make the queue
lossless only on paper. A blocked reader stops reading the socket, the
venue's send buffer fills, and the venue may disconnect us as a slow
consumer. Then the book is lost anyway, with a reconnect on top, and we
learn about it later. Dropping keeps the connection healthy and turns the
problem into one we already handle: a missing update. The real invariant is
that a book is never *used* with a missing delta, and stale-until-resync
keeps it. The cost is a gap in usable data until the next snapshot. For
Kalshi, that means until the process restarts, until step 3's resync
exists.

**How the book goroutine learns about a drop.** The reader and the book
goroutine share nothing but the queue. Each queued message carries the
reader's running drop count at the moment it was enqueued. When the book
goroutine handles a message whose count is higher than the previous one's,
it knows at least one message was lost immediately before this one. It
marks the book stale, then handles the message: a delta is ignored, and a
snapshot rebuilds the book.

Carrying the signal on the next message, rather than in a shared atomic
flag, puts the stale mark at the exact point in the stream where the gap
is. A flag would be seen at whatever moment the book goroutine next
checked it. That can be while the goroutine is still working through
messages queued *before* the drop. If one of those is a snapshot, the
goroutine would treat it as the resync, clear the stale mark, and then
apply post-drop updates onto a book that's missing one. Kalshi's `seq`
check would catch that late; Polymarket has nothing that would. A separate
"dropped" marker message doesn't work either, because the queue it would
go into is the one that's full. The running count also never needs
resetting, so there's no window where a reset could erase a new drop. The
count is still an atomic, but only so other goroutines can read the total
(for metrics later). Correctness doesn't depend on it.

One consequence: a drop is noticed when the *next* message gets through,
not when it happens. Until then the book goroutine is still working
through updates received before the gap, and those are valid.

**Why 1024.** Measured on the captures, applying a message costs about
9µs, so the book goroutine can process on the order of 100k messages/s. The
busiest second in the captures is 8 messages for Kalshi and 19 for
Polymarket. So the queue isn't absorbing a consumer that is steadily
slower than the feed. It's riding out short stalls: a GC pause, scheduling,
a log write to a slow terminal. 1024 covers 1–2 minutes of the captured
peak rate with the consumer completely stopped, and roughly half a second
to a second at a hundred times that rate. That's a few MB at the largest
captured message sizes. A larger queue wouldn't add safety. A consumer
stalled long enough to fill it is already serving a book that's seconds
old, and resyncing gets back to a current book faster than working
through a long backlog of old deltas.

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
cmd/kalshi/main.go                 connects to Kalshi's authenticated orderbook_delta channel; reader goroutine -> queue -> book goroutine, logs best bid/ask changes
cmd/polymarket/main.go             connects to Polymarket's public market channel; reader goroutine -> queue -> book goroutine, logs best bid/ask changes, sends the app-level heartbeat
internal/kalshi/book.go            Kalshi order book state: snapshot/delta application, yes/no inversion, sequence-gap rejection
internal/kalshi/book_test.go       table-driven tests plus a replay of testdata/kalshi_capture.jsonl
internal/kalshi/feed.go            message dispatch, staleness (no snapshot yet / seq gap), top-of-book change detection
internal/kalshi/feed_test.go       fixture replay, injected seq gap, synthetic top-of-book moves
internal/kalshi/queue.go           bounded non-blocking reader->book queue, drop count carried on each message, QueueSize
internal/kalshi/queue_test.go      fixture replay through a 2-slot queue forcing drops; stale mark lands at the drop's position
internal/polymarket/book.go        Polymarket order book state: snapshot/absolute price_change application, negRisk asset filtering
internal/polymarket/book_test.go   table-driven tests plus a replay of testdata/polymarket_capture.jsonl
internal/polymarket/feed.go        event dispatch (object or array framing), staleness until first book event, top-of-book change detection
internal/polymarket/feed_test.go   fixture replay, array framing, synthetic top-of-book moves
internal/polymarket/queue.go       same queue as kalshi's, duplicated per the no-shared-venue-code rule
internal/polymarket/queue_test.go  fixture replay through a 2-slot queue forcing drops; a pre-drop book event doesn't clear staleness
testdata/kalshi_capture.jsonl      captured orderbook_snapshot/orderbook_delta messages for one Kalshi market
testdata/polymarket_capture.jsonl  captured book/price_change/last_trade_price messages for one Polymarket asset
go.mod, go.sum                     module definition (Go 1.27, gorilla/websocket)
```
