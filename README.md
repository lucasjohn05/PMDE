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
in basis points whenever it changes — not on every message. When the
connection drops or the book goes stale, they reconnect and rebuild the
book from a fresh snapshot (see Reconnect and resync below).

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
`seq`. Nothing is applied to a stale book; the client resyncs instead.

Each venue's `Client` (`internal/kalshi`, `internal/polymarket`) runs one
session per connection, with two goroutines: a reader that only reads the
socket and hands messages off, and a book goroutine that owns the `Feed`
and applies them. A bounded queue sits between them. `cmd/kalshi` and
`cmd/polymarket` just configure a `Client` and run it.

Each binary serves Prometheus metrics on `/metrics`: message, drop,
reconnect and resync counts, whether the book is stale, and ingest lag
(see Metrics below).

Not built yet: no `Tick` struct, no venue-pair config, no divergence
calculation, and no storage.

**The hardcoded markets have closed.** Both clients still point at the
September 2026 Fed decision market, which resolved on Sept 16. Polymarket
no longer has an order book for that asset. Run live, the client connects,
gets no `book` event, and keeps retrying (with backoff) on the snapshot
timeout. Pointing it at a live asset works: a 30-second run synced from the
first `book` event and held the connection through the heartbeat. The
Kalshi ticker is for the same September meeting. Choosing live markets
belongs with the venue-pair config.

## Reconnect and resync

**Resync and reconnect are the same operation.** When a book goes stale (a
seq gap, a dropped message, an update that won't apply), the book
goroutine closes the connection and the client takes the ordinary
reconnect path. A new connection means a new subscription, and both venues
start every subscription with a full snapshot. That gives one recovery
path instead of two, and it's the only one guaranteed to produce a
snapshot on both venues. Polymarket sends `book` events mid-stream when it
chooses, not on request. Kalshi might support re-subscribing in place, but
that would be a second path to get right and test, for an event (a stale
book) that should be rare. The cost is a fresh handshake, plus re-signing
for Kalshi, on each resync.

**Each session starts from nothing.** Every connection gets a fresh `Feed`
and queue, so the old book is discarded rather than patched. For Kalshi,
the new subscription's snapshot sets the baseline `seq`, and deltas are
validated from there. For Polymarket, `price_change` events are ignored
until the first `book` event. While a session has no trusted book, the
client reports the best bid/ask as unavailable. The same happens the moment
a session ends, so downstream code never holds a quote from a book that no
longer exists.

**Backoff.** The client waits before each reconnect. The wait window grows
exponentially from 500ms and is capped at 30s. The actual delay is a
random point in the upper half of that window ("equal jitter"). The floor
stops a connection that keeps failing from retrying in a tight loop, and
the randomness stops clients that fail for the same reason (one network
blip) from retrying in lockstep. A session that stayed up for at least a
minute resets the backoff, so an occasional drop reconnects within half a
second while a flapping connection slows down. Each resync is logged with
its reason: `seq_gap`, `dropped`, `invalid_update`, `reconnect` (the
connection failed or was lost), or `snapshot_timeout`.

**Two more ways a session ends.**
- **Snapshot timeout.** No snapshot within 10s of subscribing, for
  example a rejected subscription or, as it turns out, a closed market.
- **Read timeout.** A minute with nothing at all from the venue. Kalshi
  sends protocol pings and Polymarket answers our `PING`s, so silence that
  long means the connection is dead even if TCP hasn't noticed. Kalshi's
  ping interval is an assumption, roughly 10s; only the 60s read timeout
  depends on it.

**No shared reconnect code.** The two `Client`s are near-duplicates. They
differ in auth (Kalshi signs every dial), heartbeat (Polymarket's
app-level `PING`), and what counts as a snapshot. The backoff function is
the only piece small and identical enough to share. It's duplicated
instead, like `parseFixedPoint`, per the no-venue-abstraction rule.

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
keeps it. The cost is a gap in usable data until the client has
reconnected and received a fresh snapshot.

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
count is an atomic so the book goroutine can also read the running total
directly, for the idle check below.

**Drops followed by silence.** On its own, the in-band count only
reports a drop when the *next* message gets through. If the feed went
quiet right after a drop, the book would look valid indefinitely while
missing an update. So whenever the book goroutine finds the queue empty,
it also compares the reader's atomic drop total with the last count it
saw, and marks the book stale if the total has gone up. That's safe
because an empty queue means everything enqueued before the drop has
already been applied. The order matters: the total is read *before*
checking that the queue is empty. Read after, the queue could fill up and
drop in between, and the book would be marked stale while pre-drop
messages were still queued. A pre-drop snapshot among them would then
clear the mark, and the next message's count would already have been
seen, so the drop would go unnoticed. The same check runs once more when
the reader closes the queue, so a drop just before shutdown still
counts. Until a drop is noticed, the book goroutine is working through
updates from before the gap, and those are valid.

The goroutine tests run under `go test -race` in CI (GitHub Actions,
`ubuntu-latest`). The race detector doesn't run on the main dev machine
(Windows with GCC 7.3; the race-enabled test binary fails to start with
`0xc0000139`).

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

## Metrics

Each binary serves `/metrics` on `-metrics-addr` (default `:9101` for
Kalshi, `:9102` for Polymarket, so both can run on one host). Every
series is labeled `venue`.

| Metric | Type | Meaning |
|---|---|---|
| `messages_total` | counter | Messages read off the websocket, dropped ones included. Polymarket's `PONG` heartbeat replies aren't counted. |
| `dropped_messages_total` | counter | Messages the reader dropped because the queue was full. |
| `reconnects_total` | counter | Every dial after the first, failed ones included. |
| `resyncs_total{reason}` | counter | Times a trusted book was lost and had to be rebuilt from a fresh snapshot. `reason` is `seq_gap`, `dropped`, `reconnect` or `invalid_update`. |
| `book_stale` | gauge | 1 while there's no trusted book: before the first snapshot, after it goes stale, and while reconnecting. |
| `ingest_lag_seconds` | histogram | Receive time minus the venue's timestamp on the message. Buckets from 1ms to ~16s, doubling. |

**Reconnects and resyncs count different things.** A resync is the loss
of a book; a reconnect is a dial. One seq gap is one resync and at least
one reconnect. A string of failed dials, or a market that never sends a
snapshot (the closed hardcoded markets do exactly that), adds reconnects
but no resyncs, because there was no book to lose. So the reasons are what
broke a trusted book: `seq_gap`, `dropped`, `reconnect` (the connection
was lost while the book was live), and `invalid_update` (an update that
couldn't be parsed or applied). That last reason isn't in the original
spec, but folding it into one of the others would misreport it. A snapshot
timeout can't be a resync reason: it only fires before the first snapshot.

**Every series exists from startup.** Counters start at 0 and
`book_stale` at 1, rather than appearing on their first event. Otherwise a
query like `rate(resyncs_total{reason="seq_gap"}[5m])` has nothing to
return until the first gap, and an alert on it can't tell "no gaps" from
"not scraping".

**The reader stays cheap.** The reader goroutine increments
`messages_total` and `dropped_messages_total`. Each venue's client gets
its counters with the venue label already applied, so that's one atomic
add per message, with no label lookup and no lock. It doesn't parse JSON
either: the queue stamps each message with its receive time, and the book
goroutine, which parses the message anyway, reads the venue's timestamp
and records the lag.

**Ingest lag.** Both venues provide a usable timestamp.
- **Kalshi:** `ts_ms` on each `orderbook_delta`. Snapshots don't carry
  one.
- **Polymarket:** `timestamp` (Unix ms, as a string) on `price_change`,
  `last_trade_price` and the other events. If one message frames several
  events, the newest timestamp counts.
- **Polymarket `book` events are excluded.** Their `timestamp` is when the
  book last changed, not when the event was sent. In a live run, the
  snapshot on subscribe was 3s and 13s old, while `price_change` events
  arrived ~100ms after their stamp. Counting snapshots would measure how
  quiet the market is and would skew the histogram on every reconnect.
- **Messages count whether or not they're applied** (for example, deltas
  before the first snapshot), because lag measures the transport, not the
  book. Dropped messages never reach the book goroutine, so they aren't
  measured.
- **Lag includes clock skew** between the venue and this host, so the host
  clock needs NTP. If the venue's clock runs ahead, a sample can be
  negative; it's recorded as is rather than clamped, so skew stays visible
  instead of being hidden as zero.
- Kalshi's `ts_ms` is assumed to be when Kalshi produced the delta. It
  hasn't been checked against a live feed (no credentials yet).

**Serving.** The metrics port is bound at startup, so a bad or busy
address stops the binary immediately instead of failing quietly in the
background. The registry also carries the standard Go runtime and process
collectors.

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
`PING` text message every 10 seconds. So the Kalshi client sends no
heartbeat of its own. It answers Kalshi's protocol-level pings with pongs,
as gorilla/websocket's default handler does, and also uses each ping to
extend its read deadline. The Polymarket client sends the literal text
`PING` every 10 seconds and treats a `PONG` reply as a heartbeat, not a
data message.

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
changes, resyncs and reconnects until interrupted. (Both hardcoded markets
have closed; see Status.)

Metrics are on `http://localhost:9101/metrics` (Kalshi) and
`http://localhost:9102/metrics` (Polymarket). Change the address with
`-metrics-addr`, e.g. `go run ./cmd/polymarket -metrics-addr :9200`.

## Layout

```
CLAUDE.md                          engineering constraints and working style for this repo
.github/workflows/ci.yml           go vet and go test -race on push and PR (ubuntu-latest)
cmd/kalshi/main.go                 loads the API key, signs each dial, serves /metrics, runs a kalshi.Client, logs best bid/ask changes
cmd/polymarket/main.go             serves /metrics, runs a polymarket.Client, logs best bid/ask changes
internal/kalshi/book.go            Kalshi order book state: snapshot/delta application, yes/no inversion, sequence-gap rejection
internal/kalshi/book_test.go       table-driven tests plus a replay of testdata/kalshi_capture.jsonl
internal/kalshi/client.go          sessions (dial, subscribe, reader + book goroutines), resync on stale, reconnect with backoff, snapshot/read timeouts, metrics
internal/kalshi/client_test.go     against a local websocket server: seq gap then mid-stream hang-up then full replay; snapshot timeout; backoff; metric values
internal/kalshi/feed.go            message dispatch, staleness (no snapshot yet / seq gap / drop), top-of-book change detection, Next (the book loop step, incl. idle drop check)
internal/kalshi/feed_test.go       fixture replay, injected seq gap, synthetic top-of-book moves
internal/kalshi/lag_test.go        Feed.Lag: ts_ms on deltas, none on snapshots, negative skew, cleared by a step with no message
internal/kalshi/queue.go           bounded non-blocking reader->book queue, drop count carried on each message, QueueSize
internal/kalshi/queue_test.go      fixture replay through a 2-slot queue forcing drops; stale mark lands at the drop's position
internal/kalshi/next_test.go       Feed.Next: drop then silence, drop at close, real reader/book goroutines (for -race)
internal/metrics/metrics.go        the Prometheus metrics, a per-venue recorder with the label pre-applied, the /metrics server
internal/metrics/metrics_test.go   series exist up front, values recorded, nil recorder, server and busy-port startup error
internal/polymarket/book.go        Polymarket order book state: snapshot/absolute price_change application, negRisk asset filtering
internal/polymarket/book_test.go   table-driven tests plus a replay of testdata/polymarket_capture.jsonl
internal/polymarket/client.go      as kalshi's, plus the app-level PING heartbeat
internal/polymarket/client_test.go against a local websocket server: mid-stream hang-up then full replay; forced drop through the real goroutines; backoff; metric values
internal/polymarket/feed.go        event dispatch (object or array framing), staleness until first book event, top-of-book change detection, Next (as kalshi)
internal/polymarket/feed_test.go   fixture replay, array framing, synthetic top-of-book moves
internal/polymarket/lag_test.go    Feed.Lag: timestamp on price_change/last_trade_price, not book events, newest in an array
internal/polymarket/queue.go       same queue as kalshi's, duplicated per the no-shared-venue-code rule
internal/polymarket/queue_test.go  fixture replay through a 2-slot queue forcing drops; a pre-drop book event doesn't clear staleness
internal/polymarket/next_test.go   Feed.Next: drop then silence, drop at close, real reader/book goroutines (for -race)
testdata/kalshi_capture.jsonl      captured orderbook_snapshot/orderbook_delta messages for one Kalshi market
testdata/polymarket_capture.jsonl  captured book/price_change/last_trade_price messages for one Polymarket asset
go.mod, go.sum                     module definition (Go 1.27, gorilla/websocket, prometheus/client_golang)
```
