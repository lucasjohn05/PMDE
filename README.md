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

`cmd/pmde` is the service. It reads venue pairs from `pairs.yaml`, runs a
client for every market in them, tracks each pair's divergence (see
Divergence below), stores every Tick with 1s/1m/1h rollups (see
Storage below), and records when it was running, with the commit and
config it ran with, in `runs.jsonl` (see Run log). `cmd/pmde-analyze`
reports on the stored data per pair: coverage, mid divergence, the
executable gap before and after fees, and lead/lag (see Analysis). `cmd/kalshi` and `cmd/polymarket` run a single
venue's client against one hardcoded market, for debugging.

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
`cmd/polymarket` just configure a `Client` and run it. Clients report
their top of book as a `Tick` (`internal/tick`), the one venue-neutral
shape everything downstream works on.

Each binary serves Prometheus metrics on `/metrics`: message, drop,
reconnect and resync counts, whether each book is stale, ingest lag, and
(for `cmd/pmde`) each pair's divergence (see Metrics below).

All six planned steps are built.

**Live runs.** `cmd/pmde` with the two October 2026 FOMC pairs in
`pairs.yaml` synced all four books within a second and logged both
divergences. One run also lived through the host losing its network
(it went to sleep): all four connections dropped, dials failed on DNS with
backoff growing to ~17s, and every book resynced on its own once the
network was back. `cmd/kalshi` and `cmd/polymarket` still point at the
September 2026 Fed market, which has resolved: Polymarket no longer has a
book for it, so that client loops on the snapshot timeout, while Kalshi
still serves a snapshot with no quotes. With storage, a 75-second run
wrote the expected layout and `index.json` for all four markets, and a
restart on the same directory appended to the same day's files with the
first bars flagged `partial`. Every stored line parsed.

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
  exact ping interval isn't documented here, but a live Kalshi connection
  that carried no data for 90s stayed up, so its pings arrive well inside
  the 60s.

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

Each binary serves `/metrics` on `-metrics-addr` (default `:9100` for
`cmd/pmde`, `:9101` for `cmd/kalshi`, `:9102` for `cmd/polymarket`, so
they can run side by side). The ingest series are labeled `venue` and
`market` (the Kalshi ticker or Polymarket asset ID): `cmd/pmde` runs one
client per market, and a per-venue `book_stale` would just be whichever
client wrote last. Sum over `market` for per-venue totals. The divergence
gauges are labeled `pair`. `storage_blocked_seconds_total` has no labels
(see Storage).

| Metric | Type | Meaning |
|---|---|---|
| `messages_total` | counter | Messages read off the websocket, dropped ones included. Polymarket's `PONG` heartbeat replies aren't counted. |
| `dropped_messages_total` | counter | Messages the reader dropped because the queue was full. |
| `reconnects_total` | counter | Every dial after the first, failed ones included. |
| `resyncs_total{reason}` | counter | Times a trusted book was lost and had to be rebuilt from a fresh snapshot. `reason` is `seq_gap`, `dropped`, `reconnect` or `invalid_update`. |
| `book_stale` | gauge | 1 while there's no trusted book: before the first snapshot, after it goes stale, and while reconnecting. |
| `ingest_lag_seconds` | histogram | Receive time minus the venue's timestamp on the message. Buckets from 1ms to ~16s, doubling. |
| `divergence_mid_bps{pair}` | gauge | Kalshi mid minus Polymarket mid. Absent while either book is untrusted. |
| `divergence_gap_bps{pair}` | gauge | Executable gap (see Divergence). Absent while either book is untrusted. |
| `storage_blocked_seconds_total` | counter | Time book goroutines spent blocked handing Ticks to the storage writer. Rising means a slow disk is holding up the books. |

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

**Every ingest series exists from startup.** Counters start at 0 and
`book_stale` at 1, rather than appearing on their first event. Otherwise a
query like `rate(resyncs_total{reason="seq_gap"}[5m])` has nothing to
return until the first gap, and an alert on it can't tell "no gaps" from
"not scraping". The divergence gauges are the exception, on purpose: any
number there, 0 included, reads as a real comparison, so they exist only
while there is one.

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
  instead of being hidden as zero. This showed up for real: after the dev
  machine woke from sleep, both venues' lag read ~1.3s instead of ~0.1s,
  and `w32tm /stripchart` put the local clock 1.24s ahead. Net of that,
  Kalshi (`ts_ms`) and Polymarket both came to ~100ms, which also
  confirms `ts_ms` behaves as the event time.

**Serving.** The metrics port is bound at startup, so a bad or busy
address stops the binary immediately instead of failing quietly in the
background. The registry also carries the standard Go runtime and process
collectors.

## Divergence

`cmd/pmde` loads venue pairs from `pairs.yaml` (`-config` to change it):

```yaml
pairs:
  - name: fed-oct-2026-hold
    kalshi: KXFEDDECISION-26OCT-H0
    polymarket: "111061902544814266207267295505639408607400625795891618462682726460921782993748"
```

Each pair is a Kalshi market ticker and the Polymarket asset (token) ID of
the outcome that means the same as Kalshi's "yes", plus optional `notes`.
Pairs are written by hand; nothing matches markets automatically.

**Each pair's `notes` records how the two venues' resolution rules
differ**, read from Kalshi's market rules and contract terms and from
Polymarket's market description. For the Fed decision pairs:
- **Measure:** Kalshi resolves on "the target federal funds rate range",
  Polymarket on its upper bound.
- **Odd-sized moves:** Polymarket rounds up to the nearest 25 bps; Kalshi
  has no rounding rule. That's the one realistic way a hike-25 pair could
  resolve differently: a 12.5 bps hike.
- **No decision:** both resolve to no change, but on different timelines.
- **Trading end:** Kalshi stops trading at 1:55 PM ET on decision day,
  before the 2:00 PM ET statement, while Polymarket trades on until it
  resolves. Divergence after Kalshi's close compares a frozen book with a
  live one.
- **Settlement:** by the exchange on Kalshi; through the UMA oracle on
  Polymarket. The loader is strict: an
unknown key (a typo like `polymarkt`) is an error rather than an empty
field, and so is a duplicate name, a duplicate pairing, or a `0x...`
condition ID where the decimal token ID belongs. A market can appear in
more than one pair; it still gets one client and one connection.

**Two numbers per pair, both in bps:**
- **Mid divergence**: Kalshi mid minus Polymarket mid. Signed, so it says
  which venue prices the outcome higher. It's computed as
  `((kBid + kAsk) - (pBid + pAsk)) / 2`, with Go's integer division
  (truncating toward zero). Halving the difference once rounds once
  instead of twice; it's only inexact when the two spreads add up to an
  odd number of bps, and then by half a basis point.
- **Executable gap**: `max(kBid - pAsk, pBid - kAsk)`. Positive means the
  books cross: buying on one venue and selling on the other locks in that
  much, before fees. Zero means the best quotes touch; negative is how far
  apart they are. Mid divergence alone can't tell "the venues disagree"
  from "both spreads are wide".

**When it's computed.** Clients only emit a Tick when their top of book
changes, so the tracker recomputes a pair exactly when either side's best
changes. It logs a pair only when its divergence changes (or becomes
unavailable), not on every Tick.

**Skipped when either book is untrusted** (stale, reconnecting, not yet
synced, or with an empty side). The pair's gauges are deleted rather than
left at their last value, which would look current, and the log says
which side is missing. They come back on the next valid Tick.

**The tick stage blocks.** All clients send Ticks into one bounded channel
(`TickQueueSize`, 256) read by the tracker. Per CLAUDE.md, stages
downstream of the book block on full rather than drop. A dropped Tick would
leave the tracker holding an old top of book with nothing to say it's old;
a dropped *message* upstream is safe only because the book goroutine marks
the book stale. If the tracker stalled for long, the block would back up
into that client's reader->book queue, which drops, marks the book stale
and resyncs: an existing failure path that's already counted and logged.
The tracker's work per Tick is a few map lookups, so in practice the
channel is near empty. On shutdown the send gives up instead, so a client
can't hang on a tracker that has stopped.

**Live, 2026-10-20:** `fed-oct-2026-hold` at mid +100 (Kalshi 87/88¢,
Polymarket 86/87¢) and `fed-oct-2026-hike-25` at mid -100 (Kalshi 12/13¢,
Polymarket 13/14¢). Both have a gap of 0: the venues sit a tick apart and
touch, without crossing.

**Live, 2026-10-21, with the December pairs added:** both December pairs
*cross*, by 200 bps:
- `fed-dec-2026-hold`: Kalshi 21/22¢ vs Polymarket 18/19¢. Buying "no
  change" on Polymarket at 19¢ and selling on Kalshi at 21¢ nets 2¢ per
  contract before fees.
- `fed-dec-2026-hike-25`: Kalshi 75/76¢ vs Polymarket 78/79¢, the mirror
  image.

Whether 2¢ survives both venues' fees, and the 12.5 bps rounding
difference in the notes, is what the analysis is for.

## Storage

`cmd/pmde` stores every Tick under `-data-dir` (default `data/`,
gitignored), with 1s, 1m and 1h rollups computed as the Ticks are
written.

### Layout

```
data/
  index.json                                   directory -> venue, market, pair names
  runs.jsonl                                   when pmde was running, and which build and config
  kalshi/KXFEDDECISION-26OCT-H0/
    2026-10-21.ticks.jsonl                     every Tick
    2026-10-21.1s.jsonl                        1-second bars
    2026-10-21.1m.jsonl                        1-minute bars
    2026-10-21.1h.jsonl                        1-hour bars
  polymarket/1110619025448142662...82993748/   (full asset ID)
    ...
```

- **One file per record kind, per market, per UTC day.** Segments are
  append-only: a day's file is only ever appended to, then left alone.
  Days start at UTC midnight, so no window ever spans two files, and a
  time range is just the right files. There's no automatic retention;
  delete old days by hand.
- **`index.json` maps each directory to its pairs.** Polymarket
  directories are named by 77-digit asset IDs, so this is how you find
  the right one. It's rewritten at startup (atomically, via a temp file
  and rename) and merged with what's there, so directories written under
  an older config stay explained after `pairs.yaml` changes.
- **Directory names are checked.** A market ID is used as a directory
  name only if it's letters, digits, `.`, `_` and `-` (and not `.`/`..`),
  so a bad config entry can't write outside the data directory.

### Records

JSON Lines (one JSON object per line). Venue and market come from the
path.

```
ticks:  {"t":1792613520960,"vt":1792613518936,"bid":1100,"ask":1200,"bid_sz":14022026,"ask_sz":5858113,"ok":true}
        {"t":1792613530000,"ok":false}
bars:   {"t":1792613460000,"n":9,"bid":[8600,8600,8600,8600],"ask":[8700,8700,8700,8700],"stale_ms":0,"ok":true}
```

**Two timestamps on a tick:**
- **`t`, the local receive time (Unix ms, UTC).** It's when the reader
  read the message that produced the Tick off the socket, stamped before
  queueing. A Tick with no message behind it (a session ending, a drop
  noticed while idle) gets the time it was produced.
- **`vt`, the venue's timestamp on that message**, when it has one:
  Kalshi's `ts_ms` on deltas (snapshots have none), and Polymarket's
  `timestamp` on every event. For a Polymarket `book` event that's when
  the book last changed, which is right for the state it describes, even
  though it's excluded from `ingest_lag_seconds`. `vt` is omitted when
  there isn't one.
- **Windows, days and replay all use `t`.** Both venues' `t` come from
  one clock (this host's), so two markets' files merge by `t` into the
  order the service saw them, and replaying that through the divergence
  `Tracker` reproduces exactly the divergence it computed live
  (`TestDivergenceRecomputedFromDisk` does that). `vt` is on two
  different venue clocks, and neither is this host's.
- **Neither timestamp is corrected.** On the dev machine `t - vt` read
  ~2s in one run: the local clock had drifted 1.87s fast (Windows hadn't
  synced it since the day before), on top of ~100ms of real lag. Keeping
  both raw means skew can be measured and allowed for later rather than
  baked in.

**Prices and sizes:** prices are integer bps, as everywhere. `bid_sz` and
`ask_sz` are the sizes resting at the best bid and ask, in hundredths of
the venue's unit so they stay exact integers (`"140256.51"` is
`14025651`):
- **Kalshi:** contracts. The ask size is the "no" size resting at
  `10000 - ask`, the no bid that makes that yes ask.
- **Polymarket:** shares.
- One unit pays $1 if the outcome happens on either venue.

Prices and sizes appear only when `ok`; `{"t":...,"ok":false}` means
there's no trusted book from then on. Since Ticks now carry sizes, a
client emits one when a top-of-book *size* changes too, not only a price.
The divergence tracker recomputes on those and logs only if the
divergence moved.

### Rollups

Each bar summarizes the top of book over one window: `bid` and `ask` are
`[open, high, low, close]`, `n` is the number of Ticks in it, `stale_ms`
how long it had no trusted book, and `ok` whether it ended trusted.

- **Bars describe the book, not just the Ticks in the window.** A window
  opens on the state carried in from before it, so a 1m bar with one Tick
  still has the right open.
- **High and low only count trusted periods.** A window that was
  untrusted throughout has `"bid":null,"ask":null`.
- **Sparse.** A window is only written if a Tick landed in it. A missing
  bar means nothing changed: it's the previous bar's close (and `ok`)
  carried forward. On a quiet market, writing every 1s bar would be
  86,400 lines a day per market saying nothing.
- **Closed by a timer.** A once-a-second flush closes windows that ended
  at least a second ago (the grace lets a Tick still in the queues land in
  its own window). Without it, the last bar before a quiet hour wouldn't
  be written until the next Tick.
- **The clock can step back** (an NTP correction). A late Tick keeps its
  raw `t` in the tick file but goes into the window that's open, and into
  today's file, never reopening a closed window or an earlier day.
- **`partial`.** The first window after startup is flagged
  `"partial":true` (the state before it is unknown), and so is any window
  cut short by a clean shutdown. A restart inside a window can therefore
  leave two partial bars with the same `t`, covering different parts of
  it.

### Durability

- **Records are buffered, then flushed and fsynced once a second** and on
  shutdown. A crash loses at most about a second of records plus the
  windows still open, the same as a kill. This was checked live: after a
  forced kill, every file ended on a complete line and the next run
  appended cleanly.
- **A torn last line is cut off.** If a crash leaves a file ending in a
  partial line, the writer cuts it off when it reopens the file (and logs
  it), so the next record starts on its own line. Readers (`ReadTicks`,
  `ReadBars`) also skip an unparseable *last* line, and treat a bad line
  anywhere else as corruption.

### Run log

The tick files are silent both when a market is quiet and when pmde
isn't running, and a crash leaves no trace in them. `runs.jsonl` tells
the two apart. Each process appends, tagged with a random run ID:

```
{"ev":"start","t":1792700000000,"run":"9f2c...","commit":"f20e3c5...","config":"/etc/pmde/pairs.yaml","config_sha256":"1b7e...","pairs":["fed-oct-2026-hold",...],"heartbeat_ms":60000}
{"ev":"alive","t":1792700060000,"run":"9f2c..."}                once a minute
{"ev":"stop","t":1792700093000,"run":"9f2c..."}                 clean shutdown only
```

- **A run is known to be up** from its start line to its stop line, or,
  after a crash, to its last `alive` line. Up to a minute after that
  last line is unknown, and gets counted as down. `t` is the same local
  clock as the ticks'.
- **The stop line is written last**, after the writer has flushed and
  closed every file, so a stop line means the run's data is on disk.
- **`commit`** is the git commit the binary was built from. A plain
  `go build` in the repo stamps it (with `-dirty` for uncommitted
  changes); the Docker build has no `.git`, so it's passed in as the
  `GIT_COMMIT` build arg, and is `unknown` if nobody did. (`go run`
  doesn't stamp it either.)
- **`config_sha256`** is a hash of the pair config file's bytes, so data
  can be tied to the exact pairs and notes it was recorded under. Editing
  a comment changes it too.
- **Each line is written and fsynced on its own.** It's one line a
  minute, and a heartbeat sitting in a buffer through a crash would
  prove nothing. A torn last line is cut off at the next start, as for
  the segments.

### When the disk is slow

The storage stage blocks rather than drops, per CLAUDE.md. A dropped Tick
would leave a wrong top of book in the record with nothing to say so. The
cost is that a slow disk propagates back up the pipeline, one bounded
queue at a time:

1. **The writer blocks** on a slow write or fsync, and its queue
   (`storage.QueueSize`, 1024 Ticks) fills.
2. **The fan-out in `cmd/pmde` blocks.** Each book goroutine's `OnTick`
   sends the Tick to the divergence tracker, then to the writer; the
   second send waits for room.
3. **The book goroutines block** inside `OnTick`, so they stop draining
   their reader queues.
4. **The reader queues fill and drop.** Readers never block (the
   websocket must keep being read), so once a reader's queue is full,
   messages are dropped and counted.
5. **The books go stale.** When the disk catches up and a book goroutine
   gets going again, it sees the drop count has moved and marks its book
   stale.
6. **The clients resync** from a fresh snapshot. If the disk is still
   slow, the cycle repeats, so the books flap between stale and synced
   until the disk recovers. They never serve a book with a missing delta.

**What shows up in metrics, in order:**
- `storage_blocked_seconds_total` rising;
- then `dropped_messages_total` and `book_stale` per market;
- then `resyncs_total{reason="dropped"}`.

A single wait of a second or more is also logged ("a polymarket tick
waited 1.2s for room in the storage queue"). `TestSlowStorageChain` runs
the whole chain through a real client, writer and fake venue and checks
each of those metrics.

**Why it's acceptable.** Appending a Tick is a buffered write, and Ticks
only come on top-of-book changes, so the queue absorbs ordinary fsync
pauses easily (`storage_blocked_seconds_total` stayed 0 in live runs).
Reaching step 4 takes a disk stalled for long enough to fill 1024 Ticks
*and* a full reader queue behind them. At that point, resyncing is
better than recording a history with silent gaps.

## Analysis

`cmd/pmde-analyze` reads a data directory (copied off the droplet, or
the local one) and writes one report per pair, as text or `-json`:

```
pmde-analyze -data-dir data -pair fed-dec-2026-hold -from 2026-10-22 -to 2026-10-28
```

`-from`/`-to` take a UTC date or RFC 3339 (`-to` with a date means the
end of that day); by default the report covers all the data there is.
To pull the droplet's data, see the "Copy the data out" row under
Running it.

**What it computes.** It merges both venues' tick files by `t` and
walks the timeline once. Each instant gets a single classification:

1. **After Kalshi's last trade.** The pair's `kalshi_last_trade` (1:55
   PM ET on decision day, from the contract terms) is the cutoff. After
   it, Kalshi's book is frozen while Polymarket's still trades.
2. **pmde not running.** This comes from `runs.jsonl`: a run counts as
   up from its start line to its stop line, or to its last heartbeat
   after a crash. Runs whose config didn't include the pair don't
   count. A new run starts with both books untrusted, because the stored
   ticks don't record a clean shutdown's final untrusted tick, and a
   crash records nothing at all.
3. **A book untrusted.** The last tick for that book had `ok:false`.
4. **Covered.** Everything else. Every statistic below is computed over
   covered time only.

The report sections:

- **Coverage.** How [from, to) splits into those classes.
- **Mid divergence.** Kalshi mid minus Polymarket mid, weighted by how
  long each value held rather than by tick count: mean, median, p5, p95
  and max |x|.
- **Executable gap, before and after fees.** Both legs are taker orders,
  at the smaller of the two crossing top-of-book sizes.
  - **Time:** time crossed before fees, after fees, and after fees at
    the stress rate.
  - **Episodes:** each crossing episode (a continuous stretch of crossed
    books, ended by uncrossing or by losing coverage) is shown at its
    largest gap: size, prices, the effective Kalshi fee per contract at
    that size, net per contract and net $ for the whole size.
  - **Fees:** the formulas, rates and source links are printed with
    every report.
- **Lead/lag.** The section opens with two caveats. First, `t` is local
  receive time, so results reflect arrival order at this host, not true
  venue-side ordering. Second, each venue's median `t - vt` and their
  difference: a lead shorter than that difference can't be told apart
  from one venue's messages taking longer to arrive. The difference is
  repeated next to every lead/lag number. Then:
  - **Follow delays.** Mid moves of at least `-move-bps` (default 50,
    half a 1-cent tick) are matched in time order, one to one. A move
    follows the earliest unmatched same-direction move on the other
    venue within `-follow-window` (default 60s). A move that follows
    nothing is a leader. Per direction, the report gives leaders, the
    share followed (excluding censored ones), and the median and p90
    delay.
  - **Cross-correlation.** 1-second mid changes, Kalshi at s against
    Polymarket at s+lag, for lags of up to `-corr-max-lag` (default
    30s), with the peak. The peak is flagged as likely noise when a
    venue changed in fewer than 30 seconds.
- **Not modelled.** Resolution-rule differences (the 12.5 bp case),
  that the Polymarket venue is closed to US persons, capital and
  settlement, depth beyond the top of book, and latency.

**Fees** (`fees.yaml`: rates in ppm, each with source links and the
date it was checked; the formulas are in `internal/analysis/fees.go`):

| Venue | Taker fee | Rate used |
|---|---|---|
| Kalshi | `roundup(0.07 x C x P x (1-P))` to the next cent, per order ([fee schedule](https://kalshi.com/docs/kalshi-fee-schedule.pdf), [series fee_type](https://docs.kalshi.com/api-reference/market/get-series.md), [rounding](https://docs.kalshi.com/getting_started/fee_rounding.md)) | 0.07; `KXFEDDECISION`'s `fee_multiplier` is 1 |
| Polymarket (international) | `C x rate x p x (1-p)`, rounded to 5 decimals ([fees](https://docs.polymarket.com/trading/fees.md), [per-market feeSchedule](https://docs.polymarket.com/market-data/market-details.md)) | 0, the market's own `feeSchedule.rate` (and the CLOB's `fd.r`); a stress row at 0.05, the Economics category rate |

- **The fees are taker fees.** Makers pay 0.25x the Kalshi rate and
  nothing on Polymarket, but neither leg is a maker here.
- **The Polymarket CLOB's `/fee-rate` endpoint isn't used.** It returns
  `base_fee: 1000` for every fee-bearing market, whatever that market's
  actual rate ([py-clob-client-v2 #107](https://github.com/Polymarket/py-clob-client-v2/issues/107)).
- **Prices and money stay integers.** Rates are in ppm, money in
  micro-dollars, and products that could overflow use `math/big`. Only
  the correlation coefficients are floats.

**Which Polymarket.** pmde records the international venue
(`polymarket.com`; the CLOB on Polygon), which is closed to US persons.
"Executable" therefore means executable by a non-US trader with accounts
on both venues. Polymarket US is a separate exchange, with its own books
and fees ([0.0695 x C x p x (1-p)](https://docs.polymarket.us/fees.md)
since 2026-10-19), and pmde doesn't record it.

**First run on the droplet's first 23 minutes** (2026-10-22, 98%
covered):

- **The December hold pair** was crossed by 2c (Kalshi bid 21c vs
  Polymarket ask 19c) for 100% of covered time, but net of fees for only
  46%. When the smaller top-of-book size is small, Kalshi's per-order
  round-up to a cent eats the whole 2c. At the 1,514-contract snapshot
  the effective Kalshi fee was 1.16c per contract.
- **The December hike pair** was crossed 46% of the time by 1c (Kalshi
  ask 76c vs Polymarket bid 77c), where Kalshi's fee (1.27c a contract)
  exceeds the gap. It was never net-positive.
- **Twenty minutes is far too little data for lead/lag.** The report
  says so.

## Deploying

The laptop sleeps, drifts and can't send a background process Ctrl+C, so
the service belongs on an always-on Linux host. `Dockerfile` and
`deploy/compose.yaml` run it as a container that restarts itself, keeps
its data on a named volume, and serves metrics on the host's loopback
only.

**What's in the image:** a static binary on distroless `static`: CA
certificates for the `wss://` venues, no shell, no package manager,
running as uid/gid 65532. About 23 MB. `pairs.yaml` is baked in as a
default, but compose mounts the repo's copy over it, so changing pairs is
an edit and a restart, not a rebuild.

**Tested locally with Docker Desktop** (the same compose file):
- all eight books synced;
- `docker port` showed `9100/tcp -> 127.0.0.1:9100`;
- `docker compose stop` exited 0 in 3s, and the volume held every
  market's `partial` 1h bar (the clean-shutdown path, which `taskkill`
  never exercised);
- after an external SIGKILL of the process, Docker restarted the
  container and all eight books resynced. (`docker kill` doesn't test
  this: Docker counts it as a manual stop and doesn't restart.)

**Deployed on a DigitalOcean droplet** (Ubuntu 24.04, 1 vCPU, 1 GB) on
2026-10-22, following the steps below exactly:
- all eight books synced, no drops or reconnects, and
  `storage_blocked_seconds_total` 0;
- metrics listen on `127.0.0.1:9100` only, and the port doesn't answer
  from outside;
- `runs.jsonl` got a start line with the deployed commit and the
  config hash `sha256sum pairs.yaml` gives, then an `alive` a minute;
- on a reboot, Docker stopped pmde cleanly (a `stop` line) and started
  it again at boot, 26s later, with every book resynced;
- mean ingest lag ~15ms on Kalshi and ~45ms on Polymarket, with the
  clock NTP-synced; the container uses about 7 MB of memory.

### On a fresh droplet

Ubuntu 24.04 on the smallest droplet (1 vCPU, 1 GB) is plenty: 8
connections, a few MB of data a day. The steps run as `root`,
DigitalOcean's default login, so nothing needs `sudo`. (With a sudo user
instead, prefix the commands with `sudo`, and put `GIT_COMMIT=...` after
it: `sudo` doesn't pass your environment through.)

1. **Update and reboot if needed:**
   ```
   apt update && apt full-upgrade -y
   test -f /var/run/reboot-required && reboot
   ```
   Plain `apt upgrade` holds back kernel updates that pull in new
   packages; `full-upgrade` takes them. A few packages may stay listed as
   upgradable: those are Ubuntu's phased updates, which apt installs when
   their rollout reaches the host.

2. **Install Docker Engine and the compose plugin**, from Docker's apt
   repository, following https://docs.docker.com/engine/install/ubuntu/.
   Its packages enable the Docker service at boot, which
   `restart: unless-stopped` relies on after a reboot.

3. **Check the clock is synchronized.** Every tick's `t` is this host's
   clock, and lag is measured against it (see Storage):
   ```
   timedatectl          # want "System clock synchronized: yes" and "NTP service: active"
   ```
   Ubuntu's default `systemd-timesyncd` does this. If it says no, run
   `timedatectl set-ntp true`.

4. **Firewall: SSH only.** Allow SSH *before* enabling, or the next login
   is locked out:
   ```
   ufw allow OpenSSH && ufw --force enable
   ```
   Docker-published ports bypass ufw. That's why compose binds metrics
   to `127.0.0.1:9100` rather than relying on the firewall.

5. **Get the code:**
   ```
   git clone https://github.com/lucasjohn05/PMDE.git /opt/pmde
   cd /opt/pmde
   ```

6. **Install the Kalshi key** where compose expects it, readable only by
   the container's user (uid 65532). Without swarm, a compose file
   secret is a bind mount that keeps the host file's owner and mode.
   ```
   # from your machine:
   scp ~/.kalshi/private_key.pem root@<droplet>:/tmp/kalshi.pem
   # on the droplet:
   install -d -m 0755 /etc/pmde
   install -o 65532 -g 65532 -m 0400 /tmp/kalshi.pem /etc/pmde/kalshi.pem
   rm /tmp/kalshi.pem
   ```
   Then set the key ID in `deploy/.env` (gitignored, and excluded from the
   image build):
   ```
   (umask 077; echo 'KALSHI_KEY_ID=<your key id>' > deploy/.env)
   ```

7. **Build and start**, passing the commit to record in the run log:
   ```
   GIT_COMMIT=$(git rev-parse HEAD) docker compose -f deploy/compose.yaml up -d --build
   ```
   The build takes a few minutes on 1 vCPU.

8. **Check it**, after a minute so there's a heartbeat:
   ```
   docker compose -f deploy/compose.yaml logs        # "run <id>: commit ...", "book synced" x8
   curl -s localhost:9100/metrics | grep -E '^(book_stale|divergence_mid_bps)'
   docker run --rm -v pmde_data:/data:ro busybox tail -n 3 /data/runs.jsonl   # start + alive
   ss -ltn | grep 9100                               # 127.0.0.1:9100 only
   ```
   From your own machine, reach the metrics through an SSH tunnel:
   `ssh -L 9100:localhost:9100 root@<droplet>`, then
   http://localhost:9100/metrics.

### Running it

| Task | Command (as root, from `/opt/pmde`) |
|---|---|
| Logs | `docker compose -f deploy/compose.yaml logs -f` |
| Change pairs | edit `pairs.yaml`, then `docker compose -f deploy/compose.yaml restart` |
| Update the code | `git pull && GIT_COMMIT=$(git rev-parse HEAD) docker compose -f deploy/compose.yaml up -d --build` |
| Stop cleanly | `docker compose -f deploy/compose.yaml stop` (SIGTERM: writes partial windows, flushes, fsyncs) |
| Find the data | `docker volume inspect pmde_data` (`Mountpoint`, usually `/var/lib/docker/volumes/pmde_data/_data`) |
| Copy the data out | `docker run --rm -v pmde_data:/data:ro -v "$PWD":/out busybox tar czf /out/pmde-data.tgz -C /data .` |

**Things that would lose data:**
- `docker compose down -v`, which deletes the volume. A plain `down`, a
  rebuild and a restart all keep it.
- A crash or power loss loses about the last second of records and the
  windows still open, the same as on any host (see Durability).

Container logs are capped at 5 × 20 MB. Ticks aren't: there's no
retention yet, but at a few MB a day that's years of disk on the
smallest droplet.

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

To run it permanently, see Deploying above. Locally, the service (both
venues, every pair in `pairs.yaml`):

```
KALSHI_KEY_PATH=/path/to/private_key.pem
KALSHI_KEY_ID=<your key id>
go run ./cmd/pmde                       # -config pairs.yaml -metrics-addr :9100 -data-dir data
```

Single-venue debug clients, each against one hardcoded market. Kalshi needs an API key:

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

Metrics are on `http://localhost:9100/metrics` (`cmd/pmde`),
`http://localhost:9101/metrics` (Kalshi) and
`http://localhost:9102/metrics` (Polymarket). Change the address with
`-metrics-addr`, e.g. `go run ./cmd/polymarket -metrics-addr :9200`.

## Layout

```
CLAUDE.md                          engineering constraints and working style for this repo
Dockerfile                         static build on distroless (nonroot), runs cmd/pmde with /data and /etc/pmde/pairs.yaml
.dockerignore                      keeps keys, .env, data/ and NOTES.md out of the build context
deploy/compose.yaml                the deployment: restart unless-stopped, data on a named volume, key as a secret, metrics on 127.0.0.1:9100, capped logs
.github/workflows/ci.yml           go vet and go test -race on push and PR (ubuntu-latest)
cmd/pmde/main.go                   the service: loads pairs.yaml, one client per distinct market, Ticks fanned out to the divergence tracker and the storage writer, index.json, /metrics
cmd/kalshi/main.go                 debug: one kalshi.Client on a hardcoded market, logs best bid/ask changes, /metrics
cmd/polymarket/main.go             debug: one polymarket.Client on a hardcoded asset, logs best bid/ask changes, /metrics
cmd/pmde-analyze/main.go           per-pair report from a data directory: flags, loading, text or JSON
fees.yaml                          fee rates (ppm) with source links and checked dates; Polymarket rate and stress rate
internal/analysis/analysis.go      the timeline sweep: coverage classes, run resets, cutoff, mid stats, crossings and episodes
internal/analysis/leadlag.go       follow matching, 1s cross-correlation, transport medians (t - vt)
internal/analysis/fees.go          fees.yaml loading, KalshiFee (cent round-up) and PolymarketFee (5 decimals), integer/big math
internal/analysis/load.go          reading tick files for a range (plus the day before) and runs.jsonl; default range
internal/analysis/text.go          the text report
internal/analysis/*_test.go        fee rounding and overflow; coverage, run resets, other pairs' runs, mid stats, episodes and fees, follows, threshold, correlation lag, transport, text caveats
pairs.yaml                         the hand-written venue pairs (October and December 2026 FOMC: hold, hike 25), with resolution-rule differences in notes and Kalshi's last-trade time
internal/tick/tick.go              Tick: venue, market, best bid/ask (bps) and sizes, trusted or not, receive and venue times; what clients emit
internal/storage/storage.go        Writer: per-market JSONL segments, 1s/1m/1h rollups at write time, the 1s flush, Send (blocking, counts time blocked)
internal/storage/segment.go        append-only segment files, torn-tail repair, record types, ReadTicks/ReadBars
internal/storage/index.go          index.json: directory -> venue, market, pair names (merged, atomic)
internal/storage/runs.go           runs.jsonl: start (commit, config hash, pairs), alive every minute, stop; ReadRuns
internal/storage/runs_test.go      start/alive/stop lines, two runs, write after stop, torn tail, missing file
internal/storage/storage_test.go   exact tick lines, bars over a hand-worked sequence, all-stale bars, clock steps back, day rollover, torn tail, unsafe names, blocking Send, Run, index
internal/storage/replay_test.go    divergence recomputed from the stored files equals the live tracker's, step by step
internal/divergence/config.go      pair config loading and validation (strict YAML); kalshi_last_trade must carry an explicit offset
internal/divergence/config_test.go valid config, every rejection, and the committed pairs.yaml
internal/divergence/divergence.go  Compute (mid divergence, executable gap), Tracker (latest Tick per market, recompute affected pairs), TickQueueSize
internal/divergence/divergence_test.go Compute table incl. rounding and crossed books; Tracker: shared markets, stale skip, gauge removal, log-on-change; Run over a channel
internal/kalshi/auth.go            RSA key loading and per-dial request signing
internal/kalshi/book.go            Kalshi order book state: snapshot/delta application, yes/no inversion, sequence-gap rejection
internal/kalshi/book_test.go       table-driven tests plus a replay of testdata/kalshi_capture.jsonl
internal/kalshi/client.go          sessions (dial, subscribe, reader + book goroutines), resync on stale, reconnect with backoff, snapshot/read timeouts, metrics
internal/kalshi/client_test.go     against a local websocket server: seq gap then mid-stream hang-up then full replay; snapshot timeout; backoff; metric values
internal/kalshi/feed.go            message dispatch, staleness (no snapshot yet / seq gap / drop), top-of-book change detection, Next (the book loop step, incl. idle drop check)
internal/kalshi/feed_test.go       fixture replay, injected seq gap, synthetic top-of-book moves
internal/kalshi/lag_test.go        Feed.Lag: ts_ms on deltas, none on snapshots, negative skew, cleared by a step with no message; Feed.Stamp and TopSizes
internal/kalshi/queue.go           bounded non-blocking reader->book queue, drop count carried on each message, QueueSize
internal/kalshi/queue_test.go      fixture replay through a 2-slot queue forcing drops; stale mark lands at the drop's position
internal/kalshi/next_test.go       Feed.Next: drop then silence, drop at close, real reader/book goroutines (for -race)
internal/metrics/metrics.go        the Prometheus metrics, a per-market recorder with labels pre-applied, per-pair divergence gauges, the /metrics server
internal/metrics/metrics_test.go   series exist up front, values recorded, pair gauges set/cleared, nil recorders, server and busy-port startup error
internal/polymarket/book.go        Polymarket order book state: snapshot/absolute price_change application, negRisk asset filtering
internal/polymarket/book_test.go   table-driven tests plus a replay of testdata/polymarket_capture.jsonl
internal/polymarket/client.go      as kalshi's, plus the app-level PING heartbeat
internal/polymarket/client_test.go against a local websocket server: mid-stream hang-up then full replay; forced drop through the real goroutines; the slow-storage chain end to end; backoff; metric values
internal/polymarket/feed.go        event dispatch (object or array framing), staleness until first book event, top-of-book change detection, Next (as kalshi)
internal/polymarket/feed_test.go   fixture replay, array framing, synthetic top-of-book moves
internal/polymarket/lag_test.go    Feed.Lag: timestamp on price_change/last_trade_price, not book events, newest in an array; Feed.Stamp (book events included) and TopSizes
internal/polymarket/queue.go       same queue as kalshi's, duplicated per the no-shared-venue-code rule
internal/polymarket/queue_test.go  fixture replay through a 2-slot queue forcing drops; a pre-drop book event doesn't clear staleness
internal/polymarket/next_test.go   Feed.Next: drop then silence, drop at close, real reader/book goroutines (for -race)
testdata/kalshi_capture.jsonl      captured orderbook_snapshot/orderbook_delta messages for one Kalshi market
testdata/polymarket_capture.jsonl  captured book/price_change/last_trade_price messages for one Polymarket asset
go.mod, go.sum                     module definition (Go 1.27, gorilla/websocket, prometheus/client_golang, go.yaml.in/yaml/v3)
```
