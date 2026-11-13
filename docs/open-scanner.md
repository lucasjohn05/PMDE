# Open scanner

The open scanner looks for Kalshi series where one market maker posts
the opening quotes from a crude model. It records market opens, joins
them with how the markets settled, and ranks series. It is record-only:
there is no order placement and no trading code anywhere in it.

The signature it looks for, per series:

- the same opening size on both sides, open after open (one maker,
  one script);
- opening mids across an event's rungs that sit on a two-parameter
  normal CDF (a crude model);
- opening mids that are miscalibrated against results (the model is
  wrong in a repeatable way);
- enough opens a day, at enough size, for that to matter.

## Pipeline

```
Kalshi REST ──> discovery ──> schedule ──> capture (one connection per event)
  /series         daily series snapshot        │ bounded queue, blocks
  /markets        new-series log               ▼
  /events                                   recorder ──> opens/events/<series>/<day>.jsonl
                                                         opens/captures.jsonl
Kalshi REST ──> pmde-opens backfill ──> opens/backfill.jsonl ──> pmde-opens report ──> table + CSV
  /markets, /markets/trades, candlesticks, /historical/*
```

| Step | Code | Command |
|---|---|---|
| 1. Discovery | `internal/kalshi/rest.go`, `internal/opens/discovery.go`, `schedule.go` | `pmde-scan` |
| 2. Open capture | `internal/kalshi/multifeed.go`, `eventclient.go`, `bookevent.go`, `internal/opens/capture.go` | `pmde-scan` |
| 3. Settlement backfill | `internal/opens/backfill.go` | `pmde-opens backfill` |
| 4. Analysis | `internal/opens/report.go`, `reporttext.go` | `pmde-opens report` |

What is reused rather than duplicated: the capture uses the same
`Book`, `Queue`, backoff and drop-and-mark-stale rule as `cmd/pmde`.
The connection type is new: `kalshi.EventClient` holds the books of
several markets on one subscription, next to the one-market `Client`
that `cmd/pmde` runs, which is unchanged. The JSONL files go through
`storage.Appender`, which wraps the tick segments' open/repair/flush
code. The REST client retries with the websocket client's `backoff`.

## How an event is captured

The rungs of an event that open together are one capture: one
connection, one `orderbook_delta` subscription naming every rung
(`market_tickers`).

- **Grouping.** When any rung of an event comes within `lead` of its
  `open_time`, every scheduled rung of that event opening within
  `window` of the earliest one is taken with it. The capture subscribes
  `lead` before the first open and records until `window` after the
  last. A rung opening later than that is a separate capture.
- **Whole or not at all.** `max_concurrent` counts connections. An
  event that comes due with every slot taken is skipped whole, and so is
  one whose first rung has already opened. No event is captured in part.
  `opens_skipped_total` counts the rungs.
- **Shared seq.** Kalshi numbers every message of a subscription with
  one `seq`: one snapshot per market first, then the deltas of all the
  markets interleaved. A gap therefore cannot be pinned on a market, so
  a gap, a dropped message or a bad update marks every book on the
  connection stale. Each rung's record gets a `stale` line, each rung is
  flagged incomplete, and the connection is replaced; the new
  subscription starts with a fresh snapshot per rung.
- **Unsubscribing** is closing the connection.

## Running it

```
go build ./cmd/pmde-scan && go build ./cmd/pmde-opens

# record opens (needs the Kalshi key, like pmde; metrics on :9101)
KALSHI_KEY_PATH=/path/to/key.pem KALSHI_KEY_ID=<key id> \
  ./pmde-scan -config opens.yaml -data-dir data

# results and earliest prices, for captured markets and for the
# allowlist's markets settled since a date (public REST; no key)
./pmde-opens backfill -config opens.yaml -data-dir data -since 2026-10-15

# the ranked table, plus data/opens/report.csv
./pmde-opens report -data-dir data -deciles
```

On the droplet the scanner is a second service in `deploy/compose.yaml`,
behind the `scan` profile so a plain `up` does not start it. It shares
the image, the Kalshi key and the data volume with `pmde`.

```
cd /opt/pmde && git pull       # once the open-scanner branch is on main
# edit opens.yaml (the allowlist) if needed; it is mounted, not baked in

# build the image and start both services. The rebuild restarts pmde
# too: a few seconds' gap in its ticks while its books resync.
GIT_COMMIT=$(git rev-parse HEAD) docker compose -f deploy/compose.yaml --profile scan up -d --build

docker compose -f deploy/compose.yaml --profile scan logs -f scan
curl -s localhost:9101/metrics | grep '^opens_'

# backfill and report, run in a throwaway container on the same volume
docker compose -f deploy/compose.yaml --profile scan run --rm \
  --entrypoint /usr/local/bin/pmde-opens scan backfill -config /etc/pmde/opens.yaml -data-dir /data
docker compose -f deploy/compose.yaml --profile scan run --rm \
  --entrypoint /usr/local/bin/pmde-opens scan report -data-dir /data -deciles
```

The backfill is incremental, so it can run from cron once a day. To
stop the scanner alone: `docker compose -f deploy/compose.yaml --profile
scan stop scan`. Alloy does not scrape `:9101` yet; add a scrape target
next to the `:9100` one in `deploy/monitoring/config.alloy` to get the
`opens_*` series into Grafana.

## Config (`opens.yaml`)

| Key | Default | Meaning |
|---|---|---|
| `series` | none | The allowlist of series tickers. |
| `include_new_series` | `true` | Also capture series first seen after the first snapshot. |
| `new_series_days` | 14 | How long a new series stays watched. |
| `max_new_series` | 100 | At most this many new series (the most recent). |
| `lead` | `5s` | Subscribe this long before `open_time`. |
| `window` | `60s` | Record until this long after `open_time`. |
| `max_concurrent` | 50 | Captures at once. One capture is one connection, covering one event's rungs. |
| `market_poll` | `1m` | Poll interval for the watched series' unopened markets. |
| `series_poll` | `15m` | Poll interval for the series list. |

Unknown keys are errors. The four series in the checked-in file are a
starting set of daily weather ladders, not a recommendation.

## Files, under `<data-dir>/opens`

| File | Written by | Contents |
|---|---|---|
| `series/<YYYY-MM-DD>.jsonl` | scan | Kalshi's whole series list, once per UTC day (about 16,000 series, 3MB). |
| `new_series.jsonl` | scan | One line per series that was in no earlier snapshot. |
| `events.jsonl` | scan | The events of scheduled markets: title, `mutually_exclusive`. |
| `events/<series>/<YYYY-MM-DD>.jsonl` | scan | Captured book events; the day is the market's open day (UTC). |
| `captures.jsonl` | scan | One line per captured market: counts, resyncs, `rungs` sharing its connection, `complete`. |
| `backfill.jsonl` | backfill | One row per market. Rewritten atomically each run. |
| `report.csv` | report | The ranked table. |

A book event record (an illustrative delta; the field names are the
real ones):

```json
{"market":"KXWTI-26NOV1614-T86.99","series":"KXWTI","event":"KXWTI-26NOV1614",
 "rung":"Above $86.99","strike_type":"greater","floor":"86.99","open_ms":1794529800000,
 "type":"delta","seq":2,"ts_ms":1794529800120,"recv_ms":1794529800151,
 "side":"no","price":5600,"delta":25000,"ask":4400,"ask_sz":25000}
```

`type` is `snapshot`, `delta` or `stale`. `seq` is the subscription's,
shared by the rungs of the event, so one market's records skip numbers.
`ts_ms` is Kalshi's timestamp
(snapshots have none), `recv_ms` this host's receive time. `price` is
in bps on the side named; `delta` and sizes are hundredths of a
contract. `bid`/`ask` are the best yes bid and ask after the event, each
present only if that side has size. A `stale` field marks a record after
which the book is untrusted, until the next snapshot.

A backfill row has the market's identity and strike, `open_ms`,
`source` (`capture` or `candles`), `quote_ms`, `bid`, `ask`, `mid` (bps),
`bid_sz`, `ask_sz`, the first trade (`trade_ms`, `trade_price`,
`trade_count`, `taker_yes`), `mid_before`, `mid_after`, `result`,
`settle_bps` and `settled_ms`. A row with a `result` is final; a row
built before its market's capture existed is rebuilt once.

## Metrics (`pmde-scan`, `:9101`)

| Metric | Meaning |
|---|---|
| `opens_skipped_total{reason}` | Market opens scheduled but not captured: `concurrency` (cap reached), `late` (already open when due), `vanished` (left the unopened list before `open_time`). Counts markets; an event skipped whole adds one per rung. |
| `opens_captures_total`, `opens_captures_active` | Captures (connections) started; running now. |
| `opens_captured_markets_total` | Market opens covered by those captures. |
| `opens_events_total` | Book events recorded. |
| `opens_capture_resyncs_total{reason}` | A captured market lost its book mid-window. One break counts once per rung on the connection. |
| `opens_scheduled_markets`, `opens_watched_series` | Discovery state. |
| `opens_new_series_total` | Series detected as new. |
| `opens_rest_responses_total{code}` | REST responses, retries included; watch `429`. |
| `opens_poll_errors_total{kind}` | Polls that failed after retries. |
| `opens_writer_blocked_seconds_total` | Book goroutines blocked on the recorder's queue. |

They carry no market or series label: the scanner touches thousands of
short-lived markets, and per-market series would grow without bound.

## The report's columns

- **opens, /day**: markets with a row, per UTC day over the span seen.
- **t→quote**: median time from `open_time` to the first two-sided
  book. Exact from captures; from candlesticks it is an upper bound to
  the minute and is printed with `≤`.
- **bid sz / ask sz, share**: the modal opening size per side, and the
  share of captured opens with exactly that size.
- **spread**: median opening spread, bps.
- **fit rmse**: per event with at least 3 quoted rungs, a normal CDF is
  least-squares fitted to the opening mids across strikes; the column is
  the median RMSE in bps and the number of events. Low means the ladder
  came off a normal model.
- **impact**: median mid change after the first trade, in the taker's
  direction, in bps per contract traded.
- **settled, events**: opens with a yes/no result and an opening quote,
  and the events they belong to. One outcome decides every rung of an
  event, so the event count is the sample size, not the row count.
- **brier**: opening mid against the yes/no result.
- **miscal [90%]**: the decile-weighted mean of |mean mid − realized
  frequency|, in bps, with a 90% interval. `-deciles` prints the table
  behind it.
- **null, excess**: see the next section.
- **score** = miscal (as a fraction of $1) × modal opening size
  (contracts, mean of the two sides) × opens per day. Roughly dollars a
  day. It is blank until a series has both settled opens and captured
  sizes; such series rank below scored ones, by excess × opens/day.

### The noise floor under miscal

`miscal` is a mean of absolute gaps, so it is positive even when the
quotes are exactly right, and larger the fewer events there are: twelve
perfectly calibrated events show a miscal of well over 1000 bps. Three
numbers put it in context. All of them treat the event as the unit.

- **null**: the miscal to expect if the opening mids were perfectly
  calibrated. The mids are kept and the outcomes are simulated from
  them, one uniform draw per event deciding all its rungs: "greater"
  rungs resolve as a nested set (if "above 60" is yes, so is "above
  55"), "less" rungs nested the other way, and buckets one at a time.
  Each rung still resolves yes with exactly the probability its mid
  states. Rungs with no usable strike are treated as mutually exclusive
  alternatives.
- **excess** = miscal − null, floored at zero: the part the noise floor
  does not explain. This is the number to rank on.
- **the 90% interval** comes from resampling whole events with
  replacement. It is the basic bootstrap interval, `[2m − q95, 2m − q05]`
  floored at zero, not the percentile interval: resampling adds noise,
  which adds to a mean of absolute values, so the resamples sit above
  the estimate by about as much as the estimate sits above the truth.
  Reflecting takes that bias out, and it is why the interval can lie
  below the point estimate. With a handful of events it is unreliable;
  read the null and the event count there.

Both use a seeded generator (`-seed`, default 1; `-resamples`, default
2000), keyed on the series name as well, so the same rows give the same
report and one series' numbers do not depend on which others are in it.

**Candle-sourced calibration is a lower bound.** An opening quote read
from a candle close is up to a minute after the first quote, and a
wrong quote may already have been traded back toward fair by then. Where
any of a series' settled opens are candle-sourced, its miscal and excess
are printed with `≥`, the decile table says how many, and the CSV has
`cal_from_candles` and `miscal_is_lower_bound`.

## What the Kalshi API allowed, and the fallbacks

Checked against docs.kalshi.com and the live API on 2026-11-12.

**No historical order book.** Kalshi serves no past book snapshots or
deltas. So:

- Opening **size** exists only for opens the scanner captured itself.
  Backfilled history has `bid_sz`/`ask_sz` empty, and a series' score
  stays blank until captures accumulate.
- For markets without a capture, the opening quote is the **close of the
  first 1-minute candlestick that closes two-sided** (bid > 0, ask <
  $1). That is up to a minute after the true first quote, and it is the
  quote at the end of that minute, not the first one posted.
- Price impact without a capture uses the candle closes before and
  containing the first trade, which also includes every other quote
  change in that minute.

**Trades are returned newest first**, with no sort parameter and no
documented order. The backfill reads the window from `open_time` to
`open_time + horizon` (default 1h) and sorts it. A market whose first
trade comes later than the horizon has no first trade in its row.

**Historical cutoff.** Markets settled before `GET /historical/cutoff`'s
`market_settled_ts` (2026-09-13 when checked) are only in
`/historical/markets`, with candlesticks and trades under `/historical`
too. The historical candlesticks name their fields `open`/`close`, the
live ones `open_dollars`/`close_dollars`; both are decoded.
`/historical/markets` has no time filter, so reaching back past the
cutoff pages through a series' whole archive.

**Rate limits.** The documented budget is 200 read tokens/s at 10 per
request on the lowest tier, with no `Retry-After` on 429s. In practice
unauthenticated calls draw on a smaller, undocumented per-IP budget: 4
requests/s drew occasional 429s from `/markets/trades`. The REST client
defaults to 2 requests/s, at most 2 in flight, retries 429/5xx/transport
errors with jittered exponential backoff (5 retries), and a 429 holds
every caller back. The REST calls are not signed; nothing here needs
auth.

**Subscribing before the open works.** A subscription to an
`initialized` market is accepted and answered with an empty snapshot;
the first deltas are the opening orders. This is not documented; it was
observed (`testdata/kalshi_open_capture.jsonl` is that session).

**One subscription covers an event.** A subscribe with `market_tickers`
puts every market on one subscription with one `seq` across them. The
docs say only that seq "counts messages on one subscription"; the
snapshots-then-interleaved-deltas order was observed on a live
two-market subscription. Two limits are documented without numbers: a
per-subscription market limit (error 26) and a command rate limit
(error 27). A ladder wider than the first would fail to subscribe; the
capture logs the venue's error, retries, and ends incomplete. The
connection limit is 200 on the lower tiers, shared with `pmde`.

**The market object has no series ticker.** Discovery asks for each
watched series' unopened markets by `series_ticker`, so the series is
known from the query. It never scans all unopened markets (over 12,000
when checked).

## Assumptions

- A new series is one whose ticker was in no earlier snapshot. The very
  first snapshot is the baseline and flags nothing. Between daily full
  fetches, new series are found with `min_updated_ts`, assuming a new
  series' `last_updated_ts` is its creation time; if not, it is found
  at the next day's full fetch.
- "Opening quote" means the first moment the book has both a bid and an
  ask. A maker who posts in several steps is read at the first step
  that makes the book two-sided, so the size may be partial.
- A capture that lost its book before its first two-sided quote is not
  used for the opening quote (candles are); one that lost it later
  still is.
- All fills printed at the first trade's timestamp with the same taker
  side are treated as one order; its size is their sum.
- The impact's "after" is the book 5s after the trade, from a capture.
- The mid is `(bid + ask) / 2` in integer bps; on sub-cent ticks half a
  basis point rounds down.
- The normal fit treats each rung's opening mid as the probability of
  its strike interval. For whole-number underlyings (degrees, goals)
  the real boundaries are half a unit off the strikes; the API does not
  say which underlyings those are, so the fit runs both ways and keeps
  the better. Rungs with `functional`, `custom` or `structured` strikes
  are left out, and events need 3 fitted rungs.
- Calibration uses yes/no results only (not scalar or void), and
  treats rungs of one event as independent, which they are not: one
  event's outcome decides all its rungs. With few events the decile
  table is noisy; read it by event count, not row count.
- Multivariate (combo) markets are excluded everywhere.

## Not built

- No splitting of an event across connections if it is wider than
  Kalshi's undocumented per-subscription market limit.
- No trade channel capture: first trades come from REST afterwards.
- No persistence of the schedule: it is rebuilt from a poll at startup,
  so an open within the first poll after a restart can be missed.
- No Alloy scrape config, dashboard or alerts for the `opens_*` metrics.
- The series list snapshot is not pruned.

## First live run (2026-11-13 UTC)

A short run of `pmde-scan` on KXWTI: 30 rungs of one event opened at
00:30:00Z. With `max_concurrent: 20`, 20 were captured (subscribed about
4.8s before the open, each with its empty snapshot) and 10 were counted
as skipped. No order was posted on any of the 20 in the first 60s, so
their opening quotes come from candlesticks.

`pmde-opens backfill -since 2026-11-06` on KXHIGHNY, KXHIGHCHI and KXWTI
built 244 rows in a little over two minutes (237 quotes from
candlesticks, 7 markets with no two-sided quote in the first hour).
With a week of data and no captured sizes the table is not yet evidence
of anything; it shows the pipeline produces every column.
