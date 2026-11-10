# Kalshi vs Polymarket on the October 2026 FOMC decision

## Result

Kalshi's FOMC markets close at 1:59 PM ET (the market's `close_time`), one
minute before the 2:00 PM statement. Only Polymarket is trading when the
news lands, so this setup can't measure cross-venue price discovery at an
FOMC decision. December also closes at 1:59, so this likely holds for
Kalshi's FOMC markets in general.

## Setup

PMDE records both venues' WebSocket order books and stores every
top-of-book change. A book is trusted when it was built from a fresh
snapshot, nothing has been missed or dropped since, and both sides have
a level. Statistics cover only time when both books are trusted.

Two pairs resolved on the October 28 2026 decision (a hold):
`KXFEDDECISION-26OCT-H0` vs Polymarket "no change" (hold), and
`KXFEDDECISION-26OCT-H25` vs "increase 25 bps" (hike-25).

A crossing is one venue's best bid above the other's best ask, priced as
two taker orders at the smaller top-of-book size. Kalshi's fee is 0.07 ×
C × P × (1 − P), rounded up to the cent per order. Polymarket's taker
fee was 0. The stress case charges Polymarket 0.05 × C × p × (1 − p),
the same form at a 0.05 rate, not 5% of notional.

The window is 9:30 AM ET to Kalshi's 1:59 PM close on October 28, 4h29m
in all. Both books were trusted for 100% of it on both pairs.

## Run-up

| | hold | hike-25 |
|---|---|---|
| mean mid gap, Kalshi − Polymarket | −16.35 bps | +37.34 bps |
| crossed, before fees | 35m24s (13.1%) | 57m15s (21.2%) |
| crossed, after fees | 6m45s (2.5%) | 24m14s (9.0%) |
| crossed, after fees at the 0.05 stress rate | 3.5s | 2.2s |
| crossing episodes | 39 | 38 |

The most any crossing would have netted after fees at a single moment
was $76.56. That came on hold, in an episode starting at 1:33:36 PM ET
that lasted 8m44s, of which 4m51s was net-positive. Treat it as a
ceiling: it assumes both top-of-book quotes fill in full at that instant.
On hike-25 the ceiling was $11.84.

Kalshi's mid, sampled each second, changed in only 7 seconds of the
window on hold and 3 on hike-25, against 515 and 670 for Polymarket.
Nearly all gap movement came from Polymarket moving around a mostly
static Kalshi book.

The gaps were real, but after fees they were small and short-lived, and
the 0.05-rate Polymarket taker fee of the stress case would remove them
almost entirely.

## At the decision

Polymarket repriced within 4.1 s on hold and 4.5 s on hike-25. The
quotes and times in this section come from the recorded ticks, not from
the trade history:

"First at the new level" is the first tick where hold's bid reached 0.997
and hike-25's ask reached 0.009, levels neither gave back before its book
lost a side.

| | last quote before 2:00:00 | first at the new level |
|---|---|---|
| hold | 0.974 / 0.980 | 0.997 / 0.998 at 2:00:04.115 |
| hike-25 | 0.016 / 0.038 | 0.002 / 0.009 at 2:00:04.537 |

```
pmde-analyze -config pairs-2026-10.yaml -data-dir <data> \
  -quotes-at 2026-10-28T13:59:59.999-04:00,2026-10-28T14:00:04.114-04:00,\
2026-10-28T14:00:04.115-04:00,2026-10-28T14:00:04.536-04:00,\
2026-10-28T14:00:04.537-04:00
```

Data recorded by PMDE, available on request.

At 2:00:13.6 the hold book lost a side, and the hike-25 book did the same
at 2:00:25.3. Each was pinned at the price limit. The tick size was
0.001, so a 0.999 bid on hold (inferred from trade prices) leaves no room
for an ask, and a 0.001 ask on hike-25 (inferred from trade prices)
leaves no room for a bid.

Trading continued at the limit. According to Polymarket's trade history,
hold had 231 trades after that point, 222 of them at 0.999, the last at
4:00:58 PM ET. Hike-25 had 19, of which 18 were at 0.001, the last at
3:50:56 PM ET. The recorder wrote nothing during that time because it
stores only two-sided quotes.

## Caveats

- One meeting, with an outcome the market expected: hold was 0.974 /
  0.980 just before the statement.
- Polymarket here is the international venue, closed to US persons, so
  these gaps aren't tradeable from the US.
- Not modelled: resolution-rule differences, capital lock-up, depth
  beyond the top of book, quote decay.
- Timestamps are local receive times. The two feeds' receive latency
  differs by about 27 ms, so leads shorter than that can't be resolved.
- Polymarket trade times are whole-second block times, which lag the
  match.
- Whether Polymarket sent book updates during the pinned period can't be
  determined.

## Next

Confirm the result with the December 9 meeting. After that, move to
markets where both venues stay open through the news, such as live
events or markets on unscheduled news. That's where cross-venue lead/lag
can actually be measured.
