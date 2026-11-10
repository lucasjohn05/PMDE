package analysis

import (
	"fmt"
	"io"
	"time"

	"github.com/lucasjohn05/pmde/internal/storage"
)

// QuoteAt returns the book state in effect at at: the last record with
// T <= at, in file order (several ticks can share a millisecond; the
// last one written is the state after that millisecond). ok is false if
// no record is that old.
func QuoteAt(recs []storage.TickRecord, at time.Time) (rec storage.TickRecord, ok bool) {
	ms := at.UnixMilli()
	for _, r := range recs {
		if r.T > ms {
			continue
		}
		if !ok || r.T >= rec.T {
			rec, ok = r, true
		}
	}
	return rec, ok
}

// WriteQuotes writes both venues' quotes for a pair at each of ats, as
// stored in the tick files. Times print in ats' own zones.
func WriteQuotes(w io.Writer, pair string, k, p []storage.TickRecord, ats []time.Time) error {
	if _, err := fmt.Fprintf(w, "== %s\n", pair); err != nil {
		return err
	}
	for _, at := range ats {
		for _, v := range []struct {
			name string
			recs []storage.TickRecord
		}{{"Kalshi", k}, {"Polymarket", p}} {
			rec, ok := QuoteAt(v.recs, at)
			if _, err := fmt.Fprintf(w, "%s  %-10s  %s\n", at.Format("2006-01-02T15:04:05.000Z07:00"), v.name, describe(rec, ok, at.Location())); err != nil {
				return err
			}
		}
	}
	return nil
}

func describe(rec storage.TickRecord, ok bool, loc *time.Location) string {
	if !ok {
		return "no tick at or before this time"
	}
	from := "tick at " + time.UnixMilli(rec.T).In(loc).Format("15:04:05.000")
	if !rec.OK || rec.Bid == nil || rec.Ask == nil {
		return "no two-sided quote (ok:false), " + from
	}
	return fmt.Sprintf("bid %s  ask %s  (%d/%d bps), %s", price(*rec.Bid), price(*rec.Ask), *rec.Bid, *rec.Ask, from)
}

// price formats bps as dollars, exactly: 9740 -> 0.9740.
func price(bps int) string {
	return fmt.Sprintf("%d.%04d", bps/10000, bps%10000)
}
