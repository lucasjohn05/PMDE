package analysis

import (
	"strings"
	"testing"
	"time"

	"github.com/lucasjohn05/pmde/internal/storage"
)

func TestQuoteAt(t *testing.T) {
	// Three ticks in one ms, as Polymarket's frames arrive: the last one
	// written is the state after that ms.
	recs := []storage.TickRecord{
		ok(1000, 9740, 9780, 1, 1),
		ok(2000, 9740, 9790, 1, 1),
		ok(2000, 9740, 9800, 1, 1),
		stale(3000),
	}
	for _, tc := range []struct {
		at      int64
		wantT   int64
		wantAsk int
		found   bool
	}{
		{999, 0, 0, false},
		{1000, 1000, 9780, true},
		{1999, 1000, 9780, true},
		{2000, 2000, 9800, true},
		{3500, 3000, 0, true},
	} {
		rec, found := QuoteAt(recs, time.UnixMilli(tc.at))
		if found != tc.found || rec.T != tc.wantT || (rec.Ask != nil) != (tc.wantAsk != 0) || (rec.Ask != nil && *rec.Ask != tc.wantAsk) {
			t.Errorf("QuoteAt(%d) = %+v, %v; want t=%d ask=%d found=%v", tc.at, rec, found, tc.wantT, tc.wantAsk, tc.found)
		}
	}

	var b strings.Builder
	at := time.UnixMilli(2500).In(time.FixedZone("ET", -4*3600))
	if err := WriteQuotes(&b, "hold", recs[3:], recs[:3], []time.Time{at}); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"Kalshi      no tick at or before this time",
		"Polymarket  bid 0.9740  ask 0.9800  (9740/9800 bps), tick at 20:00:02.000",
	} {
		if !strings.Contains(b.String(), want) {
			t.Errorf("output lacks %q:\n%s", want, b.String())
		}
	}
}
