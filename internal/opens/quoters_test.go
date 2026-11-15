package opens

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/lucasjohn05/pmde/internal/storage"
)

// add is a delta adding contracts at a price on one side, at ms.
func add(market string, open, ms int64, seq int64, side string, price int, contracts int64) EventRecord {
	return EventRecord{Market: market, Series: "Q", Event: "Q-1", OpenMs: open, Type: "delta", Seq: seq,
		TsMs: ms, RecvMs: ms, Side: side, Price: intp(price), Delta: int64p(contracts * 100)}
}

func TestPlacements(t *testing.T) {
	open := int64(1_000_000)
	evs := []EventRecord{
		add("M", open, open+100, 1, "yes", 4400, 200), // bid 44c x 200
		add("M", open, open+120, 2, "no", 5200, 200),  // ask 48c x 200, 20ms later: one placement
		add("M", open, open+900, 3, "yes", 4000, 50),  // a lone bid
		add("M", open, open+1300, 4, "no", 5000, 50),  // ask 400ms after it: too late to pair
		add("M", open, open+2000, 5, "yes", 100, 500), // 1c / 99c stub: wider than the rule
		add("M", open, open+2001, 6, "no", 100, 500),
		add("M", open, open+3000, 7, "yes", 3000, 5), // 5 contracts: under the rule's size
		add("M", open, open+3001, 8, "no", 6800, 5),
	}
	ps := placements(evs, DefaultQuoteRule())
	if len(ps) != 1 || ps[0].bidSz != 20000 || ps[0].askSz != 20000 || ps[0].spread != 400 || ps[0].delayMs != 120 {
		t.Fatalf("placements %+v", ps)
	}
	// a hole ends the market
	hole := append([]EventRecord{{Market: "M", Type: "stale", Stale: "seq_gap", Seq: 0}}, evs...)
	if ps := placements(hole, DefaultQuoteRule()); len(ps) != 0 {
		t.Errorf("read past a hole: %+v", ps)
	}
}

func TestClusterQuoters(t *testing.T) {
	p := func(market string, d int64, bid, ask int64, spread int) placement {
		return placement{bidSz: bid, askSz: ask, spread: spread, market: market, day: d, delayMs: 5000}
	}
	ps := []placement{
		// the big maker: 200 x 200 at 4c on three markets over two days
		p("A", 1, 20000, 20000, 400), p("B", 1, 20000, 20000, 400), p("C", 2, 20000, 20000, 400),
		// a smaller one: 10 x 10 at 8c, twice on one market but two days
		p("A", 1, 1000, 1000, 800), p("A", 2, 1000, 1000, 800),
		// a one-off
		p("B", 1, 3300, 3300, 600),
	}
	q := clusterQuoters(ps, 3)
	if q.Count() != 2 || q.OneOffs != 1 || q.Placements != 6 {
		t.Fatalf("quoters %+v", q)
	}
	top := q.Quoters[0]
	if top.BidSz != 20000 || top.SpreadBps != 400 || top.Markets != 3 || top.Days != 2 || top.Placements != 3 {
		t.Errorf("top %+v", top)
	}
	total := 3*40000 + 2*2000 + 6600
	if got, want := q.TopShare(), 120000.0/float64(total); got != want {
		t.Errorf("top share %v, want %v", got, want)
	}
	if got, want := q.OneOffShare, 6600.0/float64(total); got != want {
		t.Errorf("one-off share %v, want %v", got, want)
	}
}

func TestLoadQuoters(t *testing.T) {
	dir := t.TempDir()
	open := time.Date(2026, 11, 14, 13, 0, 0, 0, time.UTC).UnixMilli()
	var evs []EventRecord
	for i, m := range []string{"Q-1-A", "Q-1-B"} {
		evs = append(evs, add(m, open, open+5000, int64(2*i+1), "yes", 2600, 200), add(m, open, open+5010, int64(2*i+2), "no", 7000, 200))
	}
	if err := storage.WriteJSONL(filepath.Join(dir, eventsDir, "Q", "2026-11-14.jsonl"), evs); err != nil {
		t.Fatal(err)
	}
	got, err := LoadQuoters(dir, time.UnixMilli(open-1), time.UnixMilli(open+1), DefaultQuoteRule())
	if err != nil {
		t.Fatal(err)
	}
	q := got["Q"]
	if q.Markets != 2 || q.Count() != 1 || q.TopShare() != 1 || q.Quoters[0].SpreadBps != 400 {
		t.Errorf("%+v", q)
	}
	if got, _ := LoadQuoters(dir, time.UnixMilli(open+1), time.UnixMilli(open+2), DefaultQuoteRule()); len(got) != 0 {
		t.Errorf("outside the period: %+v", got)
	}
}
