package opens

import (
	"context"
	"testing"
	"time"
)

func TestQuoteRule(t *testing.T) {
	r := DefaultQuoteRule()
	for _, c := range []struct {
		name         string
		bid, ask     int
		bidSz, askSz int64
		want         bool
	}{
		{"10c wide, 10 a side", 4000, 5000, 1000, 1000, true},
		{"just over 10c", 4000, 5001, 1000, 1000, false},
		{"1c/99c stub", 100, 9900, 100000, 100000, false},
		{"9.99 contracts", 4000, 4200, 999, 1000, false},
		{"crossed", 4200, 4000, 1000, 1000, false},
		{"no bid", 0, 500, 1000, 1000, false},
		{"no ask", 9500, 10000, 1000, 1000, false},
	} {
		if got := r.Meets(c.bid, c.ask, c.bidSz, c.askSz); got != c.want {
			t.Errorf("%s: Meets = %v, want %v", c.name, got, c.want)
		}
	}
	if !r.MeetsSpread(4000, 5000) || r.MeetsSpread(100, 9900) {
		t.Errorf("MeetsSpread")
	}
}

// The opening quote is the first moment both sides have a level of at
// least the minimum size within the spread cap. Stubs and sub-contract
// orders are passed over.
func TestOpeningUnderTheRule(t *testing.T) {
	half := func(seq, ms int64, side string, price int, centi int64) EventRecord {
		r := delta(seq, ms, side, price, 0)
		r.Delta = int64p(centi)
		return r
	}
	events := withTop([]EventRecord{
		{Type: "snapshot", Seq: 1, RecvMs: bfOpen.UnixMilli() - 4000},
		// +1s: a 1c/99c stub, 100 a side. Two-sided, not a quote.
		delta(2, 1000, "yes", 100, 100),
		delta(3, 1000, "no", 100, 100),
		// +2s: 5 a side at 40/48c. Inside the spread cap, too small.
		delta(4, 2000, "yes", 4000, 5),
		delta(5, 2000, "no", 5200, 5),
		// +3s: half a contract bid at 45c, in front.
		half(6, 3000, "yes", 4500, 50),
		// +4s: both 40/48 levels grow to 20: the quote, read behind the
		// half contract.
		delta(7, 4000, "yes", 4000, 15),
		delta(8, 4100, "no", 5200, 15),
	})
	q, ok := openingFromEvents(events, DefaultQuoteRule())
	if !ok || q.bid != 4000 || q.ask != 4800 || q.bidSz != 2000 || q.askSz != 2000 || q.ms != bfOpen.UnixMilli()+4100 {
		t.Errorf("opening %+v, %v; want 40/48c, 20 a side, at +4.1s", q, ok)
	}
	// The best bid on file at that moment is the half contract.
	if b := *events[len(events)-1].Bid; b != 4500 {
		t.Errorf("fixture: best bid %d, want the 45c half contract", b)
	}
	// Under a looser rule the stub is the quote.
	if q, ok := openingFromEvents(events, anyQuote); !ok || q.bid != 100 || q.ask != 9900 {
		t.Errorf("anyQuote opening %+v, %v; want the stub", q, ok)
	}
	// A book never meeting the rule has none.
	if _, ok := openingFromEvents(events[:6], DefaultQuoteRule()); ok {
		t.Errorf("an opening from stubs and 5-lots")
	}
}

func TestParseConfigQuote(t *testing.T) {
	c, err := ParseConfig([]byte("series: [A]\nquote:\n  max_spread_bps: 500\n  min_size: 2.5\n"))
	if err != nil {
		t.Fatal(err)
	}
	if c.Quote != (QuoteRule{MaxSpreadBps: 500, MinSizeCenti: 250}) {
		t.Errorf("quote %+v", c.Quote)
	}
	if c, _ := ParseConfig([]byte("series: [A]\n")); c.Quote != DefaultQuoteRule() {
		t.Errorf("default quote %+v", c.Quote)
	}
	for _, bad := range []string{
		"series: [A]\nquote:\n  min_size: 0.5\n",
		"series: [A]\nquote:\n  max_spread_bps: 0\n",
		"series: [A]\nquote:\n  max_spread_bps: 10000\n",
		"series: [A]\nquote:\n  spread: 5\n",
	} {
		if _, err := ParseConfig([]byte(bad)); err == nil {
			t.Errorf("no error for %q", bad)
		}
	}
}

// A candle quote wider than the cap is passed over for the first one
// inside it, and rows built under another rule are rebuilt.
func TestBackfillQuoteRule(t *testing.T) {
	fake := &backfillFake{
		markets: map[string]string{"S-D1-T66": settledJSON("S-D1-T66", "S-D1", "no", "0.0000", 66, bfOpen, bfOpen.Add(45*time.Hour))},
		settled: map[string][]string{"S": {"S-D1-T66"}},
		candles: map[string]string{"S-D1-T66": `{"candlesticks":[` +
			candle(1, "0.0100", "0.0100", "0.9900", "0.9900") + "," + // stub
			candle(2, "0.2000", "0.2000", "0.4000", "0.4000") + "," + // 20c wide
			candle(3, "0.2500", "0.2500", "0.3200", "0.3200") + `]}`}, // 7c
	}
	b := newBackfill(t, fake, "S")
	b.Rule = QuoteRule{MaxSpreadBps: 2000, MinSizeCenti: 1000}
	if _, err := b.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if r := rowsByMarket(t, b.Dir)["S-D1-T66"]; *r.Bid != 2000 || *r.Ask != 4000 || r.RuleSpread != 2000 || r.RuleSize != 1000 {
		t.Errorf("under a 20c cap: %+v", r)
	}
	// The settled row is final under the same rule, rebuilt under
	// another.
	if stats, _ := b.Run(context.Background()); stats.Built != 0 {
		t.Errorf("same rule rebuilt %d", stats.Built)
	}
	b.Rule = DefaultQuoteRule()
	if stats, err := b.Run(context.Background()); err != nil || stats.Built != 1 {
		t.Fatalf("new rule: %+v, %v", stats, err)
	}
	rows, _ := LoadRows(b.Dir)
	if r := rows[0]; *r.Bid != 2500 || *r.Ask != 3200 || r.RuleSpread != 1000 {
		t.Errorf("under the default rule: %+v", r)
	}
	if n := BuiltUnderOtherRule(rows, QuoteRule{MaxSpreadBps: 300, MinSizeCenti: 1000}); n != 1 {
		t.Errorf("BuiltUnderOtherRule %d", n)
	}
}

// The report holds every row to its rule: a captured quote too small, or
// a candle quote too wide, is no quote.
func TestReportAppliesTheRule(t *testing.T) {
	good := quoted("S", "S-E1", "S-1", repOpen, 4000, 300, "")
	small := quoted("S", "S-E2", "S-2", repOpen, 4000, 300, "")
	small.Source, small.BidSz, small.AskSz = SourceCapture, int64p(500), int64p(5000)
	wide := quoted("S", "S-E3", "S-3", repOpen, 5000, 4900, "")
	r := Report([]OpenRow{good, small, wide}, Options{Seed: 1, Resamples: 10})[0]
	if r.Opens != 3 || r.Quoted != 1 || r.SpreadBps != 600 || r.Sized != 0 {
		t.Errorf("default rule: %+v", r)
	}
	if r := Report([]OpenRow{good, small, wide}, Options{Seed: 1, Resamples: 10, Rule: anyQuote})[0]; r.Quoted != 3 || r.Sized != 1 {
		t.Errorf("anyQuote: quoted %d sized %d", r.Quoted, r.Sized)
	}
}

// Triage holds candle quotes to the spread cap: a stub book is not
// quoted.
func TestTriageSpreadCap(t *testing.T) {
	fake := &triageFake{markets: map[string][]string{}, candles: map[string]string{}}
	day := time.Date(2026, 11, 2, 15, 0, 0, 0, time.UTC)
	fake.markets["STUB"] = []string{settledJSON("STUB-A", "STUB-E", "no", "0.0000", 1, day, day.Add(30*time.Hour))}
	fake.candles["STUB-A"] = triageCandles(day, 1, 100, 9900)
	rows, err := newTriage(t, fake, "STUB").Run(context.Background())
	if err != nil || len(rows) != 1 {
		t.Fatal(rows, err)
	}
	if r := rows[0]; r.Sampled != 1 || r.Quoted != 0 || r.QuotedFirstMinute != 0 {
		t.Errorf("stub book counted as quoted: %+v", r)
	}
	tr := newTriage(t, fake, "STUB")
	tr.Rule = anyQuote
	if rows, _ := tr.Run(context.Background()); rows[0].QuotedFirstMinute != 1 {
		t.Errorf("anyQuote: %+v", rows[0])
	}
}

// The checked-in config parses, with the default rule spelled out.
func TestRepoOpensConfig(t *testing.T) {
	c, err := LoadConfig("../../opens.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if c.Quote != DefaultQuoteRule() || len(c.Series) == 0 {
		t.Errorf("opens.yaml: quote %+v, %d series", c.Quote, len(c.Series))
	}
}
