package analysis

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/lucasjohn05/pmde/internal/divergence"
	"github.com/lucasjohn05/pmde/internal/storage"
)

var testPair = divergence.Pair{Name: "hold", Kalshi: "KX-H0", Polymarket: "111"}

var testFees = Fees{
	Kalshi:     VenueFees{Venue: "Kalshi", TakerRatePPM: 70000, Checked: "x", Sources: []string{"k"}},
	Polymarket: VenueFees{Venue: "Polymarket intl", TakerRatePPM: 0, StressTakerRatePPM: 50000, Checked: "x", Sources: []string{"p"}},
}

// ok is a trusted tick; sizes are in whole contracts/shares.
func ok(t int64, bid, ask int, bidSz, askSz int64) storage.TickRecord {
	bs, as := bidSz*100, askSz*100
	return storage.TickRecord{T: t, Bid: &bid, Ask: &ask, BidSize: &bs, AskSize: &as, OK: true}
}

func stale(t int64) storage.TickRecord { return storage.TickRecord{T: t} }

func runRecs(id string, start int64, alive ...int64) []storage.RunRecord {
	recs := []storage.RunRecord{{Ev: "start", T: start, Run: id, Pairs: []string{testPair.Name}}}
	for _, a := range alive {
		recs = append(recs, storage.RunRecord{Ev: "alive", T: a, Run: id})
	}
	return recs
}

func analyze(k, p []storage.TickRecord, runs []storage.RunRecord, from, to int64, mod ...func(*Input)) Report {
	in := Input{Pair: testPair, Kalshi: k, Polymarket: p, Runs: runs,
		From: time.UnixMilli(from), To: time.UnixMilli(to), Fees: testFees, Opts: DefaultOptions()}
	for _, f := range mod {
		f(&in)
	}
	return Analyze(in)
}

func TestCoverage(t *testing.T) {
	pair := testPair
	pair.KalshiLastTrade = "1970-01-01T00:00:09-00:00" // 9000 ms
	k := []storage.TickRecord{ok(0, 2100, 2200, 10, 10)}
	p := []storage.TickRecord{ok(1000, 1800, 1900, 10, 10), stale(5000), ok(6000, 1800, 1900, 10, 10)}
	runs := runRecs("r1", 0, 10000) // up [0, 10000), then down
	r := analyze(k, p, runs, 0, 12000, func(in *Input) { in.Pair = pair })

	c := r.Coverage
	want := Coverage{TotalMs: 12000, CoveredMs: 7000, AfterLastTradeMs: 3000, PolymarketUntrustedMs: 2000, Runs: 1}
	if c != want {
		t.Fatalf("coverage = %+v\nwant       %+v", c, want)
	}

	// Without the cutoff, the time after the run's last heartbeat is down.
	r = analyze(k, p, runs, 0, 12000)
	if r.Coverage.CoveredMs != 8000 || r.Coverage.DownMs != 2000 {
		t.Fatalf("coverage without cutoff = %+v", r.Coverage)
	}
}

// TestRunResetsBooks: a new run starts with no book, even though the
// previous run's last ticks said trusted (a clean stop doesn't store the
// final untrusted tick, and a crash stores nothing).
func TestRunResetsBooks(t *testing.T) {
	k := []storage.TickRecord{ok(0, 2100, 2200, 10, 10), ok(20000, 2100, 2200, 10, 10)}
	p := []storage.TickRecord{ok(0, 1800, 1900, 10, 10), ok(25000, 1800, 1900, 10, 10)}
	runs := append(runRecs("a", 0, 5000), runRecs("b", 10000, 30000)...)
	r := analyze(k, p, runs, 0, 30000)
	c := r.Coverage
	// Up [0,5000) covered; down [5000,10000); run b: both untrusted until
	// 20000, Polymarket until 25000, covered [25000,30000).
	want := Coverage{TotalMs: 30000, CoveredMs: 10000, DownMs: 5000, BothUntrustedMs: 10000, PolymarketUntrustedMs: 5000, Runs: 2}
	if c != want {
		t.Fatalf("coverage = %+v\nwant       %+v", c, want)
	}
}

// TestRunsOfOtherPairs: a run whose start line doesn't list the pair
// didn't subscribe to its markets.
func TestRunsOfOtherPairs(t *testing.T) {
	k := []storage.TickRecord{ok(0, 2100, 2200, 10, 10)}
	p := []storage.TickRecord{ok(0, 1800, 1900, 10, 10)}
	runs := []storage.RunRecord{{Ev: "start", T: 0, Run: "x", Pairs: []string{"other"}}, {Ev: "alive", T: 9000, Run: "x"}}
	r := analyze(k, p, runs, 0, 10000)
	if r.Coverage.CoveredMs != 0 || r.Coverage.DownMs != 10000 || len(r.Warnings) != 1 {
		t.Fatalf("coverage = %+v, warnings %v", r.Coverage, r.Warnings)
	}
}

func TestMidStats(t *testing.T) {
	// Mid divergence: +300 for 6s, +200 for 3s, -100 for 1s.
	k := []storage.TickRecord{ok(0, 2100, 2200, 1, 1), ok(6000, 2000, 2100, 1, 1), ok(9000, 1700, 1800, 1, 1)}
	p := []storage.TickRecord{ok(0, 1800, 1900, 1, 1)}
	r := analyze(k, p, runRecs("r", 0, 10000), 0, 10000)
	want := MidStats{MeanCentiBps: (300*6 + 200*3 - 100*1) * 100 / 10, MedianBps: 300, P5Bps: -100, P95Bps: 300, MaxAbsBps: 300}
	if r.Mid != want {
		t.Fatalf("mid = %+v, want %+v", r.Mid, want)
	}
}

func TestCrossingEpisodes(t *testing.T) {
	k := []storage.TickRecord{
		ok(0, 2100, 2200, 10, 50),   // crosses Polymarket's 1900 ask by 200; size min(10, 7) = 7
		ok(2000, 2200, 2300, 3, 50), // gap 300, size min(3, 7) = 3
		ok(4000, 1900, 2000, 3, 50), // gap 0: episode ends
		ok(6000, 1700, 1750, 3, 1),  // Polymarket bid 1800 > Kalshi ask 1750: other direction, gap 50, size 1
	}
	p := []storage.TickRecord{ok(0, 1800, 1900, 4, 7)}
	r := analyze(k, p, runRecs("r", 0, 10000), 0, 10000)
	g := r.Gap
	if g.CrossedMs != 8000 || len(g.Episodes) != 2 {
		t.Fatalf("gap = %+v", g)
	}
	e := g.Episodes[0]
	if e.DurationMs != 4000 || e.Direction != SellKalshiBuyPolymarket || e.Start.UnixMilli() != 0 {
		t.Fatalf("episode 0 = %+v", e)
	}
	s := e.Snapshot // at the max gap, 300 at 2000ms
	// Kalshi: 0.07 x 3 x 0.22 x 0.78 = $0.036036 -> $0.04; per contract 1.333c.
	// Polymarket rate 0. Stress: 3 x 0.05 x 0.19 x 0.81 = 0.023085 -> 0.02309 (5 decimals, half up... 0.0230850 -> 0.02309).
	want := Crossing{
		At: time.UnixMilli(2000).UTC(), GapBps: 300, KalshiPriceBps: 2200, PolymarketPriceBps: 1900, SizeCenti: 300,
		GrossMicro: 90_000, KalshiFeeMicro: 40_000, KalshiFeePerContractMicro: 13_333,
		PolymarketFeeMicro: 0, PolymarketStressFeeMicro: 23_090,
		NetMicro: 50_000, NetStressMicro: 26_910,
		NetPerContractCentiBps: 16_666, NetStressPerContractCentiBps: 8_970,
	}
	if s != want {
		t.Fatalf("snapshot = %+v\nwant       %+v", s, want)
	}
	// First 2s: gap 200 x 7 = $0.14 gross; Kalshi 0.07 x 7 x .21 x .79 = $0.081291 -> $0.09;
	// net $0.05 > 0. So net > 0 throughout the episode.
	if e.NetPositiveMs != 4000 || e.MaxNetMicro != 50_000 {
		t.Fatalf("episode 0 net = %+v", e)
	}

	e = g.Episodes[1]
	// Gap 50 on 1 contract: $0.005 gross, Kalshi fee at 17.5c rounds up to $0.02: net negative.
	if e.Direction != BuyKalshiSellPolymarket || e.DurationMs != 4000 || e.Snapshot.NetMicro != 5000-20000 || e.NetPositiveMs != 0 {
		t.Fatalf("episode 1 = %+v", e)
	}
	if g.NetPositiveMs != 4000 {
		t.Fatalf("net positive = %d", g.NetPositiveMs)
	}
}

// TestEpisodeEndsOnUntrusted: losing a book ends a crossing episode
// rather than bridging the gap.
func TestEpisodeEndsOnUntrusted(t *testing.T) {
	k := []storage.TickRecord{ok(0, 2100, 2200, 1, 1), stale(1000), ok(2000, 2100, 2200, 1, 1)}
	p := []storage.TickRecord{ok(0, 1800, 1900, 1, 1)}
	r := analyze(k, p, runRecs("r", 0, 4000), 0, 4000)
	if len(r.Gap.Episodes) != 2 || r.Gap.CrossedMs != 3000 {
		t.Fatalf("gap = %+v", r.Gap)
	}
}

func TestFollows(t *testing.T) {
	const s = 1000
	k := []storage.TickRecord{
		ok(0, 2000, 2100, 1, 1),
		ok(10*s, 2100, 2200, 1, 1),     // Kalshi up at 10s: leader
		ok(40*s+500, 2200, 2300, 1, 1), // Kalshi up at 40.5s: follows Polymarket's 40s move
		ok(70*s, 2300, 2400, 1, 1),     // up at 70s, same ms as Polymarket: simultaneous
		ok(200*s, 2200, 2300, 1, 1),    // down at 200s: leader, never followed, covered after: not followed
		ok(590*s, 2100, 2200, 1, 1),    // down at 590s: window runs past the end: censored
	}
	p := []storage.TickRecord{
		ok(0, 2000, 2100, 1, 1),
		ok(10*s+300, 2100, 2200, 1, 1), // follows Kalshi's 10s move after 300ms
		ok(40*s, 2200, 2300, 1, 1),     // Polymarket leads at 40s
		ok(70*s, 2300, 2400, 1, 1),     // simultaneous
	}
	r := analyze(k, p, runRecs("r", 0, 600*s), 0, 600*s)
	ll := r.LeadLag
	if ll.Caveat != TCaveat {
		t.Errorf("caveat = %q", ll.Caveat)
	}
	kf, pf := ll.KalshiFirst, ll.PolymarketFirst
	if kf.Leaders != 3 || kf.Followed != 1 || kf.Censored != 1 || *kf.MedianMs != 300 {
		t.Errorf("Kalshi first = %+v", kf)
	}
	if pf.Leaders != 1 || pf.Followed != 1 || *pf.MedianMs != 500 {
		t.Errorf("Polymarket first = %+v", pf)
	}
	if ll.Simultaneous != 1 {
		t.Errorf("simultaneous = %d", ll.Simultaneous)
	}
}

// TestMoveThreshold: a mid change below -move-bps isn't a move.
func TestMoveThreshold(t *testing.T) {
	k := []storage.TickRecord{ok(0, 2000, 2100, 1, 1), ok(1000, 2000, 2150, 1, 1)} // mid +25
	p := []storage.TickRecord{ok(0, 2000, 2100, 1, 1)}
	r := analyze(k, p, runRecs("r", 0, 5000), 0, 5000)
	if r.LeadLag.KalshiFirst.Leaders != 0 {
		t.Fatalf("Kalshi first = %+v", r.LeadLag.KalshiFirst)
	}
}

func TestCorrelationPeak(t *testing.T) {
	// Kalshi's mid steps every 10s with a varying pattern; Polymarket's
	// does the same 2s later. The 1s changes correlate perfectly at +2s.
	var k, p []storage.TickRecord
	steps := []int{100, -100, 200, -100, 100, -200, 100, 100, -100, -100}
	km, pm := 5000, 5000
	k = append(k, ok(0, km-50, km+50, 1, 1))
	p = append(p, ok(0, pm-50, pm+50, 1, 1))
	for i, st := range steps {
		ts := int64(i+1) * 10_000
		km += st
		k = append(k, ok(ts+100, km-50, km+50, 1, 1))
		pm += st
		p = append(p, ok(ts+2100, pm-50, pm+50, 1, 1))
	}
	r := analyze(k, p, runRecs("r", 0, 130_000), 0, 130_000)
	c := r.LeadLag.Corr
	if c.PeakLagS == nil || *c.PeakLagS != 2 || *c.PeakCorr < 0.999 {
		t.Fatalf("correlation peak = %v %v (lags %+v)", deref(c.PeakLagS), derefF(c.PeakCorr), c.Lags)
	}
	if c.KalshiMoves != 10 || c.PolymarketMoves != 10 {
		t.Fatalf("moves = %d, %d", c.KalshiMoves, c.PolymarketMoves)
	}
}

func TestTransport(t *testing.T) {
	withVT := func(r storage.TickRecord, vt int64) storage.TickRecord { r.VT = vt; return r }
	k := []storage.TickRecord{withVT(ok(1000, 2000, 2100, 1, 1), 990), withVT(ok(2000, 2000, 2100, 1, 1), 1980), ok(3000, 2000, 2100, 1, 1)}
	p := []storage.TickRecord{withVT(ok(1000, 2000, 2100, 1, 1), 950), withVT(ok(2000, 2000, 2100, 1, 1), 1960), withVT(ok(3000, 2000, 2100, 1, 1), 2900)}
	r := analyze(k, p, runRecs("r", 0, 5000), 0, 5000)
	tr := r.LeadLag.Transport
	if tr[0].Ticks != 2 || *tr[0].MedianMs != 10 || tr[1].Ticks != 3 || *tr[1].MedianMs != 50 || *r.LeadLag.TransportDiffMs != 40 {
		t.Fatalf("transport = %+v %+v diff %v", tr[0], tr[1], deref64(r.LeadLag.TransportDiffMs))
	}
}

// TestTextAndJSON: the text report carries the required caveats next to
// the lead/lag numbers, and the JSON encodes.
func TestTextAndJSON(t *testing.T) {
	k := []storage.TickRecord{ok(0, 2100, 2200, 10, 50)}
	p := []storage.TickRecord{ok(0, 1800, 1900, 4, 7)}
	fees, err := LoadFees("../../fees.yaml")
	if err != nil {
		t.Fatal(err)
	}
	r := analyze(k, p, runRecs("r", 0, 10000), 0, 10000, func(in *Input) { in.Fees = fees })
	var b bytes.Buffer
	if err := WriteText(&b, r, 20); err != nil {
		t.Fatal(err)
	}
	out := b.String()
	ll := out[strings.Index(out, "Lead/lag"):]
	for _, want := range []string{TCaveat, "median t - vt", "[transport diff", "Kalshi fee/ct", "1.28c", "https://", "Not modelled"} {
		if !strings.Contains(out, want) {
			t.Errorf("text report lacks %q:\n%s", want, out)
		}
	}
	if !strings.Contains(ll[:400], TCaveat) || !strings.Contains(ll[:600], "median t - vt") {
		t.Errorf("caveats aren't at the top of the lead/lag section:\n%s", ll)
	}
	if _, err := json.Marshal(r); err != nil {
		t.Fatal(err)
	}
}

func deref(p *int) any {
	if p == nil {
		return nil
	}
	return *p
}

func derefF(p *float64) any {
	if p == nil {
		return nil
	}
	return *p
}

func deref64(p *int64) any {
	if p == nil {
		return nil
	}
	return *p
}
