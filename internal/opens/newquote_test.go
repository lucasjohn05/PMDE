package opens

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/lucasjohn05/pmde/internal/kalshi"
	"github.com/lucasjohn05/pmde/internal/metrics"
	"github.com/lucasjohn05/pmde/internal/storage"
)

// quotedGauges returns opens_new_series_quoted's series as label maps.
func quotedGauges(t *testing.T, reg *prometheus.Registry) []map[string]string {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	var out []map[string]string
	for _, mf := range mfs {
		if mf.GetName() != "opens_new_series_quoted" {
			continue
		}
		for _, m := range mf.GetMetric() {
			l := map[string]string{}
			for _, p := range m.GetLabel() {
				l[p.GetName()] = p.GetValue()
			}
			out = append(out, l)
		}
	}
	return out
}

func TestNewSeriesQuotes(t *testing.T) {
	dir := t.TempDir()
	reg := prometheus.NewRegistry()
	detected := time.Date(2026, 11, 10, 0, 0, 0, 0, time.UTC)
	n := &NewSeriesQuotes{Dir: dir, For: 14 * 24 * time.Hour, Metrics: metrics.NewOpens(reg)}
	q := NewSeriesQuote{T: detected.Add(time.Hour).UnixMilli(), Series: "KXNEW", Title: "New thing", DetectedMs: detected.UnixMilli(),
		Market: "KXNEW-1", Bid: 3600, Ask: 4400, BidSz: 25000, AskSz: 25000}
	now := detected.Add(2 * time.Hour)
	if !n.Quoted(q, now) {
		t.Fatal("first quote not alerted")
	}
	if n.Quoted(q, now) || n.Want("KXNEW") {
		t.Error("a second quote alerted")
	}
	// Past its first 14 days, a series is no longer new.
	old := q
	old.Series = "KXOLD"
	if n.Quoted(old, detected.Add(15*24*time.Hour)) {
		t.Error("an old series alerted")
	}
	g := quotedGauges(t, reg)
	if len(g) != 1 || g[0]["series"] != "KXNEW" || g[0]["title"] != "New thing" || g[0]["market"] != "KXNEW-1" ||
		g[0]["bid"] != "36c" || g[0]["ask"] != "44c" || g[0]["bid_size"] != "250" || g[0]["spread_bps"] != "800" {
		t.Errorf("gauges %v", g)
	}
	recs, err := storage.ReadJSONL[NewSeriesQuote](filepath.Join(dir, newQuotesFile))
	if err != nil || len(recs) != 1 || recs[0] != q {
		t.Errorf("file %+v, %v", recs, err)
	}

	// A restart reads it back: still alerted, still exposed, and gone
	// once the series is past its 14 days.
	reg2 := prometheus.NewRegistry()
	n2 := &NewSeriesQuotes{Dir: dir, For: 14 * 24 * time.Hour, Metrics: metrics.NewOpens(reg2)}
	if err := n2.Load(now); err != nil {
		t.Fatal(err)
	}
	if n2.Want("KXNEW") || len(quotedGauges(t, reg2)) != 1 {
		t.Errorf("after Load: want %v, gauges %v", n2.Want("KXNEW"), quotedGauges(t, reg2))
	}
	n2.Expire(detected.Add(15 * 24 * time.Hour))
	if g := quotedGauges(t, reg2); len(g) != 0 {
		t.Errorf("after Expire: %v", g)
	}
}

// A capture of a new series raises the alert at the first quote that
// meets the rule, not at the stub before it, and only once.
func TestCaptureRaisesNewSeriesAlert(t *testing.T) {
	var open time.Time
	v := &wsVenue{script: func(conn *websocket.Conn, tickers []string) {
		for i, tk := range tickers {
			wsSend(conn, wsSnap(i+1, tk))
		}
		time.Sleep(time.Until(open.Add(20 * time.Millisecond)))
		seq := len(tickers)
		send := func(tk, side, price, size string) {
			seq++
			wsSend(conn, wsDelta(seq, tk, side, price, size))
		}
		// A stub on T1, then a real quote on T2, then one on T1.
		send("NEW-E1-T1", "yes", "0.0100", "500.00")
		send("NEW-E1-T1", "no", "0.0100", "500.00")
		send("NEW-E1-T2", "yes", "0.3600", "250.00")
		send("NEW-E1-T2", "no", "0.5600", "250.00")
		send("NEW-E1-T1", "yes", "0.2000", "50.00")
		send("NEW-E1-T1", "no", "0.7400", "50.00")
	}}
	detected := time.Now().Add(-24 * time.Hour)
	var quotes *NewSeriesQuotes
	rig := startCapturer(t, v, captureConfig(300*time.Millisecond, 300*time.Millisecond, 1), func(rig *captureRig) {
		quotes = &NewSeriesQuotes{Dir: rig.dir, For: 14 * 24 * time.Hour, Metrics: rig.m, Logf: rig.c.Logf}
		rig.c.NewQuotes = quotes
		rig.c.NewSeries = func(s string) (NewSeriesRecord, bool) {
			return NewSeriesRecord{T: detected.UnixMilli(), Ticker: s, Title: "A new series"}, s == "NEW"
		}
	})
	open = time.Now().Add(600 * time.Millisecond)
	rig.c.Schedule.Update("NEW", []kalshi.Market{
		{Ticker: "NEW-E1-T1", Event: "NEW-E1", Open: open},
		{Ticker: "NEW-E1-T2", Event: "NEW-E1", Open: open},
	})
	waitFor(t, 10*time.Second, "the capture to finish", func() bool { return len(rig.captures(t)) == 2 })
	rig.stop()

	recs, err := storage.ReadJSONL[NewSeriesQuote](filepath.Join(rig.dir, newQuotesFile))
	if err != nil || len(recs) != 1 {
		t.Fatalf("quotes %+v, %v", recs, err)
	}
	if q := recs[0]; q.Series != "NEW" || q.Market != "NEW-E1-T2" || q.Bid != 3600 || q.Ask != 4400 || q.BidSz != 25000 || q.Title != "A new series" {
		t.Errorf("quote %+v", q)
	}
	if g := quotedGauges(t, rig.reg); len(g) != 1 || g[0]["market"] != "NEW-E1-T2" {
		t.Errorf("gauges %v", g)
	}
	var logged bool
	rig.mu.Lock()
	for _, l := range rig.logs {
		logged = logged || strings.Contains(l, "NEW SERIES QUOTED NEW (A new series")
	}
	rig.mu.Unlock()
	if !logged {
		t.Errorf("no log line: %q", rig.logs)
	}
}
