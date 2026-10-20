package metrics

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// TestMarketSeriesExistUpFront checks that a market's series exist before
// anything happens: counters at 0, book_stale at 1.
func TestMarketSeriesExistUpFront(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := New(reg)
	m.Market("kalshi", "K1")

	want := `
# HELP book_stale 1 while the venue's book is untrusted (not yet synced, stale, or reconnecting), else 0.
# TYPE book_stale gauge
book_stale{market="K1",venue="kalshi"} 1
# HELP resyncs_total Times a trusted book was lost and had to be rebuilt from a fresh snapshot, by what broke it.
# TYPE resyncs_total counter
resyncs_total{market="K1",reason="dropped",venue="kalshi"} 0
resyncs_total{market="K1",reason="invalid_update",venue="kalshi"} 0
resyncs_total{market="K1",reason="reconnect",venue="kalshi"} 0
resyncs_total{market="K1",reason="seq_gap",venue="kalshi"} 0
`
	if err := testutil.GatherAndCompare(reg, strings.NewReader(want), "book_stale", "resyncs_total"); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"messages_total", "dropped_messages_total", "reconnects_total", "ingest_lag_seconds"} {
		if n, err := testutil.GatherAndCount(reg, name); err != nil || n != 1 {
			t.Errorf("%s: %d series (err %v), want 1", name, n, err)
		}
	}
}

func TestMarketRecords(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := New(reg)
	k, p := m.Market("kalshi", "K1"), m.Market("polymarket", "P1")

	k.Message()
	k.Message()
	k.Dropped()
	k.Reconnect()
	k.Resync(ResyncSeqGap)
	k.Resync("something_new") // unknown reasons fold into invalid_update
	k.SetStale(false)
	k.ObserveLag(40 * time.Millisecond)
	k.ObserveLag(3 * time.Second)
	p.Message()

	checks := []struct {
		c    prometheus.Collector
		want float64
	}{
		{m.messages.WithLabelValues("kalshi", "K1"), 2},
		{m.messages.WithLabelValues("polymarket", "P1"), 1},
		{m.dropped.WithLabelValues("kalshi", "K1"), 1},
		{m.reconnect.WithLabelValues("kalshi", "K1"), 1},
		{m.resyncs.WithLabelValues("kalshi", "K1", ResyncSeqGap), 1},
		{m.resyncs.WithLabelValues("kalshi", "K1", ResyncInvalid), 1},
		{m.resyncs.WithLabelValues("kalshi", "K1", ResyncDropped), 0},
		{m.stale.WithLabelValues("kalshi", "K1"), 0},
		{m.stale.WithLabelValues("polymarket", "P1"), 1},
	}
	for i, c := range checks {
		if got := testutil.ToFloat64(c.c); got != c.want {
			t.Errorf("check %d: got %v, want %v", i, got, c.want)
		}
	}
	if n, err := testutil.GatherAndCount(reg, "resyncs_total"); err != nil || n != 8 {
		t.Errorf("resyncs_total: %d series (err %v), want 8 (an unknown reason must not add one)", n, err)
	}

	want := `
# HELP ingest_lag_seconds Receive time minus the venue's message timestamp. Includes clock skew between the venue and this host.
# TYPE ingest_lag_seconds histogram
ingest_lag_seconds_bucket{market="K1",venue="kalshi",le="0.001"} 0
ingest_lag_seconds_bucket{market="K1",venue="kalshi",le="0.002"} 0
ingest_lag_seconds_bucket{market="K1",venue="kalshi",le="0.004"} 0
ingest_lag_seconds_bucket{market="K1",venue="kalshi",le="0.008"} 0
ingest_lag_seconds_bucket{market="K1",venue="kalshi",le="0.016"} 0
ingest_lag_seconds_bucket{market="K1",venue="kalshi",le="0.032"} 0
ingest_lag_seconds_bucket{market="K1",venue="kalshi",le="0.064"} 1
ingest_lag_seconds_bucket{market="K1",venue="kalshi",le="0.128"} 1
ingest_lag_seconds_bucket{market="K1",venue="kalshi",le="0.256"} 1
ingest_lag_seconds_bucket{market="K1",venue="kalshi",le="0.512"} 1
ingest_lag_seconds_bucket{market="K1",venue="kalshi",le="1.024"} 1
ingest_lag_seconds_bucket{market="K1",venue="kalshi",le="2.048"} 1
ingest_lag_seconds_bucket{market="K1",venue="kalshi",le="4.096"} 2
ingest_lag_seconds_bucket{market="K1",venue="kalshi",le="8.192"} 2
ingest_lag_seconds_bucket{market="K1",venue="kalshi",le="16.384"} 2
ingest_lag_seconds_bucket{market="K1",venue="kalshi",le="+Inf"} 2
ingest_lag_seconds_sum{market="K1",venue="kalshi"} 3.04
ingest_lag_seconds_count{market="K1",venue="kalshi"} 2
ingest_lag_seconds_bucket{market="P1",venue="polymarket",le="0.001"} 0
ingest_lag_seconds_bucket{market="P1",venue="polymarket",le="0.002"} 0
ingest_lag_seconds_bucket{market="P1",venue="polymarket",le="0.004"} 0
ingest_lag_seconds_bucket{market="P1",venue="polymarket",le="0.008"} 0
ingest_lag_seconds_bucket{market="P1",venue="polymarket",le="0.016"} 0
ingest_lag_seconds_bucket{market="P1",venue="polymarket",le="0.032"} 0
ingest_lag_seconds_bucket{market="P1",venue="polymarket",le="0.064"} 0
ingest_lag_seconds_bucket{market="P1",venue="polymarket",le="0.128"} 0
ingest_lag_seconds_bucket{market="P1",venue="polymarket",le="0.256"} 0
ingest_lag_seconds_bucket{market="P1",venue="polymarket",le="0.512"} 0
ingest_lag_seconds_bucket{market="P1",venue="polymarket",le="1.024"} 0
ingest_lag_seconds_bucket{market="P1",venue="polymarket",le="2.048"} 0
ingest_lag_seconds_bucket{market="P1",venue="polymarket",le="4.096"} 0
ingest_lag_seconds_bucket{market="P1",venue="polymarket",le="8.192"} 0
ingest_lag_seconds_bucket{market="P1",venue="polymarket",le="16.384"} 0
ingest_lag_seconds_bucket{market="P1",venue="polymarket",le="+Inf"} 0
ingest_lag_seconds_sum{market="P1",venue="polymarket"} 0
ingest_lag_seconds_count{market="P1",venue="polymarket"} 0
`
	if err := testutil.GatherAndCompare(reg, strings.NewReader(want), "ingest_lag_seconds"); err != nil {
		t.Fatal(err)
	}
}

// TestNilVenue checks that a client with no metrics configured can call
// every method.
func TestNilRecorders(t *testing.T) {
	var v *Market
	v.Message()
	v.Dropped()
	v.Reconnect()
	v.Resync(ResyncSeqGap)
	v.SetStale(true)
	v.ObserveLag(time.Second)

	var p *Pair
	p.Set(1, 2)
	p.Clear()
}

// TestPair checks the divergence gauges: absent until set, and removed
// again by Clear rather than left holding a stale value.
func TestPair(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := New(reg)
	hold, hike := m.Pair("hold"), m.Pair("hike")

	if n, _ := testutil.GatherAndCount(reg, "divergence_mid_bps", "divergence_gap_bps"); n != 0 {
		t.Fatalf("%d divergence series before any Set, want 0", n)
	}

	hold.Set(-50, -150)
	hike.Set(100, 0)
	want := `
# HELP divergence_gap_bps Executable gap in bps: max(Kalshi bid - Polymarket ask, Polymarket bid - Kalshi ask). Positive means the books cross. Absent while either book has no trusted top of book.
# TYPE divergence_gap_bps gauge
divergence_gap_bps{pair="hike"} 0
divergence_gap_bps{pair="hold"} -150
# HELP divergence_mid_bps Kalshi mid minus Polymarket mid, in bps. Absent while either book has no trusted top of book.
# TYPE divergence_mid_bps gauge
divergence_mid_bps{pair="hike"} 100
divergence_mid_bps{pair="hold"} -50
`
	if err := testutil.GatherAndCompare(reg, strings.NewReader(want), "divergence_mid_bps", "divergence_gap_bps"); err != nil {
		t.Fatal(err)
	}

	hold.Clear()
	if n, _ := testutil.GatherAndCount(reg, "divergence_mid_bps", "divergence_gap_bps"); n != 2 {
		t.Fatalf("%d divergence series after clearing one pair, want 2 (the other pair's)", n)
	}
	if got := testutil.ToFloat64(m.divMid.WithLabelValues("hike")); got != 100 {
		t.Fatalf("hike mid = %v after clearing hold, want 100", got)
	}
}

func TestServe(t *testing.T) {
	reg := NewRegistry()
	New(reg).Market("polymarket", "P1").Message()

	ctx, cancel := context.WithCancel(context.Background())
	addr, err := Serve(ctx, "127.0.0.1:0", reg, t.Logf)
	if err != nil {
		t.Fatal(err)
	}
	url := "http://" + addr.String() + "/metrics"

	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /metrics: status %d", resp.StatusCode)
	}
	for _, want := range []string{
		`messages_total{market="P1",venue="polymarket"} 1`,
		`book_stale{market="P1",venue="polymarket"} 1`,
		`resyncs_total{market="P1",reason="seq_gap",venue="polymarket"} 0`,
		"go_goroutines",
	} {
		if !strings.Contains(string(body), want) {
			t.Errorf("/metrics is missing %q", want)
		}
	}

	// A busy address is reported at startup, not in the background.
	if _, err := Serve(ctx, addr.String(), reg, t.Logf); err == nil {
		t.Errorf("Serve on a busy address: want an error")
	}

	cancel()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := http.Get(url); err != nil {
			break // shut down
		}
		if time.Now().After(deadline) {
			t.Fatal("server still answering after ctx was cancelled")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
