package polymarket

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/lucasjohn05/pmde/internal/metrics"
)

// startClient runs c against v and returns a channel closed when Run
// returns, for tests where Run is expected to stop by itself.
func startClient(t *testing.T, v *fakeVenue, c *Client) <-chan struct{} {
	t.Helper()
	srv := httptest.NewServer(v)
	c.URL = "ws" + strings.TrimPrefix(srv.URL, "http")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		c.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Errorf("Run did not return after cancel")
		}
		srv.Close()
	})
	return done
}

func waitDone(t *testing.T, done <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatalf("Run did not return by itself: %s", what)
	}
}

// closeWith sends a close frame, as Polymarket did on 2026-10-28 20:02:09
// UTC: "websocket: close 1000 (normal): all subscribed assets resolved".
func closeWith(conn *websocket.Conn, code int, text string) {
	conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(code, text), time.Now().Add(time.Second))
}

// TestClientStopsOnResolvedClose replays a synced session that the venue
// closes with "all subscribed assets resolved". Run must return without
// dialing again: no reconnect, no resync, not stale, market_resolved 1,
// and a final untrusted tick for the book that went away.
func TestClientStopsOnResolvedClose(t *testing.T) {
	lines := captureLines(t)
	v := &fakeVenue{scripts: []func(*websocket.Conn){
		func(conn *websocket.Conn) {
			send(conn, lines)
			closeWith(conn, websocket.CloseNormalClosure, "all subscribed assets resolved")
		},
	}}
	rec := &recorder{}
	reg := prometheus.NewRegistry()
	var checks atomic.Int32
	c := &Client{AssetID: captureAssetID, OnTick: rec.onTick, Logf: rec.logf,
		Metrics: metrics.New(reg).Market("polymarket", captureAssetID),
		Resolved: func(context.Context, string) (bool, error) {
			checks.Add(1)
			return true, nil
		}}
	waitDone(t, startClient(t, v, c), "after the resolved close")

	// Longer than the first backoff (at most backoffBase), so a reconnect
	// would have dialed by now.
	time.Sleep(2 * backoffBase)
	if n := v.conns.Load(); n != 1 {
		t.Errorf("connections = %d, want 1 (no reconnect)", n)
	}
	checkMetrics(t, reg, "polymarket", map[string]float64{
		"market_resolved":                  1,
		"book_stale":                       0,
		"reconnects_total":                 0,
		"resyncs_total/" + ReasonReconnect: 0,
		"resyncs_total/" + ReasonDropped:   0,
		"resyncs_total/" + ReasonInvalid:   0,
	})
	if n := checks.Load(); n != 0 {
		t.Errorf("Resolved called %d times; the close frame alone should be enough", n)
	}
	logs, bests, _ := rec.snapshot()
	t.Logf("log:\n  %s", strings.Join(logs, "\n  "))
	if len(bests) < 2 || !bests[0].ok || bests[len(bests)-1].ok {
		t.Errorf("ticks = %v, want trusted ones, then a final untrusted one", bests)
	}
	if r := resyncReasons(logs); len(r) != 0 {
		t.Errorf("resyncs logged: %v", r)
	}
	if last := logs[len(logs)-1]; !strings.Contains(last, "market resolved") || !strings.Contains(last, "not reconnecting") {
		t.Errorf("last log line = %q, want the resolved stop", last)
	}
}

// TestClientReconnectsOnOtherNormalClose: a 1000 close that doesn't say
// resolved (Polymarket also closes 1001 "server shutting down" and the
// like) is an ordinary reconnect.
func TestClientReconnectsOnOtherNormalClose(t *testing.T) {
	lines := captureLines(t)
	v := &fakeVenue{scripts: []func(*websocket.Conn){
		func(conn *websocket.Conn) {
			send(conn, lines)
			closeWith(conn, websocket.CloseNormalClosure, "bye")
		},
		func(conn *websocket.Conn) { send(conn, lines) },
	}}
	reg := prometheus.NewRegistry()
	c := &Client{AssetID: captureAssetID, Metrics: metrics.New(reg).Market("polymarket", captureAssetID)}
	stop := runClient(t, v, c)
	waitFor(t, 10*time.Second, "a second connection", func() bool { return v.conns.Load() == 2 })
	stop()
	checkMetrics(t, reg, "polymarket", map[string]float64{"market_resolved": 0, "reconnects_total": 1})
}

// TestClientStopsWhenSnapshotNeverArrivesForResolved: after its one-time
// close, Polymarket accepts subscriptions to a resolved asset and sends
// nothing. Run must ask Resolved after the snapshot timeout and stop.
func TestClientStopsWhenSnapshotNeverArrivesForResolved(t *testing.T) {
	v := &fakeVenue{} // every connection: subscribed, then silence
	reg := prometheus.NewRegistry()
	var asked atomic.Value
	c := &Client{AssetID: captureAssetID, snapshotTimeout: 50 * time.Millisecond,
		Metrics: metrics.New(reg).Market("polymarket", captureAssetID),
		Resolved: func(_ context.Context, id string) (bool, error) {
			asked.Store(id)
			return true, nil
		}}
	waitDone(t, startClient(t, v, c), "after a snapshot timeout on a resolved market")
	time.Sleep(2 * backoffBase)
	if n := v.conns.Load(); n != 1 {
		t.Errorf("connections = %d, want 1", n)
	}
	if id, _ := asked.Load().(string); id != captureAssetID {
		t.Errorf("Resolved asked about %q, want %q", id, captureAssetID)
	}
	checkMetrics(t, reg, "polymarket", map[string]float64{
		"market_resolved": 1, "book_stale": 0, "reconnects_total": 0,
	})
}

// TestClientRetriesWhenNotResolved: a snapshot timeout on a market that
// hasn't resolved, or a failed check, is retried as before; a snapshot
// timeout alone (a slow or broken venue) is never taken as resolution.
func TestClientRetriesWhenNotResolved(t *testing.T) {
	v := &fakeVenue{}
	reg := prometheus.NewRegistry()
	rec := &recorder{}
	var calls atomic.Int32
	c := &Client{AssetID: captureAssetID, snapshotTimeout: 50 * time.Millisecond, Logf: rec.logf,
		Metrics: metrics.New(reg).Market("polymarket", captureAssetID),
		Resolved: func(context.Context, string) (bool, error) {
			if calls.Add(1) == 1 {
				return false, errors.New("gamma down")
			}
			return false, nil
		}}
	stop := runClient(t, v, c)
	waitFor(t, 10*time.Second, "a third connection", func() bool { return v.conns.Load() >= 3 })
	stop()
	checkMetrics(t, reg, "polymarket", map[string]float64{"market_resolved": 0, "book_stale": 1})
	logs, _, _ := rec.snapshot()
	if !strings.Contains(strings.Join(logs, "\n"), "checking whether the market resolved: gamma down") {
		t.Errorf("failed check not logged:\n  %s", strings.Join(logs, "\n  "))
	}
}

func TestGammaResolved(t *testing.T) {
	const id = "111061902544814266207267295505639408607400625795891618462682726460921782993748"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if q.Get("closed") != "true" {
			t.Errorf("query %q doesn't ask for closed markets", r.URL.RawQuery)
		}
		switch q.Get("clob_token_ids") {
		case id: // trimmed from the live response, 2026-11-10
			w.Write([]byte(`[{"question":"Will there be no change in Fed interest rates after the October 2026 meeting?","closed":true,"acceptingOrders":false,"umaResolutionStatus":"resolved","clobTokenIds":"[\"` + id + `\", \"42038775866198212615491650114881818219665139981763207699110491615333273795415\"]"}]`))
		case "2": // closed, resolution proposed but not final
			w.Write([]byte(`[{"closed":true,"umaResolutionStatus":"proposed","clobTokenIds":"[\"1\", \"2\"]"}]`))
		case "3": // open: not in a closed=true listing
			w.Write([]byte(`[]`))
		case "4": // some other market came back
			w.Write([]byte(`[{"closed":true,"umaResolutionStatus":"resolved","clobTokenIds":"[\"5\", \"6\"]"}]`))
		default:
			http.Error(w, "boom", http.StatusInternalServerError)
		}
	}))
	defer srv.Close()
	ctx := context.Background()
	for _, tc := range []struct {
		id      string
		want    bool
		wantErr bool
	}{{id, true, false}, {"2", false, false}, {"3", false, false}, {"4", false, false}, {"9", false, true}} {
		got, err := Resolved(ctx, srv.Client(), srv.URL, tc.id)
		if got != tc.want || (err != nil) != tc.wantErr {
			t.Errorf("Resolved(%s) = %v, %v; want %v, error %v", tc.id, got, err, tc.want, tc.wantErr)
		}
	}
}
