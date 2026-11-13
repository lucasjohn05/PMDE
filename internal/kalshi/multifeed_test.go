package kalshi

import (
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// Messages of a multi-market subscription, in the shape Kalshi sends
// them (November 2026): one sid, one seq across the markets.
func mSnap(seq int64, ticker string, yes, no string) []byte {
	body := ""
	if yes != "" {
		body += `,"yes_dollars_fp":[` + yes + `]`
	}
	if no != "" {
		body += `,"no_dollars_fp":[` + no + `]`
	}
	return []byte(fmt.Sprintf(`{"type":"orderbook_snapshot","sid":1,"seq":%d,"msg":{"market_ticker":%q%s}}`, seq, ticker, body))
}

func mDelta(seq int64, ticker, side, price, delta string) []byte {
	return []byte(fmt.Sprintf(`{"type":"orderbook_delta","sid":1,"seq":%d,"msg":{"market_ticker":%q,"price_dollars":%q,"delta_fp":%q,"side":%q,"ts_ms":%d}}`,
		seq, ticker, price, delta, side, 1794532313000+seq))
}

func feedAll(f *MultiFeed, lines ...[]byte) (events []BookEvent, errs []error) {
	for _, l := range lines {
		ev, ok, err := f.HandleQueued(Queued{Raw: l, Recv: time.UnixMilli(1794532313700)})
		if ok {
			events = append(events, ev)
		}
		if err != nil {
			errs = append(errs, err)
		}
	}
	return events, errs
}

// One seq runs across the markets: snapshots 1..N, then interleaved
// deltas, each applied to its own market's book.
func TestMultiFeedSharedSeq(t *testing.T) {
	f := NewMultiFeed([]string{"E-A", "E-B", "E-C"})
	events, errs := feedAll(f,
		[]byte(`{"type":"subscribed","id":1,"msg":{"channel":"orderbook_delta","sid":1}}`),
		mSnap(1, "E-A", `["0.4000","10.00"]`, `["0.5000","20.00"]`),
		mSnap(2, "E-B", "", ""),
	)
	if len(errs) != 0 || len(events) != 2 {
		t.Fatalf("events %d errs %v", len(events), errs)
	}
	if f.Synced() || !f.AnySynced() {
		t.Errorf("synced %v anySynced %v with one snapshot outstanding", f.Synced(), f.AnySynced())
	}
	// A delta for the market still waiting for its snapshot is reported
	// as unsynced and not applied.
	early, _ := feedAll(f, mDelta(3, "E-C", "yes", "0.1000", "5.00"))
	if len(early) != 1 || !early[0].Stale || early[0].StaleReason != "" || early[0].Market != "E-C" {
		t.Errorf("delta before its market's snapshot: %+v", early)
	}
	events, errs = feedAll(f,
		mSnap(4, "E-C", "", `["0.8000","7.00"]`),
		mDelta(5, "E-B", "yes", "0.2500", "30.00"),
		mDelta(6, "E-A", "yes", "0.4200", "5.00"),
		mDelta(7, "E-B", "no", "0.7000", "30.00"),
	)
	if len(errs) != 0 {
		t.Fatalf("errs %v", errs)
	}
	if !f.Synced() {
		t.Errorf("not synced after every market's snapshot")
	}
	if ev := events[1]; ev.Market != "E-B" || ev.Seq != 5 || ev.Stale || !ev.HasBid || ev.BidBps != 2500 || ev.BidSize != 3000 || ev.HasAsk {
		t.Errorf("E-B after its first delta: %+v", ev)
	}
	if ev := events[2]; ev.Market != "E-A" || ev.BidBps != 4200 || ev.BidSize != 500 || ev.AskBps != 5000 || ev.AskSize != 2000 || ev.VenueMs != 1794532313006 {
		t.Errorf("E-A after seq 6: %+v", ev)
	}
	if ev := events[3]; ev.BidBps != 2500 || ev.AskBps != 3000 || ev.AskSize != 3000 {
		t.Errorf("E-B two-sided: %+v", ev)
	}
	// E-C's early delta was not applied.
	if _, ok := f.Book("E-C").YesSize(1000); ok {
		t.Errorf("a delta before the snapshot was applied to E-C")
	}
	if bid, ask, ok := f.Book("E-A").BestYes(); !ok || bid != 4200 || ask != 5000 {
		t.Errorf("E-A book: %d/%d %v", bid, ask, ok)
	}
}

// A gap in the shared seq breaks every book on the subscription, and
// nothing is applied after it.
func TestMultiFeedGapBreaksEveryBook(t *testing.T) {
	f := NewMultiFeed([]string{"E-A", "E-B"})
	feedAll(f, mSnap(1, "E-A", `["0.4000","10.00"]`, `["0.5000","20.00"]`), mSnap(2, "E-B", `["0.1000","1.00"]`, `["0.8000","1.00"]`))
	events, errs := feedAll(f, mDelta(4, "E-B", "yes", "0.1500", "9.00")) // seq 3 is missing
	if len(errs) != 1 || !errors.Is(errs[0], ErrSeqGap) {
		t.Fatalf("errs %v, want a seq gap", errs)
	}
	if broken, why := f.Broken(); !broken || why != ReasonSeqGap {
		t.Errorf("broken %v reason %q", broken, why)
	}
	if ev := events[0]; !ev.Stale || ev.StaleReason != ReasonSeqGap || ev.HasBid {
		t.Errorf("the gap's own event: %+v", ev)
	}
	// The other market's next delta is consecutive, and still not
	// trusted or applied: the missing message may have been its.
	events, errs = feedAll(f, mDelta(5, "E-A", "yes", "0.4500", "3.00"))
	if len(errs) != 0 || !events[0].Stale || events[0].StaleReason != ReasonSeqGap {
		t.Errorf("E-A after the gap: %+v (errs %v)", events[0], errs)
	}
	if _, ok := f.Book("E-A").YesSize(4500); ok {
		t.Errorf("a delta was applied after the gap")
	}
	if f.Synced() {
		t.Errorf("Synced after a gap")
	}
}

func TestMultiFeedDropsAndBadMessages(t *testing.T) {
	// A drop reported in-band breaks everything before the message is
	// looked at.
	f := NewMultiFeed([]string{"E-A", "E-B"})
	feedAll(f, mSnap(1, "E-A", "", ""), mSnap(2, "E-B", "", ""))
	ev, ok, err := f.HandleQueued(Queued{Raw: mDelta(3, "E-A", "yes", "0.4000", "1.00"), Drops: 2})
	if !ok || err == nil || !ev.Stale || ev.StaleReason != ReasonDropped {
		t.Errorf("after a drop: %+v, err %v", ev, err)
	}

	// A message for a market that isn't subscribed.
	f = NewMultiFeed([]string{"E-A"})
	_, errs := feedAll(f, mSnap(1, "E-A", "", ""), mDelta(2, "E-Z", "yes", "0.4000", "1.00"))
	if broken, why := f.Broken(); len(errs) != 1 || !broken || why != ReasonInvalid {
		t.Errorf("unknown market: errs %v broken %v %q", errs, broken, why)
	}

	// A delta that would drive a level negative.
	f = NewMultiFeed([]string{"E-A"})
	_, errs = feedAll(f, mSnap(1, "E-A", `["0.4000","1.00"]`, ""), mDelta(2, "E-A", "yes", "0.4000", "-5.00"))
	if broken, why := f.Broken(); len(errs) != 1 || !broken || why != ReasonInvalid {
		t.Errorf("negative level: errs %v broken %v %q", errs, broken, why)
	}

	// A venue error is reported but breaks nothing by itself.
	f = NewMultiFeed([]string{"E-A"})
	_, errs = feedAll(f, []byte(`{"id":1,"type":"error","msg":{"code":26,"msg":"Subscription market limit exceeded"}}`))
	if broken, _ := f.Broken(); len(errs) != 1 || broken || !strings.Contains(errs[0].Error(), "limit exceeded") {
		t.Errorf("venue error: errs %v broken %v", errs, broken)
	}
}

// A drop with nothing queued behind it is noticed while idle, as with
// Feed.Next.
func TestMultiFeedNextIdleDrop(t *testing.T) {
	f := NewMultiFeed([]string{"E-A"})
	q := NewQueue(1)
	q.Offer(mSnap(1, "E-A", "", ""))
	if q.Offer(mDelta(2, "E-A", "yes", "0.4000", "1.00")) {
		t.Fatal("the second message was not dropped")
	}
	ev, got, more, err := f.Next(q)
	if !got || !more || err != nil || ev.Stale {
		t.Fatalf("first step: %+v got %v more %v err %v", ev, got, more, err)
	}
	_, got, more, err = f.Next(q)
	if got || !more || err == nil {
		t.Fatalf("idle step: got %v more %v err %v, want a reported drop", got, more, err)
	}
	if broken, why := f.Broken(); !broken || why != ReasonDropped {
		t.Errorf("broken %v %q", broken, why)
	}
	q.Close()
	if _, _, more, _ := f.Next(q); more {
		t.Errorf("more after the queue closed")
	}
}

// The EventClient subscribes all its markets on one connection, and on
// a gap marks every market stale and resyncs all of them.
func TestEventClientResyncsEveryMarket(t *testing.T) {
	v := &fakeVenue{scripts: []func(*websocket.Conn){
		func(conn *websocket.Conn) {
			send(conn, [][]byte{
				mSnap(1, "E-A", `["0.4000","10.00"]`, ""),
				mSnap(2, "E-B", "", `["0.5000","20.00"]`),
				mDelta(3, "E-A", "no", "0.5500", "4.00"),
				mDelta(5, "E-B", "yes", "0.4000", "1.00"), // seq 4 is missing
				mDelta(6, "E-A", "yes", "0.4100", "1.00"),
			})
		},
		func(conn *websocket.Conn) {
			send(conn, [][]byte{mSnap(1, "E-A", `["0.4100","11.00"]`, ""), mSnap(2, "E-B", "", ""), mDelta(3, "E-B", "yes", "0.2000", "2.00")})
		},
	}}
	var mu sync.Mutex
	var events []BookEvent
	c := &EventClient{Tickers: []string{"E-A", "E-B"}, OnEvent: func(ev BookEvent) {
		mu.Lock()
		events = append(events, ev)
		mu.Unlock()
	}}
	srvStop := runEventClient(t, v, c)
	waitFor(t, 10*time.Second, "both sessions' events", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(events) >= 8
	})
	srvStop()

	var got []string
	for _, ev := range events {
		s := fmt.Sprintf("%s/%s/%d", ev.Type, ev.Market, ev.Seq)
		if ev.Stale {
			s += "/" + ev.StaleReason
		}
		got = append(got, s)
	}
	want := []string{
		"snapshot/E-A/1", "snapshot/E-B/2", "delta/E-A/3",
		"delta/E-B/5/seq_gap", // the gap shows on E-B's message...
		"stale/E-A/0/seq_gap", // ...and E-A is told too
		"snapshot/E-A/1", "snapshot/E-B/2", "delta/E-B/3",
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("events\n got %v\nwant %v", got, want)
	}
	if ev := events[2]; !ev.HasBid || ev.BidBps != 4000 || ev.AskBps != 4500 || ev.AskSize != 400 {
		t.Errorf("E-A after seq 3: %+v", ev)
	}
	if ev := events[7]; ev.BidBps != 2000 || ev.Stale {
		t.Errorf("E-B after the resync: %+v", ev)
	}
	subs := v.subscriptions()
	if len(subs) != 2 || !strings.Contains(subs[0], `"market_tickers":["E-A","E-B"]`) || !strings.Contains(subs[0], `"channels":["orderbook_delta"]`) {
		t.Errorf("subscribe messages: %q", subs)
	}
}

// Books lost with the connection are reported for every market.
func TestEventClientConnectionLoss(t *testing.T) {
	v := &fakeVenue{scripts: []func(*websocket.Conn){
		func(conn *websocket.Conn) {
			send(conn, [][]byte{mSnap(1, "E-A", "", ""), mSnap(2, "E-B", "", "")})
			time.Sleep(50 * time.Millisecond)
			conn.Close()
		},
	}}
	var mu sync.Mutex
	var events []BookEvent
	c := &EventClient{Tickers: []string{"E-A", "E-B"}, OnEvent: func(ev BookEvent) {
		mu.Lock()
		events = append(events, ev)
		mu.Unlock()
	}}
	stop := runEventClient(t, v, c)
	waitFor(t, 10*time.Second, "the loss to be reported", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(events) >= 4
	})
	stop()
	for i, m := range []string{"E-A", "E-B"} {
		if ev := events[2+i]; ev.Type != EventStale || ev.Market != m || ev.StaleReason != ReasonReconnect {
			t.Errorf("event %d: %+v, want a reconnect stale for %s", 2+i, ev, m)
		}
	}
}

// A subscription that never delivers every snapshot is abandoned and
// retried.
func TestEventClientSnapshotTimeout(t *testing.T) {
	v := &fakeVenue{scripts: []func(*websocket.Conn){
		func(conn *websocket.Conn) { send(conn, [][]byte{mSnap(1, "E-A", "", "")}) }, // E-B's never comes
		func(conn *websocket.Conn) { send(conn, [][]byte{mSnap(1, "E-A", "", ""), mSnap(2, "E-B", "", "")}) },
	}}
	var mu sync.Mutex
	var logs []string
	c := &EventClient{Tickers: []string{"E-A", "E-B"}, snapshotTimeout: 200 * time.Millisecond,
		Logf: func(f string, a ...any) {
			mu.Lock()
			logs = append(logs, fmt.Sprintf(f, a...))
			mu.Unlock()
		}}
	stop := runEventClient(t, v, c)
	waitFor(t, 10*time.Second, "the second connection to sync", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return strings.Contains(strings.Join(logs, "\n"), "2 book(s) synced")
	})
	stop()
	if !strings.Contains(strings.Join(logs, "\n"), "resync ("+ReasonSnapshotTimeout+")") {
		t.Errorf("no snapshot-timeout resync in the log: %q", logs)
	}
}

// runEventClient starts c.Run against v and returns a stop func.
func runEventClient(t *testing.T, v *fakeVenue, c *EventClient) (stop func()) {
	t.Helper()
	srv := httptest.NewServer(v)
	c.URL = "ws" + strings.TrimPrefix(srv.URL, "http")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		c.Run(ctx)
	}()
	return func() {
		t.Helper()
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatalf("Run did not return after cancel")
		}
		srv.Close()
	}
}
