package kalshi

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

const openTicker = "KXNHL2PTOTAL-26NOV12MTLBOS-4"

// openCaptureLines returns testdata/kalshi_open_capture.jsonl: a real
// session subscribed 29s before the market's open_time. The snapshot is
// empty (the msg has no yes/no arrays at all), and the first deltas
// arrive at the open.
func openCaptureLines(t *testing.T) [][]byte {
	t.Helper()
	data, err := os.ReadFile("../../testdata/kalshi_open_capture.jsonl")
	if err != nil {
		t.Fatalf("reading capture file: %v", err)
	}
	var lines [][]byte
	sc := bufio.NewScanner(bytes.NewReader(data))
	for sc.Scan() {
		if line := bytes.TrimSpace(sc.Bytes()); len(line) > 0 {
			lines = append(lines, append([]byte(nil), line...))
		}
	}
	return lines
}

// delta builds an orderbook_delta line for openTicker.
func openDelta(seq int64, side, price, delta string, tsMs int64) []byte {
	return []byte(fmt.Sprintf(`{"type":"orderbook_delta","sid":1,"seq":%d,"msg":{"market_ticker":%q,"price_dollars":%q,"delta_fp":%q,"side":%q,"ts_ms":%d}}`,
		seq, openTicker, price, delta, side, tsMs))
}

// The Feed reports every snapshot and delta of a pre-open session, with
// each side of the book on its own.
func TestFeedEventOverOpenCapture(t *testing.T) {
	lines := openCaptureLines(t)
	// The real capture only ever had no bids (yes asks). Add a yes bid
	// so the book goes two-sided.
	lines = append(lines, openDelta(9, "yes", "0.9200", "15.00", 1794528732000))

	var f Feed
	var events []BookEvent
	recv := time.UnixMilli(1794528683478)
	for _, line := range lines {
		f.HandleQueued(Queued{Raw: line, Recv: recv})
		if ev, ok := f.Event(); ok {
			events = append(events, ev)
		}
	}
	if len(events) != 9 {
		t.Fatalf("%d events, want 9 (the subscribed ack is not one)", len(events))
	}
	snap := events[0]
	if snap.Type != EventSnapshot || snap.Seq != 1 || snap.Stale || snap.HasBid || snap.HasAsk || len(snap.Yes)+len(snap.No) != 0 || snap.VenueMs != 0 {
		t.Errorf("pre-open snapshot: %+v", snap)
	}
	first := events[1]
	if first.Type != EventDelta || first.Seq != 2 || first.Side != "no" || first.PriceBps != 100 || first.DeltaCenti != 1500 ||
		first.VenueMs != 1794528712733 || !first.Recv.Equal(recv) {
		t.Errorf("first delta: %+v", first)
	}
	// 15 no at 1c is a yes ask of 99c for 15; no bid yet.
	if first.HasBid || !first.HasAsk || first.AskBps != 9900 || first.AskSize != 1500 {
		t.Errorf("book after the first delta: %+v", first)
	}
	// After seq 8: no bids at 1c (44), 5c (2), 6c (100+50-50+50 = 150).
	if ev := events[7]; ev.AskBps != 9400 || ev.AskSize != 15000 || ev.HasBid {
		t.Errorf("book after seq 8: %+v", ev)
	}
	last := events[8]
	if !last.HasBid || last.BidBps != 9200 || last.BidSize != 1500 || last.AskBps != 9400 {
		t.Errorf("two-sided book: %+v", last)
	}

	// A seq gap: the delta is reported, not applied, with the reason.
	f.HandleQueued(Queued{Raw: openDelta(11, "yes", "0.9300", "5.00", 1794528733000), Recv: recv})
	gap, ok := f.Event()
	if !ok || gap.Type != EventDelta || !gap.Stale || gap.StaleReason != ReasonSeqGap || gap.HasBid || gap.HasAsk || gap.PriceBps != 9300 {
		t.Errorf("delta after a seq gap: %+v (ok %v)", gap, ok)
	}

	// Next clears it: a step that handles no message reports no event.
	q := NewQueue(1)
	q.Close()
	f.Next(q)
	if _, ok := f.Event(); ok {
		t.Errorf("an event was reported for a step that handled no message")
	}
}

func TestSnapshotEventLevels(t *testing.T) {
	var f Feed
	f.HandleQueued(Queued{Raw: []byte(`{"type":"orderbook_snapshot","sid":1,"seq":7,"msg":{"market_ticker":"M",` +
		`"yes_dollars_fp":[["0.4300","47.00"],["0.4100","10.50"]],"no_dollars_fp":[["0.5500","3.00"]]}}`)})
	ev, ok := f.Event()
	if !ok || len(ev.Yes) != 2 || len(ev.No) != 1 {
		t.Fatalf("event %+v ok %v", ev, ok)
	}
	if ev.Yes[0] != (Level{4100, 1050}) || ev.Yes[1] != (Level{4300, 4700}) || ev.No[0] != (Level{5500, 300}) {
		t.Errorf("levels yes %v no %v", ev.Yes, ev.No)
	}
	if !ev.HasBid || ev.BidBps != 4300 || ev.BidSize != 4700 || !ev.HasAsk || ev.AskBps != 4500 || ev.AskSize != 300 {
		t.Errorf("top: %+v", ev)
	}
}

// The Client passes every book message to OnEvent, across a reconnect,
// and reports the book lost with the connection.
func TestClientOnEvent(t *testing.T) {
	lines := openCaptureLines(t)
	v := &fakeVenue{scripts: []func(*websocket.Conn){
		func(conn *websocket.Conn) {
			send(conn, lines[:5]) // ack, snapshot, three deltas
			time.Sleep(50 * time.Millisecond)
			conn.Close()
		},
		func(conn *websocket.Conn) { send(conn, lines[:3]) },
	}}
	var mu sync.Mutex
	var events []BookEvent
	c := &Client{Ticker: openTicker, OnEvent: func(ev BookEvent) {
		mu.Lock()
		events = append(events, ev)
		mu.Unlock()
	}}
	stop := runClient(t, v, c)
	waitFor(t, 10*time.Second, "both sessions' events", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(events) >= 7
	})
	stop()

	var got []string
	for _, ev := range events {
		got = append(got, fmt.Sprintf("%s/%d", ev.Type, ev.Seq))
	}
	want := "[snapshot/1 delta/2 delta/3 delta/4 stale/0 snapshot/1 delta/2]"
	if fmt.Sprint(got) != want {
		t.Fatalf("events %v, want %s", got, want)
	}
	if lost := events[4]; !lost.Stale || lost.StaleReason != ReasonReconnect || lost.Recv.IsZero() {
		t.Errorf("connection-loss event: %+v", lost)
	}
}
