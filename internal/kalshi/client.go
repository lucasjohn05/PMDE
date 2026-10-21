package kalshi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"github.com/lucasjohn05/pmde/internal/metrics"
	"github.com/lucasjohn05/pmde/internal/tick"
)

// Reasons a session ends other than the book going stale (see the Feed's
// Reason constants). Together they are the reasons the Client resyncs.
const (
	ReasonReconnect       = "reconnect"        // the connection failed or was lost
	ReasonSnapshotTimeout = "snapshot_timeout" // subscribed, but no snapshot arrived
)

const (
	// Reconnect backoff: exponential from backoffBase, capped at
	// backoffMax, with jitter (see backoff). A session that stayed up for
	// healthyAfter resets it, so an occasional drop reconnects quickly
	// while a connection that keeps failing backs off.
	backoffBase  = 500 * time.Millisecond
	backoffMax   = 30 * time.Second
	healthyAfter = time.Minute

	// defaultSnapshotTimeout bounds how long a session waits after
	// subscribing for its orderbook_snapshot. Without it, a rejected
	// subscription would leave the book untrusted forever with nothing to
	// trigger a reconnect.
	defaultSnapshotTimeout = 10 * time.Second

	// readTimeout closes a connection that has gone silent. Kalshi sends
	// protocol-level pings (every ~10s), and every message or ping pushes
	// the deadline out, so a minute of nothing means the connection is
	// dead even if TCP hasn't noticed.
	readTimeout  = time.Minute
	writeTimeout = 10 * time.Second
)

// Client keeps a live Feed for one Kalshi market. It connects, subscribes,
// and runs the reader and book goroutines. When the connection is lost or
// the book goes stale, it reconnects and rebuilds from a fresh snapshot.
//
// Resync and reconnect are the same operation here. A stale book is fixed
// by closing the connection and taking the normal reconnect path: a new
// connection means a new subscription, and Kalshi starts every
// subscription with an orderbook_snapshot. Each session gets a fresh Feed
// and Queue, so nothing from the old book survives, and seq is validated
// from the new snapshot's seq onward.
type Client struct {
	URL    string
	Ticker string

	// Header returns the headers for each dial. Kalshi's auth signature
	// covers a timestamp, so it has to be computed fresh per connection.
	// Nil means no extra headers.
	Header func() (http.Header, error)

	// OnTick is called whenever the trusted top of book changes (a best
	// price or the size at one), including to OK=false when the book goes stale or the connection is
	// lost. It runs on the book goroutine, so a slow OnTick backs up the
	// queue (and, past QueueSize, makes the reader drop and the book
	// resync).
	OnTick func(tick.Tick)

	// Logf receives operational log lines. Nil discards them.
	Logf func(format string, args ...any)

	// Metrics records this market's metrics. Nil records nothing.
	Metrics *metrics.Market

	// Test hooks; zero in production.
	snapshotTimeout time.Duration  // overrides defaultSnapshotTimeout
	onQueue         func(q *Queue) // called with each session's queue
	onFeed          func(f *Feed)  // called on the book goroutine after each step

	// Last top of book passed to OnTick. Touched by one session's book
	// goroutine at a time, and by Run between sessions.
	last top
}

// top is the part of a Tick that decides whether it changed.
type top struct {
	bid, ask     int
	bidSz, askSz int64
	ok           bool
}

// Run keeps the book live until ctx is cancelled. It never gives up:
// failures are logged and retried with backoff.
func (c *Client) Run(ctx context.Context) {
	attempt := 0
	for {
		if attempt > 0 {
			c.Metrics.Reconnect()
		}
		start := time.Now()
		reason, hadBook, err := c.session(ctx)
		c.report(top{}, time.Time{}, 0) // whatever ended the session, the book went with it
		c.Metrics.SetStale(true)
		if ctx.Err() != nil {
			return
		}
		// A resync is the loss of a trusted book. A session that never
		// synced (a failed dial, a snapshot timeout) lost nothing; it
		// shows up in reconnects_total only.
		if hadBook {
			c.Metrics.Resync(reason)
		}

		if time.Since(start) >= healthyAfter {
			attempt = 0
		}
		delay := backoff(attempt, rand.Int64N)
		attempt++
		if err != nil {
			c.logf("kalshi: resync (%s): %v; reconnecting in %v", reason, err, delay.Round(time.Millisecond))
		} else {
			c.logf("kalshi: resync (%s); reconnecting in %v", reason, delay.Round(time.Millisecond))
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
	}
}

// session runs one connection from dial to close and reports why it
// ended: one of the Feed's Reason constants if the book went stale, or
// ReasonReconnect / ReasonSnapshotTimeout. hadBook reports whether the
// book was trusted at some point during the session.
func (c *Client) session(ctx context.Context) (reason string, hadBook bool, err error) {
	var header http.Header
	if c.Header != nil {
		if header, err = c.Header(); err != nil {
			return ReasonReconnect, false, fmt.Errorf("building headers: %w", err)
		}
	}
	conn, resp, err := websocket.DefaultDialer.DialContext(ctx, c.URL, header)
	if err != nil {
		if resp != nil {
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
			return ReasonReconnect, false, fmt.Errorf("dial: %w (status %d: %s)", err, resp.StatusCode, body)
		}
		return ReasonReconnect, false, fmt.Errorf("dial: %w", err)
	}
	// Closing the connection is how every session ends: the reader's
	// ReadMessage fails, it closes the queue, and the book goroutine
	// drains and returns. Cancelling ctx does the same.
	defer conn.Close()
	stop := context.AfterFunc(ctx, func() { conn.Close() })
	defer stop()
	c.logf("kalshi: connected")

	conn.SetReadDeadline(time.Now().Add(readTimeout))
	conn.SetPingHandler(func(data string) error {
		conn.SetReadDeadline(time.Now().Add(readTimeout))
		// Same as gorilla's default handler: reply with a pong.
		err := conn.WriteControl(websocket.PongMessage, []byte(data), time.Now().Add(writeTimeout))
		var netErr net.Error
		if errors.Is(err, websocket.ErrCloseSent) || (errors.As(err, &netErr) && netErr.Timeout()) {
			return nil
		}
		return err
	})

	sub, err := c.subscribeMsg()
	if err != nil {
		return ReasonReconnect, false, err
	}
	conn.SetWriteDeadline(time.Now().Add(writeTimeout))
	if err := conn.WriteMessage(websocket.TextMessage, sub); err != nil {
		return ReasonReconnect, false, fmt.Errorf("subscribe: %w", err)
	}

	q := NewQueue(QueueSize)
	if c.onQueue != nil {
		c.onQueue(q)
	}

	snapshotTimeout := defaultSnapshotTimeout
	if c.snapshotTimeout > 0 {
		snapshotTimeout = c.snapshotTimeout
	}
	var synced, timedOut atomic.Bool
	timer := time.AfterFunc(snapshotTimeout, func() {
		if !synced.Load() {
			timedOut.Store(true)
			conn.Close()
		}
	})
	defer timer.Stop()

	staleReason := make(chan string, 1)
	bookDone := make(chan struct{})
	go func() {
		defer close(bookDone)
		c.runBook(q, conn, &synced, staleReason)
	}()

	// The reader: read and hand off, nothing else.
	var readErr error
	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			readErr = err
			break
		}
		conn.SetReadDeadline(time.Now().Add(readTimeout))
		c.Metrics.Message()
		if !q.Offer(msg) {
			c.Metrics.Dropped()
		}
	}
	q.Close()
	<-bookDone
	if n := q.Drops(); n > 0 {
		c.logf("kalshi: %d message(s) dropped this session", n)
	}

	select {
	case reason := <-staleReason:
		return reason, true, nil
	default:
	}
	if timedOut.Load() {
		return ReasonSnapshotTimeout, false, fmt.Errorf("no snapshot within %v of subscribing", snapshotTimeout)
	}
	return ReasonReconnect, synced.Load(), readErr
}

// runBook is the book goroutine for one session: the only goroutine that
// touches this session's Feed.
func (c *Client) runBook(q *Queue, conn *websocket.Conn, synced *atomic.Bool, staleReason chan<- string) {
	var f Feed
	for {
		wasStale := f.Stale()
		_, more, err := f.Next(q)
		if err != nil {
			c.logf("%v", err) // errors carry their own "kalshi:" prefix
		}
		if c.onFeed != nil {
			c.onFeed(&f)
		}
		if lag, ok := f.Lag(); ok {
			c.Metrics.ObserveLag(lag)
		}

		switch {
		case wasStale && !f.Stale():
			synced.Store(true)
			c.Metrics.SetStale(false)
			c.logf("kalshi: book synced from snapshot (seq %d)", f.Book().LastSeq())
		case !wasStale && f.Stale():
			// The book can't be repaired in place. End the session so Run
			// reconnects and resubscribes for a fresh snapshot. Whatever
			// is still queued belongs to the broken stream, so it's
			// drained unread.
			staleReason <- f.StaleReason()
			c.Metrics.SetStale(true)
			c.emit(&f)
			conn.Close()
			for range q.Messages() {
			}
			return
		}

		c.emit(&f)
		if !more {
			return
		}
	}
}

// emit reports the Feed's top of book, stamped with the message the
// last step handled.
func (c *Client) emit(f *Feed) {
	var t top
	t.bid, t.ask, t.ok = f.Best()
	t.bidSz, t.askSz = f.TopSizes()
	recv, venueMs := f.Stamp()
	c.report(t, recv, venueMs)
}

// report passes the top of book to OnTick if it changed. recv is the
// receive time of the message behind it (zero means now); venueMs the
// venue's timestamp on it (0 if none).
func (c *Client) report(t top, recv time.Time, venueMs int64) {
	if !t.ok {
		t = top{}
	}
	if t == c.last {
		return
	}
	c.last = t
	if c.OnTick == nil {
		return
	}
	if recv.IsZero() {
		recv = time.Now()
	}
	var vt time.Time
	if venueMs > 0 {
		vt = time.UnixMilli(venueMs)
	}
	c.OnTick(tick.Tick{
		Venue: tick.Kalshi, Market: c.Ticker,
		BidBps: t.bid, AskBps: t.ask, BidSize: t.bidSz, AskSize: t.askSz, OK: t.ok,
		Time: recv, VenueTime: vt,
	})
}

func (c *Client) subscribeMsg() ([]byte, error) {
	type params struct {
		Channels     []string `json:"channels"`
		MarketTicker string   `json:"market_ticker"`
	}
	msg, err := json.Marshal(struct {
		ID     int    `json:"id"`
		Cmd    string `json:"cmd"`
		Params params `json:"params"`
	}{1, "subscribe", params{[]string{"orderbook_delta"}, c.Ticker}})
	if err != nil {
		return nil, fmt.Errorf("encoding subscribe: %w", err)
	}
	return msg, nil
}

func (c *Client) logf(format string, args ...any) {
	if c.Logf != nil {
		c.Logf(format, args...)
	}
}

// backoff returns how long to wait before reconnect attempt n (0-based):
// a window growing exponentially from backoffBase, capped at backoffMax,
// with "equal jitter", a random point in the upper half of the window.
// The floor keeps a connection that keeps failing from retrying in a
// tight loop, and the randomness keeps reconnects that share a cause (a
// network blip hitting every client at once) from retrying in lockstep.
// randN is rand.Int64N, injected for tests.
func backoff(attempt int, randN func(int64) int64) time.Duration {
	window := backoffMax
	if attempt < 16 { // beyond this, the shift is far past the cap anyway
		window = min(backoffBase<<attempt, backoffMax)
	}
	half := window / 2
	return half + time.Duration(randN(int64(half)))
}
