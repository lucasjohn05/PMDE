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

	// OnBest is called whenever the trusted top of book changes,
	// including to ok=false when the book goes stale or the connection is
	// lost. It runs on the book goroutine, so a slow OnBest backs up the
	// queue.
	OnBest func(bidBps, askBps int, ok bool)

	// Logf receives operational log lines. Nil discards them.
	Logf func(format string, args ...any)

	// Test hooks; zero in production.
	snapshotTimeout time.Duration  // overrides defaultSnapshotTimeout
	onQueue         func(q *Queue) // called with each session's queue
	onFeed          func(f *Feed)  // called on the book goroutine after each step

	// Last top of book passed to OnBest. Touched by one session's book
	// goroutine at a time, and by Run between sessions.
	lastBid, lastAsk int
	lastOK           bool
}

// Run keeps the book live until ctx is cancelled. It never gives up:
// failures are logged and retried with backoff.
func (c *Client) Run(ctx context.Context) {
	attempt := 0
	for {
		start := time.Now()
		reason, err := c.session(ctx)
		c.report(0, 0, false) // whatever ended the session, the book went with it
		if ctx.Err() != nil {
			return
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
// ReasonReconnect / ReasonSnapshotTimeout.
func (c *Client) session(ctx context.Context) (reason string, err error) {
	var header http.Header
	if c.Header != nil {
		if header, err = c.Header(); err != nil {
			return ReasonReconnect, fmt.Errorf("building headers: %w", err)
		}
	}
	conn, resp, err := websocket.DefaultDialer.DialContext(ctx, c.URL, header)
	if err != nil {
		if resp != nil {
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
			return ReasonReconnect, fmt.Errorf("dial: %w (status %d: %s)", err, resp.StatusCode, body)
		}
		return ReasonReconnect, fmt.Errorf("dial: %w", err)
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
		return ReasonReconnect, err
	}
	conn.SetWriteDeadline(time.Now().Add(writeTimeout))
	if err := conn.WriteMessage(websocket.TextMessage, sub); err != nil {
		return ReasonReconnect, fmt.Errorf("subscribe: %w", err)
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
		q.Offer(msg)
	}
	q.Close()
	<-bookDone
	if n := q.Drops(); n > 0 {
		c.logf("kalshi: %d message(s) dropped this session", n)
	}

	select {
	case reason := <-staleReason:
		return reason, nil
	default:
	}
	if timedOut.Load() {
		return ReasonSnapshotTimeout, fmt.Errorf("no snapshot within %v of subscribing", snapshotTimeout)
	}
	return ReasonReconnect, readErr
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

		switch {
		case wasStale && !f.Stale():
			synced.Store(true)
			c.logf("kalshi: book synced from snapshot (seq %d)", f.Book().LastSeq())
		case !wasStale && f.Stale():
			// The book can't be repaired in place. End the session so Run
			// reconnects and resubscribes for a fresh snapshot. Whatever
			// is still queued belongs to the broken stream, so it's
			// drained unread.
			staleReason <- f.StaleReason()
			c.report(f.Best())
			conn.Close()
			for range q.Messages() {
			}
			return
		}

		c.report(f.Best())
		if !more {
			return
		}
	}
}

// report passes the top of book to OnBest if it changed.
func (c *Client) report(bidBps, askBps int, ok bool) {
	if !ok {
		bidBps, askBps = 0, 0
	}
	if bidBps == c.lastBid && askBps == c.lastAsk && ok == c.lastOK {
		return
	}
	c.lastBid, c.lastAsk, c.lastOK = bidBps, askBps, ok
	if c.OnBest != nil {
		c.OnBest(bidBps, askBps, ok)
	}
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
