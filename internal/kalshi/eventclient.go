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

// EventClient keeps the books of several markets live over one
// connection and one orderbook_delta subscription: an event's rungs,
// for the open scanner. It reports every book message through OnEvent
// and has no Ticks; cmd/pmde's one-market Client is a separate type
// and is not changed by this one.
//
// It recovers the way Client does: whatever breaks the books (a seq
// gap, a dropped message, a bad update, a lost connection), the
// connection is closed and a new one subscribes again, and Kalshi
// starts every subscription with a snapshot per market. Because one
// seq runs across all the markets (see MultiFeed), any break is a
// break of every book on the connection.
type EventClient struct {
	URL     string
	Tickers []string // the markets to subscribe, at least one

	// Header returns the auth headers for each dial; see Client.Header.
	Header func() (http.Header, error)

	// OnEvent is called with every orderbook snapshot and delta, and
	// with an EventStale event per market when the books are lost. It
	// runs on the book goroutine (or on Run's, between sessions), never
	// concurrently, so a slow OnEvent backs up the queue and, past
	// QueueSize, makes the reader drop and the books resync.
	OnEvent func(BookEvent)

	// Logf receives operational log lines. Nil discards them.
	Logf func(format string, args ...any)

	snapshotTimeout time.Duration // test hook; overrides defaultSnapshotTimeout
}

func (c *EventClient) logf(format string, args ...any) {
	if c.Logf != nil {
		c.Logf(format, args...)
	}
}

func (c *EventClient) emit(ev BookEvent) {
	if c.OnEvent != nil {
		c.OnEvent(ev)
	}
}

// Run keeps the books live until ctx is cancelled, reconnecting with
// Client's backoff.
func (c *EventClient) Run(ctx context.Context) {
	for attempt := 0; ; {
		start := time.Now()
		reason, hadBooks, reported, err := c.session(ctx)
		if ctx.Err() != nil {
			return
		}
		// Books lost with the connection have no message to mark the
		// loss; a break the feed saw was reported from the book
		// goroutine.
		if hadBooks && !reported {
			for _, t := range c.Tickers {
				c.emit(staleEvent(t, reason))
			}
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

// session runs one connection from dial to close. reason is why it
// ended; hadBooks whether any market had a trusted book during it;
// reported whether the loss of those books was already passed to
// OnEvent.
func (c *EventClient) session(ctx context.Context) (reason string, hadBooks, reported bool, err error) {
	var header http.Header
	if c.Header != nil {
		if header, err = c.Header(); err != nil {
			return ReasonReconnect, false, false, fmt.Errorf("building headers: %w", err)
		}
	}
	conn, resp, err := websocket.DefaultDialer.DialContext(ctx, c.URL, header)
	if err != nil {
		if resp != nil {
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
			return ReasonReconnect, false, false, fmt.Errorf("dial: %w (status %d: %s)", err, resp.StatusCode, body)
		}
		return ReasonReconnect, false, false, fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()
	stop := context.AfterFunc(ctx, func() { conn.Close() })
	defer stop()

	conn.SetReadDeadline(time.Now().Add(readTimeout))
	conn.SetPingHandler(func(data string) error {
		conn.SetReadDeadline(time.Now().Add(readTimeout))
		err := conn.WriteControl(websocket.PongMessage, []byte(data), time.Now().Add(writeTimeout))
		var netErr net.Error
		if errors.Is(err, websocket.ErrCloseSent) || (errors.As(err, &netErr) && netErr.Timeout()) {
			return nil
		}
		return err
	})

	type params struct {
		Channels      []string `json:"channels"`
		MarketTickers []string `json:"market_tickers"`
	}
	sub, err := json.Marshal(struct {
		ID     int    `json:"id"`
		Cmd    string `json:"cmd"`
		Params params `json:"params"`
	}{1, "subscribe", params{[]string{"orderbook_delta"}, c.Tickers}})
	if err != nil {
		return ReasonReconnect, false, false, fmt.Errorf("encoding subscribe: %w", err)
	}
	conn.SetWriteDeadline(time.Now().Add(writeTimeout))
	if err := conn.WriteMessage(websocket.TextMessage, sub); err != nil {
		return ReasonReconnect, false, false, fmt.Errorf("subscribe: %w", err)
	}

	q := NewQueue(QueueSize)
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

	// Written by the book goroutine, read after bookDone.
	var broke string
	var any bool
	bookDone := make(chan struct{})
	go func() {
		defer close(bookDone)
		f := NewMultiFeed(c.Tickers)
		defer func() { any = f.AnySynced() }()
		for {
			ev, got, more, err := f.Next(q)
			if err != nil {
				c.logf("%v", err)
			}
			if got {
				c.emit(ev)
			}
			if !synced.Load() && f.Synced() {
				synced.Store(true)
				c.logf("kalshi: %d book(s) synced from snapshots", len(c.Tickers))
			}
			if broken, why := f.Broken(); broken {
				// Every book on the connection is lost. Say so for each
				// market (the one whose message showed it has just been
				// told), then end the session for a fresh subscription.
				for _, t := range c.Tickers {
					if !got || t != ev.Market {
						c.emit(staleEvent(t, why))
					}
				}
				broke = why
				conn.Close()
				for range q.Messages() {
				}
				return
			}
			if !more {
				return
			}
		}
	}()

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

	switch {
	case broke != "":
		return broke, any, true, nil
	case timedOut.Load():
		return ReasonSnapshotTimeout, any, false,
			fmt.Errorf("not every market had a snapshot within %v of subscribing", snapshotTimeout)
	}
	return ReasonReconnect, any, false, readErr
}
