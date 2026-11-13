package opens

import (
	"context"
	"net/http"
	"path/filepath"
	"sync"
	"time"

	"github.com/lucasjohn05/pmde/internal/kalshi"
	"github.com/lucasjohn05/pmde/internal/metrics"
	"github.com/lucasjohn05/pmde/internal/storage"
)

// EventRecord is one line of events/<series>/<day>.jsonl: one book
// event of one market's open. Every line carries the market's identity,
// so a line stands on its own under grep or jq.
//
// Prices are basis points, sizes hundredths of a contract.
type EventRecord struct {
	Market string `json:"market"`
	Series string `json:"series"`
	Event  string `json:"event"`

	// The market's rung within its event: Kalshi's yes_sub_title and
	// strike fields, as given.
	Rung       string `json:"rung,omitempty"`
	StrikeType string `json:"strike_type,omitempty"`
	Floor      string `json:"floor,omitempty"`
	Cap        string `json:"cap,omitempty"`

	OpenMs int64 `json:"open_ms"` // the market's open_time, Unix ms

	Type   string `json:"type"`            // snapshot, delta or stale (kalshi.BookEvent)
	Seq    int64  `json:"seq,omitempty"`   // the message's seq
	TsMs   int64  `json:"ts_ms,omitempty"` // the venue's timestamp; snapshots have none
	RecvMs int64  `json:"recv_ms"`         // when this host read the message, Unix ms

	// A delta's level and signed change.
	Side  string `json:"side,omitempty"`
	Price *int   `json:"price,omitempty"`
	Delta *int64 `json:"delta,omitempty"`

	// A snapshot's whole book: [price, size] pairs, by price.
	Yes [][2]int64 `json:"yes,omitempty"`
	No  [][2]int64 `json:"no,omitempty"`

	// The best yes bid and ask after the event, each present only if
	// that side has resting size.
	Bid   *int   `json:"bid,omitempty"`
	BidSz *int64 `json:"bid_sz,omitempty"`
	Ask   *int   `json:"ask,omitempty"`
	AskSz *int64 `json:"ask_sz,omitempty"`

	// Stale is set when the book is untrusted after the event: what
	// broke it (seq_gap, dropped, invalid_update, reconnect), or
	// "unsynced" before the first snapshot. The capture has a hole from
	// here to the next snapshot.
	Stale string `json:"stale,omitempty"`
}

// CaptureRecord is one line of captures.jsonl, written when a capture
// ends.
type CaptureRecord struct {
	Market  string `json:"market"`
	Series  string `json:"series"`
	Event   string `json:"event"`
	OpenMs  int64  `json:"open_ms"`
	StartMs int64  `json:"start_ms"` // when the capture began connecting
	EndMs   int64  `json:"end_ms"`
	Events  int    `json:"events"`  // EventRecords written
	Resyncs int    `json:"resyncs"` // times a trusted book was lost

	// Complete is true if the book was synced before open_time and
	// never lost: the record holds every book event from the open on.
	Complete bool `json:"complete"`
}

func eventRecord(o Opening, ev kalshi.BookEvent) EventRecord {
	m := o.Market
	r := EventRecord{
		Market: m.Ticker, Series: o.Series, Event: m.Event,
		Rung: m.Rung, StrikeType: m.StrikeType, Floor: m.FloorStrike, Cap: m.CapStrike,
		OpenMs: m.Open.UnixMilli(),
		Type:   ev.Type, Seq: ev.Seq, TsMs: ev.VenueMs, RecvMs: ev.Recv.UnixMilli(),
	}
	switch ev.Type {
	case kalshi.EventDelta:
		r.Side, r.Price, r.Delta = ev.Side, &ev.PriceBps, &ev.DeltaCenti
	case kalshi.EventSnapshot:
		r.Yes, r.No = levelPairs(ev.Yes), levelPairs(ev.No)
	}
	if ev.HasBid {
		r.Bid, r.BidSz = &ev.BidBps, &ev.BidSize
	}
	if ev.HasAsk {
		r.Ask, r.AskSz = &ev.AskBps, &ev.AskSize
	}
	if ev.Stale {
		r.Stale = ev.StaleReason
		if r.Stale == "" {
			r.Stale = "unsynced"
		}
	}
	return r
}

func levelPairs(levels []kalshi.Level) [][2]int64 {
	if len(levels) == 0 {
		return nil
	}
	out := make([][2]int64, len(levels))
	for i, l := range levels {
		out[i] = [2]int64{int64(l.PriceBps), l.SizeCenti}
	}
	return out
}

const (
	// RecorderQueueSize is the capacity of the channel from the capture
	// book goroutines to the Recorder. As with the tick writer, the send
	// blocks when it's full.
	RecorderQueueSize = 4096

	recorderFlush = time.Second

	// recorderIdle is how long a file goes unwritten before it's closed.
	// Captures last about a minute, so files don't stay open all day.
	recorderIdle = 2 * time.Minute
)

// Recorder is the opens writer: capture book goroutines hand it
// EventRecords with Send, and Run, its one goroutine, appends them to
// events/<series>/<day>.jsonl, where day is the UTC day of the market's
// open_time (so one capture never spans two files). Files are flushed
// and fsynced every second, like the tick segments.
type Recorder struct {
	dir     string // <data-dir>/opens
	in      chan EventRecord
	metrics *metrics.Opens
	logf    func(format string, args ...any)

	files map[string]*recorderFile // by path
	bad   map[string]bool          // paths whose error has been logged
}

type recorderFile struct {
	a    *storage.Appender
	used time.Time
}

// NewRecorder returns a Recorder writing under dir. m and logf may be
// nil.
func NewRecorder(dir string, m *metrics.Opens, logf func(format string, args ...any)) *Recorder {
	return &Recorder{
		dir: dir, in: make(chan EventRecord, RecorderQueueSize), metrics: m, logf: logf,
		files: map[string]*recorderFile{}, bad: map[string]bool{},
	}
}

// Send hands rec to the Recorder, blocking while its queue is full
// (CLAUDE.md: stages downstream of the book block); it gives up only
// when ctx is cancelled. A blocked send holds up that capture's book
// goroutine, and if it lasts, the capture's reader drops and the
// capture records a resync. Time spent blocked is counted.
func (r *Recorder) Send(ctx context.Context, rec EventRecord) {
	select {
	case r.in <- rec:
		return
	default:
	}
	start := time.Now()
	select {
	case r.in <- rec:
	case <-ctx.Done():
	}
	r.metrics.WriterBlocked(time.Since(start))
}

// Run writes records until ctx is cancelled, then writes what's still
// queued and closes every file.
func (r *Recorder) Run(ctx context.Context) {
	ticker := time.NewTicker(recorderFlush)
	defer ticker.Stop()
	for {
		select {
		case rec := <-r.in:
			r.write(rec)
		case now := <-ticker.C:
			r.flush(now)
		case <-ctx.Done():
			for drained := false; !drained; {
				select {
				case rec := <-r.in:
					r.write(rec)
				default:
					drained = true
				}
			}
			for path, f := range r.files {
				r.check(path, f.a.Close())
			}
			r.files = map[string]*recorderFile{}
			return
		}
	}
}

func (r *Recorder) write(rec EventRecord) {
	if !storage.SafeComponent(rec.Series) {
		r.check(rec.Series, errUnsafeSeries)
		return
	}
	day := time.UnixMilli(rec.OpenMs).UTC().Format("2006-01-02")
	path := filepath.Join(r.dir, eventsDir, rec.Series, day+".jsonl")
	f := r.files[path]
	if f == nil {
		a, cut, err := storage.OpenAppender(path)
		if !r.check(path, err) {
			return
		}
		if cut > 0 && r.logf != nil {
			r.logf("opens: %s ended in a partial line (%d bytes, from a crash); cut it off", path, cut)
		}
		f = &recorderFile{a: a}
		r.files[path] = f
	}
	f.used = time.Now()
	if r.check(path, f.a.Append(rec)) {
		r.metrics.Event()
	}
}

func (r *Recorder) flush(now time.Time) {
	for path, f := range r.files {
		r.check(path, f.a.Flush())
		if now.Sub(f.used) > recorderIdle {
			r.check(path, f.a.Close())
			delete(r.files, path)
		}
	}
}

// check logs err once per path until that path works again, and
// reports whether there was none.
func (r *Recorder) check(path string, err error) bool {
	if err == nil {
		delete(r.bad, path)
		return true
	}
	if !r.bad[path] && r.logf != nil {
		r.logf("opens: %s: %v", path, err)
	}
	r.bad[path] = true
	return false
}

type constError string

func (e constError) Error() string { return string(e) }

const errUnsafeSeries = constError("not recording: the series ticker is not a safe directory name")

// captureTick is how often the Capturer checks the Schedule. It bounds
// how much of Lead can be lost before a capture starts connecting.
const captureTick = 250 * time.Millisecond

// Capturer records market opens. Each market that comes due gets one
// kalshi.Client, the same client cmd/pmde runs, on its own connection:
// it subscribes Lead before open_time, records every book event until
// Window after it, and then closes the connection, which ends the
// subscription.
//
// Subscribing before the open works: Kalshi answers with an empty
// snapshot, and the first deltas are the opening orders. Reconnects and
// resyncs are the Client's; a capture that lost its book has stale
// records marking the hole and is flagged incomplete.
//
// At most Cfg.MaxConcurrent captures run at once. An open past the cap
// is skipped and counted in opens_skipped_total, never queued: a
// capture that starts late has missed what it was for.
type Capturer struct {
	URL    string                      // the websocket endpoint
	Header func() (http.Header, error) // auth headers for each dial

	Cfg      Config
	Schedule *Schedule
	Recorder *Recorder
	Dir      string // <data-dir>/opens
	Metrics  *metrics.Opens
	Logf     func(format string, args ...any)
	Now      func() time.Time // nil: time.Now

	mu sync.Mutex // serializes appends to captures.jsonl
}

func (c *Capturer) now() time.Time {
	if c.Now != nil {
		return c.Now()
	}
	return time.Now()
}

func (c *Capturer) logf(format string, args ...any) {
	if c.Logf != nil {
		c.Logf(format, args...)
	}
}

// Run starts a capture for each market as it comes due, until ctx is
// cancelled; it returns once the captures in flight have ended.
func (c *Capturer) Run(ctx context.Context) {
	slots := make(chan struct{}, c.Cfg.MaxConcurrent)
	var wg sync.WaitGroup
	defer wg.Wait()
	ticker := time.NewTicker(captureTick)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		due, late := c.Schedule.Due(c.now(), c.Cfg.Lead)
		if len(late) > 0 {
			c.Metrics.Skipped(metrics.SkipLate, len(late))
			c.logf("opens: skipped %d market(s) already past their open_time (first: %s)", len(late), late[0].Market.Ticker)
		}
		skipped := 0
		for _, o := range due {
			select {
			case slots <- struct{}{}:
				wg.Go(func() {
					defer func() { <-slots }()
					c.capture(ctx, o)
				})
			default:
				skipped++
			}
		}
		if skipped > 0 {
			c.Metrics.Skipped(metrics.SkipConcurrency, skipped)
			c.logf("opens: skipped %d open(s): %d captures already running (max_concurrent)", skipped, c.Cfg.MaxConcurrent)
		}
	}
}

// capture records one market's open.
func (c *Capturer) capture(ctx context.Context, o Opening) {
	c.Metrics.CaptureStarted()
	defer c.Metrics.CaptureDone()

	start := c.now()
	openMs := o.Market.Open.UnixMilli()
	rec := CaptureRecord{
		Market: o.Market.Ticker, Series: o.Series, Event: o.Market.Event,
		OpenMs: openMs, StartMs: start.UnixMilli(),
	}
	// Touched only from OnEvent, which the Client never calls
	// concurrently, and read after Run returns.
	syncedBeforeOpen, trusted := false, false

	cctx, cancel := context.WithTimeout(ctx, o.Market.Open.Add(c.Cfg.Window).Sub(start))
	defer cancel()
	client := &kalshi.Client{
		URL:    c.URL,
		Ticker: o.Market.Ticker,
		Header: c.Header,
		Logf:   func(format string, args ...any) { c.logf("["+o.Market.Ticker+"] "+format, args...) },
		OnEvent: func(ev kalshi.BookEvent) {
			if ev.Stale && trusted {
				rec.Resyncs++
				c.Metrics.Resync(ev.StaleReason)
			}
			trusted = !ev.Stale
			if trusted && ev.Recv.UnixMilli() <= openMs {
				syncedBeforeOpen = true
			}
			rec.Events++
			// ctx, not cctx: the events of the window's last instant
			// are still wanted.
			c.Recorder.Send(ctx, eventRecord(o, ev))
		},
	}
	client.Run(cctx)

	rec.EndMs = c.now().UnixMilli()
	rec.Complete = syncedBeforeOpen && rec.Resyncs == 0
	c.mu.Lock()
	err := storage.AppendLine(filepath.Join(c.Dir, capturesFile), rec)
	c.mu.Unlock()
	if err != nil {
		c.logf("opens: %v", err)
	}
	if !rec.Complete {
		c.logf("opens: %s: incomplete capture (%d events, %d resyncs, synced before open: %v)",
			rec.Market, rec.Events, rec.Resyncs, syncedBeforeOpen)
	}
}
