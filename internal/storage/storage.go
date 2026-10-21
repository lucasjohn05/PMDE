// Package storage writes Ticks to append-only JSON Lines segment files,
// with 1s/1m/1h rollups computed as the Ticks are written.
//
// Layout, under the data directory:
//
//	<venue>/<market>/<YYYY-MM-DD>.ticks.jsonl
//	<venue>/<market>/<YYYY-MM-DD>.1s.jsonl   (and .1m, .1h)
//	index.json                               (directory -> pair names)
//
// One file per record kind per market per UTC day. Days and windows are
// on the local receive clock (Tick.Time), shared by both venues, so a
// window never spans two files and the two venues' records line up.
package storage

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/lucasjohn05/pmde/internal/metrics"
	"github.com/lucasjohn05/pmde/internal/tick"
)

// QueueSize is the capacity of the channel from the book goroutines to
// the Writer. Ticks only come on top-of-book changes, and appending one
// is a buffered write, so this only has to cover short stalls: the
// once-a-second flush and fsync, mostly. Per CLAUDE.md the send blocks
// when it's full (see Send).
const QueueSize = 1024

const (
	// flushInterval is how often buffered records are written out and
	// fsynced, and finished windows are closed. A crash loses at most
	// about this much.
	flushInterval = time.Second

	// closeGrace is how long past a window's end the flush waits before
	// closing it, so a Tick still in the queues when the window ends
	// lands in it rather than in the next one.
	closeGrace = time.Second

	// blockedLogAfter is how long a single send has to block on a full
	// queue before it's logged, as well as counted.
	blockedLogAfter = time.Second
)

// kind is a record kind: ticks, or bars of one window size.
type kind int

const (
	kindTicks kind = iota
	kind1s
	kind1m
	kind1h
	numKinds
)

var (
	kindNames = [numKinds]string{"ticks", "1s", "1m", "1h"}
	windowMs  = [numKinds]int64{0, 1000, 60 * 1000, 60 * 60 * 1000}
)

// Writer is the storage stage. Book goroutines hand it Ticks with Send;
// Run, its one goroutine, appends them and maintains the rollups. Run
// owns all file and rollup state.
type Writer struct {
	dir     string
	in      chan tick.Tick
	metrics *metrics.Metrics
	logf    func(format string, args ...any)

	streams map[streamKey]*stream
	bad     map[streamKey]bool // markets refused for an unsafe directory name
}

type streamKey struct{ venue, market string }

// NewWriter returns a Writer storing under dir, creating it if needed. m
// and logf may be nil.
func NewWriter(dir string, m *metrics.Metrics, logf func(format string, args ...any)) (*Writer, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("storage: %w", err)
	}
	return &Writer{
		dir:     dir,
		in:      make(chan tick.Tick, QueueSize),
		metrics: m,
		logf:    logf,
		streams: map[streamKey]*stream{},
		bad:     map[streamKey]bool{},
	}, nil
}

// Send hands t to the Writer, blocking while its queue is full; it gives
// up only when ctx is cancelled. Blocking is deliberate (CLAUDE.md:
// stages downstream of the book block): a dropped Tick would leave a
// wrong top of book in the record with nothing to say so. Time spent
// blocked is added to storage_blocked_seconds_total, and a single block
// of a second or more is logged, so a slow disk is visible before it
// turns into resyncs.
func (w *Writer) Send(ctx context.Context, t tick.Tick) {
	select {
	case w.in <- t:
		return
	default:
	}
	start := time.Now()
	select {
	case w.in <- t:
	case <-ctx.Done():
	}
	blocked := time.Since(start)
	w.metrics.StorageBlocked(blocked)
	if blocked >= blockedLogAfter {
		w.log("storage: a %s tick waited %v for room in the storage queue (slow disk?)", t.Venue, blocked.Round(time.Millisecond))
	}
}

// Run writes Ticks until ctx is cancelled, then writes what's still
// queued, closes the open windows as partial, and flushes and closes
// every file.
func (w *Writer) Run(ctx context.Context) {
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()
	for {
		select {
		case t := <-w.in:
			w.write(t)
		case now := <-ticker.C:
			w.flush(now)
		case <-ctx.Done():
			for drained := false; !drained; {
				select {
				case t := <-w.in:
					w.write(t)
				default:
					drained = true
				}
			}
			w.close(time.Now())
			return
		}
	}
}

// stream is one market's files and rollup state.
type stream struct {
	dir  string // <data>/<venue>/<market>
	segs [numKinds]segment
	bars [numKinds]bar // kindTicks unused

	// The book's state as of the last Tick, and whether there has been
	// one yet: before the first Tick after startup the state is unknown.
	state     tick.Tick
	known     bool
	changedMs int64 // when state last changed (on the window clock)

	lastMs    int64           // latest Tick time seen; window time never goes backwards
	closedEnd [numKinds]int64 // end of the last closed window, per kind
}

func (w *Writer) stream(venue, market string) *stream {
	key := streamKey{venue, market}
	if s := w.streams[key]; s != nil {
		return s
	}
	if w.bad[key] {
		return nil
	}
	if !safeComponent(venue) || !safeComponent(market) {
		w.bad[key] = true
		w.log("storage: not storing %s market %q: not a safe directory name", venue, market)
		return nil
	}
	s := &stream{dir: filepath.Join(w.dir, venue, market)}
	w.streams[key] = s
	return s
}

// write appends t to its market's tick file and folds it into the open
// windows.
func (w *Writer) write(t tick.Tick) {
	s := w.stream(t.Venue, t.Market)
	if s == nil {
		return
	}
	// Window time: t's receive time, but never earlier than a Tick
	// already written. If the clock steps back (an NTP correction), late
	// Ticks fold into the window that's open rather than reopening a
	// closed one, and into today's tick file rather than yesterday's,
	// next to the bars that count them. The record keeps the raw time.
	at := max(t.Time.UnixMilli(), s.lastMs)
	s.lastMs = at

	w.appendRecord(s, kindTicks, dayOf(at), tickRecord(t))
	for k := kind1s; k < numKinds; k++ {
		w.fold(s, k, at, t)
	}
	s.state, s.known, s.changedMs = t, true, at
}

func tickRecord(t tick.Tick) TickRecord {
	r := TickRecord{T: t.Time.UnixMilli(), OK: t.OK}
	if !t.VenueTime.IsZero() {
		r.VT = t.VenueTime.UnixMilli()
	}
	if t.OK {
		r.Bid, r.Ask = &t.BidBps, &t.AskBps
		r.BidSize, r.AskSize = &t.BidSize, &t.AskSize
	}
	return r
}

// bar is one open window.
type bar struct {
	active     bool
	start, end int64
	n          int
	has        bool // a trusted price has been seen in the window
	bid, ask   [4]int
	staleMs    int64
	partial    bool
}

// fold applies t (at window time at) to s's window of kind k, closing the
// open window first if t is past it. Called before s.state is updated, so
// s.state is still the state up to t.
func (w *Writer) fold(s *stream, k kind, at int64, t tick.Tick) {
	b := &s.bars[k]
	if b.active && at >= b.end {
		w.closeBar(s, k, b.end, false)
	}
	if !b.active {
		s.openBar(k, at)
	}
	s.accrueStale(b, at)
	b.n++
	if t.OK {
		if !b.has {
			b.has = true
			b.bid = [4]int{t.BidBps, t.BidBps, t.BidBps, t.BidBps}
			b.ask = [4]int{t.AskBps, t.AskBps, t.AskBps, t.AskBps}
		} else {
			b.bid = [4]int{b.bid[0], max(b.bid[1], t.BidBps), min(b.bid[2], t.BidBps), t.BidBps}
			b.ask = [4]int{b.ask[0], max(b.ask[1], t.AskBps), min(b.ask[2], t.AskBps), t.AskBps}
		}
	}
}

// openBar opens the window of kind k containing at (or the first one
// after the last closed window, if at is earlier). The window opens on
// the state carried in from before it: if the book was trusted, that
// price is the open.
func (s *stream) openBar(k kind, at int64) {
	at = max(at, s.closedEnd[k])
	start := at - at%windowMs[k]
	b := &s.bars[k]
	*b = bar{active: true, start: start, end: start + windowMs[k], partial: !s.known}
	if s.known && s.state.OK {
		b.has = true
		b.bid = [4]int{s.state.BidBps, s.state.BidBps, s.state.BidBps, s.state.BidBps}
		b.ask = [4]int{s.state.AskBps, s.state.AskBps, s.state.AskBps, s.state.AskBps}
	}
}

// accrueStale adds the time from the last state change (or the window's
// start) up to upTo to the window's stale time, if the book was untrusted
// over it. Before the first Tick the state is unknown, so nothing is
// counted.
func (s *stream) accrueStale(b *bar, upTo int64) {
	if !s.known || s.state.OK {
		return
	}
	from := max(s.changedMs, b.start)
	if upTo > from {
		b.staleMs += upTo - from
	}
}

// closeBar ends window k at upTo, writes it, and starts tracking from its
// end. Windows are only opened by a Tick (fold), so a window with no
// Ticks never exists and is never written: the book didn't change, so
// it's the previous record's close carried forward.
func (w *Writer) closeBar(s *stream, k kind, upTo int64, partial bool) {
	b := &s.bars[k]
	s.accrueStale(b, upTo)
	r := BarRecord{
		T:       b.start,
		N:       b.n,
		StaleMs: b.staleMs,
		OK:      s.known && s.state.OK,
		Partial: b.partial || partial,
	}
	if b.has {
		bid, ask := b.bid, b.ask
		r.Bid, r.Ask = &bid, &ask
	}
	w.appendRecord(s, k, dayOf(b.start), r)
	s.closedEnd[k] = b.end
	b.active = false
}

// flush closes windows that ended at least closeGrace ago, then writes
// out and fsyncs every file written since the last flush.
func (w *Writer) flush(now time.Time) {
	nowMs := now.UnixMilli()
	for _, s := range w.streams {
		for k := kind1s; k < numKinds; k++ {
			if b := &s.bars[k]; b.active && nowMs >= b.end+closeGrace.Milliseconds() {
				w.closeBar(s, k, b.end, false)
			}
		}
		for k := range s.segs {
			w.flushSegment(&s.segs[k], true)
		}
	}
}

// close writes the open windows as partial (they're cut short at now),
// then flushes and closes every file.
func (w *Writer) close(now time.Time) {
	nowMs := now.UnixMilli()
	for _, s := range w.streams {
		for k := kind1s; k < numKinds; k++ {
			if b := &s.bars[k]; b.active {
				w.closeBar(s, k, min(max(nowMs, b.start), b.end), true)
			}
		}
		for k := range s.segs {
			w.closeSegment(&s.segs[k])
		}
	}
}

func (w *Writer) appendRecord(s *stream, k kind, day string, rec any) {
	seg := &s.segs[k]
	if seg.f == nil || seg.day != day {
		w.closeSegment(seg)
		if !w.openSegmentFor(s, k, day) {
			return
		}
	}
	line, err := json.Marshal(rec)
	if err != nil {
		w.log("storage: encoding a record for %s: %v", seg.path, err) // can't happen for these types
		return
	}
	if _, err := seg.w.Write(append(line, '\n')); err != nil {
		w.segmentErr(seg, "writing", err)
		return
	}
	seg.dirty = true
}

func (w *Writer) openSegmentFor(s *stream, k kind, day string) bool {
	seg := &s.segs[k]
	seg.day = day
	seg.path = filepath.Join(s.dir, day+"."+kindNames[k]+".jsonl")
	if err := os.MkdirAll(s.dir, 0o755); err != nil {
		w.segmentErr(seg, "creating directory for", err)
		return false
	}
	f, cut, err := openSegment(seg.path)
	if err != nil {
		w.segmentErr(seg, "opening", err)
		return false
	}
	if cut > 0 {
		w.log("storage: %s ended in a partial line (%d bytes, from a crash); cut it off", seg.path, cut)
	}
	seg.f, seg.w = f, bufio.NewWriter(f)
	return true
}

func (w *Writer) flushSegment(seg *segment, sync bool) {
	if seg.f == nil || !seg.dirty {
		return
	}
	if err := seg.w.Flush(); err != nil {
		w.segmentErr(seg, "writing", err)
		return
	}
	if sync {
		if err := seg.f.Sync(); err != nil {
			w.segmentErr(seg, "syncing", err)
			return
		}
	}
	seg.dirty, seg.bad = false, false
}

func (w *Writer) closeSegment(seg *segment) {
	if seg.f == nil {
		return
	}
	w.flushSegment(seg, true)
	if err := seg.f.Close(); err != nil {
		w.segmentErr(seg, "closing", err)
	}
	seg.f, seg.w, seg.dirty = nil, nil, false
}

// segmentErr logs a file error once until the file works again, so a
// full disk doesn't log every record.
func (w *Writer) segmentErr(seg *segment, what string, err error) {
	if !seg.bad {
		w.log("storage: %s %s: %v", what, seg.path, err)
	}
	seg.bad = true
}

func (w *Writer) log(format string, args ...any) {
	if w.logf != nil {
		w.logf(format, args...)
	}
}

// dayOf names the UTC day containing Unix ms.
func dayOf(ms int64) string {
	return time.UnixMilli(ms).UTC().Format("2006-01-02")
}

// safeComponent reports whether s can be used as one directory name:
// letters, digits, '.', '_' and '-' only, and not "." or "..". Kalshi
// tickers and Polymarket asset IDs fit; anything else in the config is a
// mistake, and must not become a path outside the data directory.
func safeComponent(s string) bool {
	if s == "" || s == "." || s == ".." {
		return false
	}
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '.', r == '_', r == '-':
		default:
			return false
		}
	}
	return true
}
