// Package metrics defines PMDE's Prometheus metrics and serves them.
//
// The ingest metrics are labeled by venue and market (one venue client
// per market), the divergence gauges by pair. The venue clients don't
// touch the metric vectors directly: each gets a *Market with its labels
// already applied, so recording on the hot path (the websocket reader) is
// a single atomic add with no label lookup.
package metrics

import (
	"context"
	"errors"
	"net"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Resync reasons, the values of resyncs_total's reason label. A resync is
// the loss of a trusted book, which then has to be rebuilt from a fresh
// snapshot; the reason says what broke it.
const (
	ResyncSeqGap    = "seq_gap"        // Kalshi: a delta's seq didn't follow the last one
	ResyncDropped   = "dropped"        // the reader dropped a message (queue full)
	ResyncReconnect = "reconnect"      // the connection failed or was lost
	ResyncInvalid   = "invalid_update" // a message couldn't be parsed or applied
)

var resyncReasons = []string{ResyncSeqGap, ResyncDropped, ResyncReconnect, ResyncInvalid}

// Metrics holds the metric vectors, registered on one registry.
type Metrics struct {
	messages  *prometheus.CounterVec
	dropped   *prometheus.CounterVec
	reconnect *prometheus.CounterVec
	resyncs   *prometheus.CounterVec
	stale     *prometheus.GaugeVec
	lag       *prometheus.HistogramVec

	divMid *prometheus.GaugeVec
	divGap *prometheus.GaugeVec

	storageBlocked prometheus.Counter
}

// New creates the metrics and registers them on reg.
func New(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		messages: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "messages_total",
			Help: "Messages read off the venue websocket, including ones later dropped. Excludes heartbeats.",
		}, []string{"venue", "market"}),
		dropped: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "dropped_messages_total",
			Help: "Messages the websocket reader dropped because the queue to the book goroutine was full.",
		}, []string{"venue", "market"}),
		reconnect: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "reconnects_total",
			Help: "Reconnect attempts: every dial after the first, failed ones included.",
		}, []string{"venue", "market"}),
		resyncs: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "resyncs_total",
			Help: "Times a trusted book was lost and had to be rebuilt from a fresh snapshot, by what broke it.",
		}, []string{"venue", "market", "reason"}),
		stale: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "book_stale",
			Help: "1 while the venue's book is untrusted (not yet synced, stale, or reconnecting), else 0.",
		}, []string{"venue", "market"}),
		lag: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "ingest_lag_seconds",
			Help: "Receive time minus the venue's message timestamp. Includes clock skew between the venue and this host.",
			// 1ms to ~16s, doubling.
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 15),
		}, []string{"venue", "market"}),
		divMid: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "divergence_mid_bps",
			Help: "Kalshi mid minus Polymarket mid, in bps. Absent while either book has no trusted top of book.",
		}, []string{"pair"}),
		divGap: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "divergence_gap_bps",
			Help: "Executable gap in bps: max(Kalshi bid - Polymarket ask, Polymarket bid - Kalshi ask). Positive means the books cross. Absent while either book has no trusted top of book.",
		}, []string{"pair"}),
	}
	m.storageBlocked = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "storage_blocked_seconds_total",
		Help: "Time book goroutines spent blocked sending Ticks to the storage writer because its queue was full. Rising means the disk is slow and the books are being held up.",
	})
	reg.MustRegister(m.messages, m.dropped, m.reconnect, m.resyncs, m.stale, m.lag, m.divMid, m.divGap, m.storageBlocked)
	return m
}

// Market returns the metrics for one venue client's market, with every
// series it can produce created up front: counters at 0, book_stale at 1
// (nothing is trusted until the first snapshot). A series that only
// appears after its first event is hard to alert on.
func (m *Metrics) Market(venue, market string) *Market {
	v := &Market{
		messages:  m.messages.WithLabelValues(venue, market),
		dropped:   m.dropped.WithLabelValues(venue, market),
		reconnect: m.reconnect.WithLabelValues(venue, market),
		resyncs:   make(map[string]prometheus.Counter, len(resyncReasons)),
		stale:     m.stale.WithLabelValues(venue, market),
		lag:       m.lag.WithLabelValues(venue, market),
	}
	for _, r := range resyncReasons {
		v.resyncs[r] = m.resyncs.WithLabelValues(venue, market, r)
	}
	v.stale.Set(1)
	return v
}

// Market records one venue client's metrics. A nil *Market records
// nothing, so clients and tests that don't care about metrics can leave it
// unset. Safe for concurrent use.
type Market struct {
	messages  prometheus.Counter
	dropped   prometheus.Counter
	reconnect prometheus.Counter
	resyncs   map[string]prometheus.Counter // read-only after Market()
	stale     prometheus.Gauge
	lag       prometheus.Observer
}

// Message counts one message read off the websocket.
func (v *Market) Message() {
	if v != nil {
		v.messages.Inc()
	}
}

// Dropped counts one message the reader dropped.
func (v *Market) Dropped() {
	if v != nil {
		v.dropped.Inc()
	}
}

// Reconnect counts one reconnect attempt.
func (v *Market) Reconnect() {
	if v != nil {
		v.reconnect.Inc()
	}
}

// Resync counts the loss of a trusted book. reason should be one of the
// Resync constants; any other value is counted under "invalid_update"
// rather than creating an unbounded label.
func (v *Market) Resync(reason string) {
	if v == nil {
		return
	}
	c, ok := v.resyncs[reason]
	if !ok {
		c = v.resyncs[ResyncInvalid]
	}
	c.Inc()
}

// SetStale sets book_stale.
func (v *Market) SetStale(stale bool) {
	if v == nil {
		return
	}
	if stale {
		v.stale.Set(1)
	} else {
		v.stale.Set(0)
	}
}

// ObserveLag records one message's ingest lag.
func (v *Market) ObserveLag(d time.Duration) {
	if v != nil {
		v.lag.Observe(d.Seconds())
	}
}

// Pair returns the divergence gauges for one venue pair. Unlike the
// ingest series, these don't exist until there's a value: 0 would read as
// "the venues agree", which is a real (and wrong) reading when one book
// is stale.
func (m *Metrics) Pair(name string) *Pair {
	return &Pair{m: m, name: name}
}

// Pair records one venue pair's divergence. A nil *Pair records nothing.
type Pair struct {
	m    *Metrics
	name string
}

// Set records the current divergence.
func (p *Pair) Set(midBps, gapBps int) {
	if p == nil {
		return
	}
	p.m.divMid.WithLabelValues(p.name).Set(float64(midBps))
	p.m.divGap.WithLabelValues(p.name).Set(float64(gapBps))
}

// Clear removes the pair's series: there's no valid divergence while
// either book is untrusted, and a held-over value would look current.
func (p *Pair) Clear() {
	if p == nil {
		return
	}
	p.m.divMid.DeleteLabelValues(p.name)
	p.m.divGap.DeleteLabelValues(p.name)
}

// StorageBlocked adds d to storage_blocked_seconds_total. A nil *Metrics
// records nothing.
func (m *Metrics) StorageBlocked(d time.Duration) {
	if m != nil {
		m.storageBlocked.Add(d.Seconds())
	}
}

// NewRegistry returns a registry with the Go runtime and process
// collectors, for a binary to register New's metrics on.
func NewRegistry() *prometheus.Registry {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	return reg
}

// Serve exposes g on addr at /metrics until ctx is cancelled. It listens
// before returning, so a bad or busy address is reported to the caller
// at startup instead of failing quietly in the background.
func Serve(ctx context.Context, addr string, g prometheus.Gatherer, logf func(string, ...any)) (net.Addr, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(g, promhttp.HandlerOpts{}))
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}

	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logf("metrics: server stopped: %v", err)
		}
	}()
	context.AfterFunc(ctx, func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(shutdownCtx)
	})
	return ln.Addr(), nil
}
