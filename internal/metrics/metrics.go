// Package metrics defines PMDE's Prometheus metrics and serves them.
//
// Every metric is labeled by venue. The venue clients don't touch the
// metric vectors directly: each gets a *Venue with the venue label
// already applied, so recording on the hot path (the websocket reader)
// is a single atomic add with no label lookup.
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
}

// New creates the metrics and registers them on reg.
func New(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		messages: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "messages_total",
			Help: "Messages read off the venue websocket, including ones later dropped. Excludes heartbeats.",
		}, []string{"venue"}),
		dropped: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "dropped_messages_total",
			Help: "Messages the websocket reader dropped because the queue to the book goroutine was full.",
		}, []string{"venue"}),
		reconnect: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "reconnects_total",
			Help: "Reconnect attempts: every dial after the first, failed ones included.",
		}, []string{"venue"}),
		resyncs: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "resyncs_total",
			Help: "Times a trusted book was lost and had to be rebuilt from a fresh snapshot, by what broke it.",
		}, []string{"venue", "reason"}),
		stale: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "book_stale",
			Help: "1 while the venue's book is untrusted (not yet synced, stale, or reconnecting), else 0.",
		}, []string{"venue"}),
		lag: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "ingest_lag_seconds",
			Help: "Receive time minus the venue's message timestamp. Includes clock skew between the venue and this host.",
			// 1ms to ~16s, doubling.
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 15),
		}, []string{"venue"}),
	}
	reg.MustRegister(m.messages, m.dropped, m.reconnect, m.resyncs, m.stale, m.lag)
	return m
}

// Venue returns the metrics for one venue, with every series it can
// produce created up front: counters at 0, book_stale at 1 (nothing is
// trusted until the first snapshot). A series that only appears after
// its first event is hard to alert on.
func (m *Metrics) Venue(name string) *Venue {
	v := &Venue{
		messages:  m.messages.WithLabelValues(name),
		dropped:   m.dropped.WithLabelValues(name),
		reconnect: m.reconnect.WithLabelValues(name),
		resyncs:   make(map[string]prometheus.Counter, len(resyncReasons)),
		stale:     m.stale.WithLabelValues(name),
		lag:       m.lag.WithLabelValues(name),
	}
	for _, r := range resyncReasons {
		v.resyncs[r] = m.resyncs.WithLabelValues(name, r)
	}
	v.stale.Set(1)
	return v
}

// Venue records one venue's metrics. A nil *Venue records nothing, so
// clients and tests that don't care about metrics can leave it unset.
// Safe for concurrent use.
type Venue struct {
	messages  prometheus.Counter
	dropped   prometheus.Counter
	reconnect prometheus.Counter
	resyncs   map[string]prometheus.Counter // read-only after Venue()
	stale     prometheus.Gauge
	lag       prometheus.Observer
}

// Message counts one message read off the websocket.
func (v *Venue) Message() {
	if v != nil {
		v.messages.Inc()
	}
}

// Dropped counts one message the reader dropped.
func (v *Venue) Dropped() {
	if v != nil {
		v.dropped.Inc()
	}
}

// Reconnect counts one reconnect attempt.
func (v *Venue) Reconnect() {
	if v != nil {
		v.reconnect.Inc()
	}
}

// Resync counts the loss of a trusted book. reason should be one of the
// Resync constants; any other value is counted under "invalid_update"
// rather than creating an unbounded label.
func (v *Venue) Resync(reason string) {
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
func (v *Venue) SetStale(stale bool) {
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
func (v *Venue) ObserveLag(d time.Duration) {
	if v != nil {
		v.lag.Observe(d.Seconds())
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
