package metrics

import (
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Reasons a market's open was not captured, the values of
// opens_skipped_total's reason label.
const (
	SkipConcurrency = "concurrency" // the cap on concurrent subscriptions was reached
	SkipLate        = "late"        // the market had already opened when it came due
	SkipVanished    = "vanished"    // it left the unopened list before its open_time (opened early, or removed)
)

var skipReasons = []string{SkipConcurrency, SkipLate, SkipVanished}

// Opens holds the open scanner's metrics. They are not labelled by
// market or series: the scanner touches thousands of short-lived
// markets, and a series per market would never stop growing. A nil
// *Opens records nothing. Safe for concurrent use.
type Opens struct {
	skipped       *prometheus.CounterVec
	captures      prometheus.Counter
	active        prometheus.Gauge
	events        prometheus.Counter
	resyncs       *prometheus.CounterVec
	scheduled     prometheus.Gauge
	watched       prometheus.Gauge
	newSeries     prometheus.Counter
	rest          *prometheus.CounterVec
	pollErrors    *prometheus.CounterVec
	writerBlocked prometheus.Counter
}

// NewOpens creates the open scanner's metrics and registers them on reg.
func NewOpens(reg prometheus.Registerer) *Opens {
	o := &Opens{
		skipped: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "opens_skipped_total",
			Help: "Market opens that were scheduled but not captured, by why.",
		}, []string{"reason"}),
		captures: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "opens_captures_total",
			Help: "Market opens a capture was started for.",
		}),
		active: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "opens_captures_active",
			Help: "Captures running now: one websocket subscription each.",
		}),
		events: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "opens_events_total",
			Help: "Book events recorded by captures.",
		}),
		resyncs: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "opens_capture_resyncs_total",
			Help: "Times a capture's trusted book was lost mid-capture, by what broke it. The capture has a hole there.",
		}, []string{"reason"}),
		scheduled: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "opens_scheduled_markets",
			Help: "Unopened markets in watched series waiting for their open_time.",
		}),
		watched: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "opens_watched_series",
			Help: "Series whose opens are captured: the allowlist plus newly detected series.",
		}),
		newSeries: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "opens_new_series_total",
			Help: "Series seen for the first time since the first snapshot.",
		}),
		rest: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "opens_rest_responses_total",
			Help: "Kalshi REST responses by status code (\"error\" for a transport error), retried ones included.",
		}, []string{"code"}),
		pollErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "opens_poll_errors_total",
			Help: "Discovery polls that failed after retries, by what was being polled.",
		}, []string{"kind"}),
		writerBlocked: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "opens_writer_blocked_seconds_total",
			Help: "Time capture book goroutines spent blocked sending events to the opens writer because its queue was full.",
		}),
	}
	reg.MustRegister(o.skipped, o.captures, o.active, o.events, o.resyncs, o.scheduled,
		o.watched, o.newSeries, o.rest, o.pollErrors, o.writerBlocked)
	// Every series that can be alerted on exists from the start.
	for _, r := range skipReasons {
		o.skipped.WithLabelValues(r)
	}
	for _, r := range resyncReasons {
		o.resyncs.WithLabelValues(r)
	}
	for _, k := range []string{"series", "markets", "events"} {
		o.pollErrors.WithLabelValues(k)
	}
	o.rest.WithLabelValues("429")
	return o
}

// Skipped counts n opens not captured. reason is one of the Skip
// constants.
func (o *Opens) Skipped(reason string, n int) {
	if o != nil && n > 0 {
		o.skipped.WithLabelValues(reason).Add(float64(n))
	}
}

// CaptureStarted and CaptureDone bracket one capture.
func (o *Opens) CaptureStarted() {
	if o != nil {
		o.captures.Inc()
		o.active.Inc()
	}
}

func (o *Opens) CaptureDone() {
	if o != nil {
		o.active.Dec()
	}
}

// Event counts one recorded book event.
func (o *Opens) Event() {
	if o != nil {
		o.events.Inc()
	}
}

// Resync counts a capture losing its trusted book. Unknown reasons are
// counted as invalid_update, as in Market.Resync.
func (o *Opens) Resync(reason string) {
	if o == nil {
		return
	}
	known := false
	for _, r := range resyncReasons {
		known = known || r == reason
	}
	if !known {
		reason = ResyncInvalid
	}
	o.resyncs.WithLabelValues(reason).Inc()
}

// SetScheduled and SetWatched set the discovery gauges.
func (o *Opens) SetScheduled(n int) {
	if o != nil {
		o.scheduled.Set(float64(n))
	}
}

func (o *Opens) SetWatched(n int) {
	if o != nil {
		o.watched.Set(float64(n))
	}
}

// NewSeries counts one newly detected series.
func (o *Opens) NewSeries() {
	if o != nil {
		o.newSeries.Inc()
	}
}

// RESTResponse counts one REST response; status 0 is a transport error.
// It fits kalshi.REST's OnResponse.
func (o *Opens) RESTResponse(status int) {
	if o == nil {
		return
	}
	code := "error"
	if status != 0 {
		code = strconv.Itoa(status)
	}
	o.rest.WithLabelValues(code).Inc()
}

// PollError counts a failed discovery poll; kind is series, markets or
// events.
func (o *Opens) PollError(kind string) {
	if o != nil {
		o.pollErrors.WithLabelValues(kind).Inc()
	}
}

// WriterBlocked adds d to opens_writer_blocked_seconds_total.
func (o *Opens) WriterBlocked(d time.Duration) {
	if o != nil {
		o.writerBlocked.Add(d.Seconds())
	}
}
