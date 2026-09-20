// Package metrics holds every metric walcast exposes. All of it lives in one private set, so two
// instances in one process, as in tests, never share or collide on a global registry.
package metrics

import (
	"fmt"
	"io"
	"time"

	vm "github.com/VictoriaMetrics/metrics"
)

// Latency buckets reach further than the library defaults: a webhook that retries in place can
// take minutes to deliver one batch, and that tail is what an operator needs to see.
var latencyBuckets = []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 300}

// latency is exposed twice: a summary whose quantiles read directly, and a histogram with
// Prometheus le buckets that can be aggregated across instances.
type latency struct {
	summary   *vm.Summary
	histogram *vm.PrometheusHistogram
}

func (l latency) Observe(d time.Duration) {
	seconds := d.Seconds()
	l.summary.Update(seconds)
	l.histogram.Update(seconds)
}

type Metrics struct {
	set *vm.Set

	EventsDelivered  map[string]*vm.Counter
	BatchesDelivered *vm.Counter
	BytesDelivered   *vm.Counter
	DeliveryFailures *vm.Counter
	Reconnects       *vm.Counter
	SinkRetries      *vm.Counter

	BackfillRows    *vm.Counter
	BackfillChunks  *vm.Counter
	BackfillDropped *vm.Counter
	BackfillRetried *vm.Counter

	Delivery latency
	EndToEnd latency
}

// Ops lists every event operation up front, so each series exists at zero from the first scrape
// and a rate over it never starts from a gap.
var Ops = []string{"insert", "update", "delete", "truncate", "read"}

func New() *Metrics {
	set := vm.NewSet()
	m := &Metrics{
		set:              set,
		EventsDelivered:  make(map[string]*vm.Counter, len(Ops)),
		BatchesDelivered: set.NewCounter("walcast_batches_delivered_total"),
		BytesDelivered:   set.NewCounter("walcast_bytes_delivered_total"),
		DeliveryFailures: set.NewCounter("walcast_delivery_failures_total"),
		Reconnects:       set.NewCounter("walcast_reconnects_total"),
		SinkRetries:      set.NewCounter("walcast_sink_retries_total"),
		BackfillRows:     set.NewCounter("walcast_backfill_rows_total"),
		BackfillChunks:   set.NewCounter("walcast_backfill_chunks_total"),
		BackfillDropped:  set.NewCounter("walcast_backfill_rows_superseded_total"),
		BackfillRetried:  set.NewCounter("walcast_backfill_rows_reread_total"),
		Delivery:         newLatency(set, "walcast_sink_delivery_seconds"),
		EndToEnd:         newLatency(set, "walcast_end_to_end_seconds"),
	}
	for _, op := range Ops {
		m.EventsDelivered[op] = set.NewCounter(fmt.Sprintf(`walcast_events_delivered_total{op=%q}`, op))
	}
	return m
}

func newLatency(set *vm.Set, name string) latency {
	return latency{
		summary:   set.NewSummaryExt(name, 5*time.Minute, []float64{0.5, 0.9, 0.99}),
		histogram: set.NewPrometheusHistogramExt(name+"_hist", latencyBuckets),
	}
}

// Gauge registers a value that is read at scrape time, so the hot path never pays for it.
func (m *Metrics) Gauge(name string, read func() float64) {
	m.set.GetOrCreateGauge(name, read)
}

// Write renders walcast's own metrics followed by the Go runtime and process metrics.
func (m *Metrics) Write(w io.Writer) {
	m.set.WritePrometheus(w)
	vm.WriteProcessMetrics(w)
}
