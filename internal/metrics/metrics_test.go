package metrics

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func TestWrite(t *testing.T) {
	m := New()
	m.EventsDelivered["insert"].Add(3)
	m.Delivery.Observe(20 * time.Millisecond)
	m.EndToEnd.Observe(-time.Second)
	m.Gauge("walcast_inflight_batches", func() float64 { return 7 })

	var out bytes.Buffer
	m.Write(&out)
	got := out.String()

	tests := []struct {
		name string
		want string
	}{
		{"labelled counter", `walcast_events_delivered_total{op="insert"} 3`},
		{"op series exists at zero", `walcast_events_delivered_total{op="read"} 0`},
		{"summary quantile", `walcast_sink_delivery_seconds{quantile="0.99"}`},
		{"prometheus le bucket", `walcast_sink_delivery_seconds_hist_bucket{le="0.025"} 1`},
		{"bucket below the observation stays empty", `walcast_sink_delivery_seconds_hist_bucket{le="0.01"} 0`},
		{"negative age lands in the first bucket", `walcast_end_to_end_seconds_hist_bucket{le="0.001"} 1`},
		{"negative age counts in the summary as zero", `walcast_end_to_end_seconds_sum 0`},
		{"gauge read at scrape time", `walcast_inflight_batches 7`},
		{"go runtime metrics", `go_goroutines`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if !strings.Contains(got, tt.want) {
				t.Fatalf("output lacks %q:\n%s", tt.want, got)
			}
		})
	}
	if strings.Contains(got, "vmrange") {
		t.Fatal("a vmrange histogram leaked into the output, plain Prometheus cannot read it")
	}
}

func TestSetsAreIndependent(t *testing.T) {
	a, b := New(), New()
	a.Reconnects.Inc()
	var out bytes.Buffer
	b.Write(&out)
	if !strings.Contains(out.String(), "walcast_reconnects_total 0") {
		t.Fatal("two Metrics values share state")
	}
}
