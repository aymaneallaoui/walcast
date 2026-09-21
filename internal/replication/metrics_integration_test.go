//go:build integration

package replication_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/aymaneallaoui/walcast/internal/config"
	"github.com/aymaneallaoui/walcast/internal/event"
	"github.com/aymaneallaoui/walcast/internal/metrics"
	"github.com/aymaneallaoui/walcast/internal/replication"
)

func metricValue(t *testing.T, exposition, series string) float64 {
	t.Helper()
	match := regexp.MustCompile(`(?m)^` + regexp.QuoteMeta(series) + ` (\S+)$`).FindStringSubmatch(exposition)
	if match == nil {
		t.Fatalf("series %s is missing from:\n%s", series, exposition)
	}
	value, err := strconv.ParseFloat(match[1], 64)
	if err != nil {
		t.Fatalf("series %s has value %q: %v", series, match[1], err)
	}
	return value
}

func TestMetricsCountWhatWasDelivered(t *testing.T) {
	h := newHarness(t, backfillOf(100, 1<<20))
	h.exec(fmt.Sprintf("INSERT INTO %s SELECT g, 'seed', true FROM generate_series(1, 5) g", h.table))
	m := metrics.New()

	ctx, cancel := context.WithCancel(context.Background())
	finished := make(chan error, 1)
	go func() {
		_, err := replication.NewRunner(h.cfg, h.sink, zerolog.Nop()).WithMetrics(m).Run(ctx)
		finished <- err
	}()
	t.Cleanup(cancel)
	h.waitFor("backfill to be marked done", func() bool { status, _ := h.backfillStatus(); return status == "done" })
	h.exec(fmt.Sprintf("INSERT INTO %s VALUES (10, 'a', true), (11, 'b', true)", h.table))
	h.exec(fmt.Sprintf("UPDATE %s SET name = 'c' WHERE id = 10", h.table))
	h.exec(fmt.Sprintf("DELETE FROM %s WHERE id = 11", h.table))
	h.waitFor("all 9 events", func() bool { return h.sink.count() >= 9 })

	var live bytes.Buffer
	m.Write(&live)
	for series, want := range map[string]float64{
		`walcast_events_delivered_total{op="read"}`:   5,
		`walcast_events_delivered_total{op="insert"}`: 2,
		`walcast_events_delivered_total{op="update"}`: 1,
		`walcast_events_delivered_total{op="delete"}`: 1,
		`walcast_backfill_rows_total`:                 5,
		`walcast_delivery_failures_total`:             0,
		`walcast_streaming`:                           1,
	} {
		if got := metricValue(t, live.String(), series); got != want {
			t.Errorf("%s = %v, want %v", series, got, want)
		}
	}
	if got := metricValue(t, live.String(), "walcast_end_to_end_seconds_count"); got < 1 {
		t.Errorf("end to end latency was observed %v times, want one per delivered batch that holds a commit", got)
	}
	if got := metricValue(t, live.String(), `walcast_end_to_end_seconds{quantile="0.99"}`); got <= 0 || got > 30 {
		t.Errorf("end to end p99 = %vs, want a small positive latency", got)
	}
	if delivered, received := metricValue(t, live.String(), "walcast_delivered_lsn"), metricValue(t, live.String(), "walcast_received_lsn"); delivered == 0 || received == 0 {
		t.Errorf("lsn gauges: delivered=%v received=%v, want both to have moved", delivered, received)
	}

	cancel()
	select {
	case err := <-finished:
		if err != nil {
			t.Fatalf("Run returned %v", err)
		}
	case <-time.After(waitTimeout):
		t.Fatal("Run did not stop")
	}
	var stopped bytes.Buffer
	m.Write(&stopped)
	if got := metricValue(t, stopped.String(), "walcast_streaming"); got != 0 {
		t.Errorf("walcast_streaming = %v after the session ended, want 0", got)
	}
}

func freeLoopbackAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	return ln.Addr().String()
}

func TestBinaryServesMetricsAndKeepsPprofOff(t *testing.T) {
	h := newHarness(t, nil)
	bin, addr := buildWalcast(t), freeLoopbackAddr(t)
	stderr, err := os.Create(filepath.Join(t.TempDir(), "walcast.stderr"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if out, readErr := os.ReadFile(stderr.Name()); t.Failed() && readErr == nil {
			t.Logf("walcast stderr:\n%s", out)
		}
		_ = stderr.Close()
	})
	sinkAddr := freeLoopbackAddr(t)
	srv := &http.Server{Addr: sinkAddr, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusNoContent)
	}), ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = srv.ListenAndServe() }()
	t.Cleanup(func() { _ = srv.Close() })

	h.startProcess(bin, "http://"+sinkAddr, stderr, "METRICS_ADDR="+addr)
	h.waitFor("slot to become active", h.slotActive)
	h.exec(fmt.Sprintf("INSERT INTO %s VALUES (1, 'scraped', true)", h.table))

	var body string
	h.waitFor("the insert to show up in /metrics", func() bool {
		resp, err := http.Get("http://" + addr + "/metrics")
		if err != nil {
			return false
		}
		defer func() { _ = resp.Body.Close() }()
		raw, _ := io.ReadAll(resp.Body)
		body = string(raw)
		return strings.Contains(body, `walcast_events_delivered_total{op="insert"} 1`)
	})
	for _, want := range []string{"walcast_sink_delivery_seconds_hist_bucket{le=", `walcast_sink_delivery_seconds{quantile="0.99"}`, "go_goroutines", "process_resident_memory_bytes", "walcast_slot_lag_bytes"} {
		if !strings.Contains(body, want) {
			t.Errorf("/metrics lacks %s", want)
		}
	}
	resp, err := http.Get("http://" + addr + "/debug/pprof/")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("GET /debug/pprof/ = %d without PPROF_ENABLED, want 404", resp.StatusCode)
	}
}

// gatedSink holds every batch until it is opened, so the slot cannot advance.
type gatedSink struct{ open chan struct{} }

func (g *gatedSink) Send(ctx context.Context, _ *event.Batch, done func(error)) {
	select {
	case <-g.open:
		done(nil)
	case <-ctx.Done():
		done(ctx.Err())
	}
}

func (g *gatedSink) Close() error { return nil }

func TestMetricsReportSlotHealth(t *testing.T) {
	h := newHarness(t, func(c *config.Config) { c.MetricsAddr = "127.0.0.1:0" })
	m, snk := metrics.New(), &gatedSink{open: make(chan struct{})}

	ctx, cancel := context.WithCancel(context.Background())
	finished := make(chan error, 1)
	go func() {
		_, err := replication.NewRunner(h.cfg, snk, zerolog.Nop()).WithMetrics(m).WithSlotPoll(20 * time.Millisecond).Run(ctx)
		finished <- err
	}()
	t.Cleanup(cancel)
	scrape := func() string {
		var out bytes.Buffer
		m.Write(&out)
		return out.String()
	}
	value := func(series string) float64 {
		match := regexp.MustCompile(`(?m)^` + regexp.QuoteMeta(series) + ` (\S+)$`).FindStringSubmatch(scrape())
		if match == nil {
			return -1
		}
		v, _ := strconv.ParseFloat(match[1], 64)
		return v
	}

	h.waitFor("the slot to be reported as reserved", func() bool { return value(`walcast_slot_wal_status{status="reserved"}`) == 1 })
	for _, status := range []string{"extended", "unreserved", "lost"} {
		if got := value(fmt.Sprintf(`walcast_slot_wal_status{status=%q}`, status)); got != 0 {
			t.Errorf("wal_status %s = %v, want 0", status, got)
		}
	}

	h.exec(fmt.Sprintf("INSERT INTO %s SELECT g, repeat('x', 200), true FROM generate_series(1, 2000) g", h.table))
	const held = 100_000
	h.waitFor("lag to show the WAL a stuck sink is holding", func() bool {
		return value("walcast_slot_lag_bytes") > held && value("walcast_slot_retained_wal_bytes") > held
	})
	close(snk.open)
	h.waitFor("lag to fall once the sink confirms", func() bool { return value("walcast_slot_lag_bytes") < held })

	cancel()
	select {
	case err := <-finished:
		if err != nil {
			t.Fatalf("Run returned %v", err)
		}
	case <-time.After(waitTimeout):
		t.Fatal("Run did not stop")
	}
}
