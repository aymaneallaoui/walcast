package metrics

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

func serve(t *testing.T, withPprof bool) (base string, m *Metrics) {
	t.Helper()
	ln, err := Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	m = New()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Serve(ctx, ln, m, withPprof, zerolog.Nop()) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("Serve returned %v on shutdown", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("Serve did not stop after cancel")
		}
	})
	return "http://" + ln.Addr().String(), m
}

func get(t *testing.T, url string) (int, string) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, string(body)
}

func TestServe(t *testing.T) {
	tests := []struct {
		name      string
		withPprof bool
		path      string
		want      int
	}{
		{"metrics", false, "/metrics", http.StatusOK},
		{"pprof is off unless asked for", false, "/debug/pprof/", http.StatusNotFound},
		{"pprof profile is off unless asked for", false, "/debug/pprof/heap", http.StatusNotFound},
		{"pprof when enabled", true, "/debug/pprof/", http.StatusOK},
		{"heap profile when enabled", true, "/debug/pprof/heap", http.StatusOK},
		{"nothing else is served", true, "/", http.StatusNotFound},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			base, m := serve(t, tt.withPprof)
			m.Reconnects.Inc()
			status, body := get(t, base+tt.path)
			if status != tt.want {
				t.Fatalf("GET %s = %d, want %d", tt.path, status, tt.want)
			}
			if tt.path == "/metrics" && !strings.Contains(body, "walcast_reconnects_total 1") {
				t.Fatalf("metrics body lacks the counter:\n%s", body)
			}
		})
	}
}

func TestMetricsRejectsWrites(t *testing.T) {
	base, _ := serve(t, false)
	resp, err := http.Post(base+"/metrics", "text/plain", strings.NewReader("x"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("POST /metrics = %d, want 405", resp.StatusCode)
	}
}

func TestListenReportsABusyAddress(t *testing.T) {
	ln, err := Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	if _, err := Listen(ln.Addr().String()); err == nil {
		t.Fatal("binding a busy address must fail at startup")
	}
}
