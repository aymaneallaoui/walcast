package metrics

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

func startServer(t *testing.T, withPprof bool) (base string, m *Metrics) {
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
			base, m := startServer(t, tt.withPprof)
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
	base, _ := startServer(t, false)
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

// hold opens a connection, sends one request and leaves it to the caller to read the answer.
func hold(t *testing.T, l limits, request string) (conn net.Conn, stop func() error) {
	t.Helper()
	ln, err := Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- serve(ctx, ln, New(), true, zerolog.Nop(), l) }()

	conn, err = net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cancel(); _ = conn.Close() })
	if _, err := io.WriteString(conn, request); err != nil {
		t.Fatal(err)
	}
	return conn, func() error {
		cancel()
		select {
		case err := <-done:
			return err
		case <-time.After(5 * time.Second):
			t.Fatal("Serve did not stop after cancel")
			return nil
		}
	}
}

func closedWithin(t *testing.T, conn net.Conn, d time.Duration) bool {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(d)); err != nil {
		t.Fatal(err)
	}
	var timeout net.Error
	_, err := io.ReadAll(conn)
	return !errors.As(err, &timeout) || !timeout.Timeout()
}

func TestServeDropsARequestThatNeverSendsItsBody(t *testing.T) {
	l := defaultLimits
	l.read = 200 * time.Millisecond
	conn, _ := hold(t, l, "GET /metrics HTTP/1.1\r\nHost: walcast\r\nContent-Length: 1\r\n\r\n")
	if !closedWithin(t, conn, 3*time.Second) {
		t.Fatal("connection was still held open after the read timeout")
	}
}

func TestServeStopsWithARequestStillRunning(t *testing.T) {
	l := defaultLimits
	l.shutdown = 100 * time.Millisecond
	conn, stop := hold(t, l, "GET /debug/pprof/profile?seconds=20 HTTP/1.1\r\nHost: walcast\r\n\r\n")
	time.Sleep(100 * time.Millisecond)

	if err := stop(); err != nil {
		t.Fatalf("Serve returned %v", err)
	}
	if !closedWithin(t, conn, 3*time.Second) {
		t.Fatal("a running profile kept its connection after the server stopped")
	}
}
