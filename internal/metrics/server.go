package metrics

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/rs/zerolog"
)

type limits struct {
	readHeader, read, write, idle, shutdown time.Duration
}

// The write limit covers a scrape only: a CPU profile or a trace writes for as long as it was asked to.
var defaultLimits = limits{
	readHeader: 5 * time.Second,
	read:       10 * time.Second,
	write:      10 * time.Second,
	idle:       time.Minute,
	shutdown:   5 * time.Second,
}

// Listen binds before anything else starts, so a bad or busy address stops the process at startup
// instead of leaving it running without the metrics its operator asked for.
func Listen(addr string) (net.Listener, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("metrics: listen on %s: %w", addr, err)
	}
	return ln, nil
}

// Serve runs until ctx is cancelled. It uses its own mux: importing net/http/pprof registers the
// profiling handlers on http.DefaultServeMux as a side effect, and that mux is never served here.
func Serve(ctx context.Context, ln net.Listener, m *Metrics, withPprof bool, log zerolog.Logger) error {
	return serve(ctx, ln, m, withPprof, log, defaultLimits)
}

func serve(ctx context.Context, ln net.Listener, m *Metrics, withPprof bool, log zerolog.Logger, l limits) error {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, _ *http.Request) {
		_ = http.NewResponseController(w).SetWriteDeadline(time.Now().Add(l.write))
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		m.Write(w)
	})
	if withPprof {
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
		if !loopback(ln.Addr()) {
			log.Warn().Stringer("addr", ln.Addr()).Msg("pprof is reachable from the network: it exposes process internals and can be used to load the process")
		}
	}
	log.Info().Stringer("addr", ln.Addr()).Bool("pprof", withPprof).Msg("metrics listening")

	srv := &http.Server{Handler: mux, ReadHeaderTimeout: l.readHeader, ReadTimeout: l.read, IdleTimeout: l.idle}
	failed := make(chan error, 1)
	go func() { failed <- srv.Serve(ln) }()

	select {
	case err := <-failed:
		return fmt.Errorf("metrics: serve: %w", err)
	case <-ctx.Done():
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), l.shutdown)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		// A connection that would not finish in time is cut, the process is stopping anyway.
		_ = srv.Close()
	}
	<-failed
	return nil
}

func loopback(addr net.Addr) bool {
	tcp, ok := addr.(*net.TCPAddr)
	return ok && tcp.IP.IsLoopback()
}
