package metrics

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/rs/zerolog"
)

const (
	readHeaderTimeout = 5 * time.Second
	idleTimeout       = time.Minute
	shutdownTimeout   = 5 * time.Second
)

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
	mux := http.NewServeMux()
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, _ *http.Request) {
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

	srv := &http.Server{Handler: mux, ReadHeaderTimeout: readHeaderTimeout, IdleTimeout: idleTimeout}
	failed := make(chan error, 1)
	go func() { failed <- srv.Serve(ln) }()

	select {
	case err := <-failed:
		return fmt.Errorf("metrics: serve: %w", err)
	case <-ctx.Done():
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("metrics: shutdown: %w", err)
	}
	<-failed
	return nil
}

func loopback(addr net.Addr) bool {
	tcp, ok := addr.(*net.TCPAddr)
	return ok && tcp.IP.IsLoopback()
}
