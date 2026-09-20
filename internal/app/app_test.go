package app

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/aymaneallaoui/walcast/internal/config"
	"github.com/aymaneallaoui/walcast/internal/sink"
)

func TestRunStopsOnContextCancelWhileRetrying(t *testing.T) {
	cfg := config.Config{
		DatabaseURL:       "postgres://walcast@127.0.0.1:1/walcast?connect_timeout=1",
		ShutdownTimeout:   time.Second,
		SettleTimeout:     time.Minute,
		SlotName:          "walcast_slot",
		PublicationName:   "walcast_pub",
		StateSchema:       "walcast_state",
		FeedbackInterval:  time.Second,
		ServerTimeout:     time.Minute,
		BatchMaxBytes:     1 << 16,
		BatchLinger:       time.Millisecond,
		InflightMaxBytes:  1 << 20,
		ReconnectMinDelay: 10 * time.Millisecond,
		ReconnectMaxDelay: 50 * time.Millisecond,
	}
	ctx, cancel := context.WithCancel(context.Background())
	a := New(cfg, zerolog.Nop(), sink.NewWriter(io.Discard))

	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()

	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after context cancel")
	}
}
