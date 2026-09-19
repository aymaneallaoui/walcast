package app_test

import (
	"context"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/aymaneallaoui/walcast/internal/app"
	"github.com/aymaneallaoui/walcast/internal/config"
)

func TestRunStopsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	a := app.New(config.Config{ShutdownTimeout: time.Second}, zerolog.Nop())

	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()

	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after context cancel")
	}
}
