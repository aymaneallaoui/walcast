package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/rs/zerolog"

	"github.com/aymaneallaoui/walcast/internal/app"
	"github.com/aymaneallaoui/walcast/internal/config"
	"github.com/aymaneallaoui/walcast/internal/logger"
	"github.com/aymaneallaoui/walcast/internal/sink"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "walcast:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	log, err := logger.New(os.Stderr, cfg.LogLevel, cfg.LogFormat)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	return app.New(cfg, log, newSink(cfg, log)).Run(ctx)
}

func newSink(cfg config.Config, log zerolog.Logger) sink.Sink {
	if cfg.Sink == config.SinkWebhook {
		return sink.NewWebhook(sink.WebhookConfig{
			URL:      cfg.WebhookURL,
			Secret:   cfg.WebhookSecret,
			Timeout:  cfg.WebhookTimeout,
			RetryMin: cfg.WebhookRetryMin,
			RetryMax: cfg.WebhookRetryMax,
		}, log)
	}
	return sink.NewWriter(os.Stdout)
}
