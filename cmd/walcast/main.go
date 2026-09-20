package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

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

	return app.New(cfg, log, sink.NewWriter(os.Stdout)).Run(ctx)
}
