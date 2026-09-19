package app

import (
	"context"

	"github.com/rs/zerolog"

	"github.com/aymaneallaoui/walcast/internal/config"
)

type App struct {
	cfg config.Config
	log zerolog.Logger
}

func New(cfg config.Config, log zerolog.Logger) *App {
	return &App{cfg: cfg, log: log}
}

func (a *App) Run(ctx context.Context) error {
	a.log.Info().Msg("walcast started")

	<-ctx.Done()
	a.log.Info().Msg("shutdown signal received")

	drainCtx, cancel := context.WithTimeout(context.Background(), a.cfg.ShutdownTimeout)
	defer cancel()

	if err := a.drain(drainCtx); err != nil {
		return err
	}
	a.log.Info().Msg("walcast stopped")
	return nil
}

func (a *App) drain(_ context.Context) error {
	return nil
}
