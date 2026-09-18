package app

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/rs/zerolog"

	"github.com/aymaneallaoui/walcast/internal/config"
	"github.com/aymaneallaoui/walcast/internal/event"
	"github.com/aymaneallaoui/walcast/internal/replication"
	"github.com/aymaneallaoui/walcast/internal/sink"
)

type App struct {
	cfg  config.Config
	log  zerolog.Logger
	sink sink.Sink
}

func New(cfg config.Config, log zerolog.Logger, snk sink.Sink) *App {
	return &App{cfg: cfg, log: log, sink: snk}
}

func (a *App) Run(ctx context.Context) error {
	a.log.Info().Msg("walcast started")
	defer a.log.Info().Msg("walcast stopped")

	runner := replication.NewRunner(a.cfg, a.sink, a.log)
	var (
		confirmed pglogrepl.LSN
		attempt   int
	)
	for {
		flushed, err := runner.Run(ctx)
		progressed := flushed > confirmed
		if progressed {
			confirmed = flushed
			attempt = 0
		}

		if ctx.Err() != nil {
			if err != nil && !errors.Is(err, context.Canceled) {
				a.log.Warn().Err(err).Msg("session ended with an error during shutdown")
			}
			return a.sink.Close()
		}
		if isFatal(err) {
			return errors.Join(err, a.sink.Close())
		}

		delay := backoff(attempt, a.cfg.ReconnectMinDelay, a.cfg.ReconnectMaxDelay)
		attempt++
		a.log.Warn().Err(err).Dur("retry_in", delay).Stringer("confirmed_lsn", confirmed).Msg("replication session ended")

		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return a.sink.Close()
		}
	}
}

// isFatal reports errors that a reconnect cannot fix: the same WAL would fail the same way.
func isFatal(err error) bool {
	return errors.Is(err, replication.ErrSlotUnusable) || errors.Is(err, event.ErrUnencodable)
}
