package replication

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/rs/zerolog"
)

const (
	defaultSlotPoll = 15 * time.Second
	// slotPollTimeout bounds one check, connecting included: a connection that goes silent would
	// otherwise leave the last values standing as if they were current.
	slotPollTimeout = 10 * time.Second
)

var slotStatuses = []string{"reserved", "extended", "unreserved", "lost"}

// slotHealth is written by the watcher and read by gauges at scrape time. Byte values hold float
// bits so that "not known" can be NaN instead of a number an alert would act on.
type slotHealth struct {
	status   atomic.Pointer[string]
	retained atomic.Uint64
	lag      atomic.Uint64
	safe     atomic.Uint64
}

func newSlotHealth() *slotHealth {
	h := &slotHealth{}
	h.reset()
	return h
}

func (h *slotHealth) reset() {
	nan := math.Float64bits(math.NaN())
	h.status.Store(nil)
	h.retained.Store(nan)
	h.lag.Store(nan)
	h.safe.Store(nan)
}

func (h *slotHealth) is(status string) float64 {
	if current := h.status.Load(); current != nil && *current == status {
		return 1
	}
	return 0
}

const slotHealthSQL = `SELECT COALESCE(wal_status, ''),
	COALESCE((pg_current_wal_lsn() - restart_lsn)::text, ''),
	COALESCE((pg_current_wal_lsn() - confirmed_flush_lsn)::text, ''),
	COALESCE(safe_wal_size::text, '')
FROM pg_replication_slots WHERE slot_name = $1`

// watchSlot polls until ctx ends. It reports and never decides: a session must not end because
// its health check could not connect.
func watchSlot(ctx context.Context, databaseURL, slot string, every time.Duration, health *slotHealth, log zerolog.Logger) {
	defer health.reset()
	var conn *pgconn.PgConn
	defer func() {
		if conn != nil {
			closeCtx, cancel := context.WithTimeout(context.Background(), closeTimeout)
			defer cancel()
			_ = conn.Close(closeCtx)
		}
	}()

	ticker := time.NewTicker(every)
	defer ticker.Stop()
	failing := false
	for {
		err := func() (err error) {
			pollCtx, cancel := context.WithTimeout(ctx, slotPollTimeout)
			defer cancel()
			if conn == nil {
				if conn, err = connectSQL(pollCtx, databaseURL); err != nil {
					return err
				}
			}
			if err = pollSlot(pollCtx, conn, slot, health, log); err != nil {
				_ = conn.Close(pollCtx)
				conn = nil
			}
			return err
		}()
		switch {
		case ctx.Err() != nil:
			return
		case err != nil && !failing:
			health.reset()
			log.Warn().Err(err).Msg("slot health check failed, slot metrics are unknown until it works again")
		}
		failing = err != nil

		select {
		case <-ticker.C:
		case <-ctx.Done():
			return
		}
	}
}

func pollSlot(ctx context.Context, conn *pgconn.PgConn, slot string, health *slotHealth, log zerolog.Logger) error {
	rows, err := queryParams(ctx, conn, slotHealthSQL, slot)
	if err != nil {
		return fmt.Errorf("read slot health: %w", err)
	}
	if len(rows) != 1 {
		return fmt.Errorf("read slot health: slot %s is not there", slot)
	}
	status := string(rows[0][0])
	if previous := health.status.Swap(&status); (previous == nil || *previous != status) && (status == "unreserved" || status == "lost") {
		log.Warn().Str("slot", slot).Str("wal_status", status).Msg("slot is about to lose, or has lost, WAL it still needs")
	}
	for i, target := range []*atomic.Uint64{&health.retained, &health.lag, &health.safe} {
		value := math.NaN()
		if text := string(rows[0][i+1]); text != "" {
			if value, err = strconv.ParseFloat(text, 64); err != nil {
				return fmt.Errorf("read slot health: %w", err)
			}
		}
		target.Store(math.Float64bits(value))
	}
	return nil
}
