//go:build integration

package replication

import (
	"context"
	"time"
)

// EmitMarker runs the worker's marker emit as a server of the given version would, on a session
// prepared with setup.
func EmitMarker(ctx context.Context, databaseURL string, serverVersion uint64, setup string) error {
	conn, err := connectSQL(ctx, databaseURL)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(context.Background()) }()
	if _, err := queryParams(ctx, conn, setup); err != nil {
		return err
	}
	w := &backfillWorker{conn: conn, link: &backfillLink{session: "flush-test"}, serverVersion: serverVersion}
	return w.emit(ctx, marker{Kind: markerStop})
}

// EnsureBackfillTable lets a test seed progress without depending on an earlier test's backfill.
func EnsureBackfillTable(ctx context.Context, databaseURL, schema, slot string) error {
	conn, err := connectSQL(ctx, databaseURL)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(context.Background()) }()
	return ensureBackfillTable(ctx, conn, schema, slot)
}

// WithSlotPoll shortens the slot health poll so a test does not wait for the default.
func (r *Runner) WithSlotPoll(every time.Duration) *Runner {
	r.slotPoll = every
	return r
}

// SetTrackedKeysLimit lowers the key tracker's capacity until the returned function is called.
func SetTrackedKeysLimit(n int) (restore func()) {
	trackedKeysLimit = n
	return func() { trackedKeysLimit = maxTrackedKeys }
}

// SpendGeneration runs the recreate override's state write the way a starting process does.
func SpendGeneration(ctx context.Context, databaseURL, schema, slot string, generation int) error {
	conn, err := connect(ctx, databaseURL)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(context.Background()) }()
	return spendGeneration(ctx, conn, schema, slot, generation, true)
}
