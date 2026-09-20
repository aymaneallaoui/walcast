package replication

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
)

const (
	stateTable    = "slots"
	progressTable = "backfills"

	// Class 42 is "syntax error or access rule violation": missing privilege, wrong column, reserved
	// name. The same statement fails the same way on every reconnect.
	sqlClassAccessRuleViolation = "42"
)

var (
	// ErrSlotLost means the state table remembers a slot that no longer exists, so every change
	// made since it vanished is gone.
	ErrSlotLost = fmt.Errorf("%w: lost while walcast was down", ErrSlotUnusable)

	// ErrStateStore is fatal: a state table that cannot be used does not heal on reconnect.
	ErrStateStore = errors.New("replication: state store is unusable")
)

type slotState struct {
	known      bool
	generation int
}

// ensureStateTable creates the schema and table only when the table is missing. CREATE SCHEMA IF NOT
// EXISTS checks the CREATE privilege before existence, so running it blindly would demand that
// privilege on every start and rule out a table provisioned ahead of time for a low-privilege role.
func ensureStateTable(ctx context.Context, conn *pgconn.PgConn, schema string) error {
	rows, err := query(ctx, conn, fmt.Sprintf("SELECT current_user, to_regclass('%s.%s') IS NOT NULL", schema, stateTable))
	if err != nil {
		return stateError("inspect state table", err)
	}
	if len(rows) != 1 {
		return fmt.Errorf("inspect state table: got %d rows, want 1", len(rows))
	}
	if string(rows[0][1]) == "t" {
		return nil
	}
	// The default search_path starts with "$user": a schema named after the role would silently
	// become the default schema for that role's unqualified tables.
	if role := string(rows[0][0]); role == schema {
		return fmt.Errorf("%w: STATE_SCHEMA %q equals the database role, pick another name", ErrStateStore, schema)
	}

	ddl := fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %[1]s;
CREATE TABLE IF NOT EXISTS %[1]s.%[2]s (
	slot_name    text PRIMARY KEY,
	generation   integer NOT NULL DEFAULT 0,
	created_at   timestamptz NOT NULL DEFAULT now(),
	recreated_at timestamptz
)`, schema, stateTable)
	if _, err := conn.Exec(ctx, ddl).ReadAll(); err != nil {
		return stateError("create state table", err)
	}
	return nil
}

func loadSlotState(ctx context.Context, conn *pgconn.PgConn, schema, slot string) (slotState, error) {
	rows, err := query(ctx, conn, fmt.Sprintf(
		"SELECT generation FROM %s.%s WHERE slot_name = '%s'", schema, stateTable, slot))
	if err != nil {
		return slotState{}, stateError("read slot state", err)
	}
	if len(rows) == 0 {
		return slotState{}, nil
	}
	generation, err := strconv.Atoi(string(rows[0][0]))
	if err != nil {
		return slotState{}, fmt.Errorf("read slot state: generation: %w", err)
	}
	return slotState{known: true, generation: generation}, nil
}

func recordSlot(ctx context.Context, conn *pgconn.PgConn, schema, slot string) error {
	_, err := query(ctx, conn, fmt.Sprintf(
		"INSERT INTO %s.%s (slot_name) VALUES ('%s') ON CONFLICT (slot_name) DO NOTHING", schema, stateTable, slot))
	if err != nil {
		return stateError("record slot", err)
	}
	return nil
}

// spendGeneration makes a SLOT_RECREATE_GENERATION value unusable for any later start.
func spendGeneration(ctx context.Context, conn *pgconn.PgConn, schema, slot string, generation int, recreated bool) error {
	stamp := ""
	if recreated {
		stamp = ", recreated_at = now()"
	}
	_, err := query(ctx, conn, fmt.Sprintf(
		"UPDATE %s.%s SET generation = %d%s WHERE slot_name = '%s'", schema, stateTable, generation, stamp, slot))
	if err != nil {
		return stateError("record slot generation", err)
	}
	return nil
}

func stateError(action string, err error) error {
	if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok && strings.HasPrefix(pgErr.Code, sqlClassAccessRuleViolation) {
		return fmt.Errorf("%w: %s: %w", ErrStateStore, action, err)
	}
	return fmt.Errorf("%s: %w", action, err)
}

type backfillProgress struct {
	status string
	upper  []string
	last   []string
	rows   int64
}

// ensureBackfillTable follows ensureStateTable: no DDL once the table exists, so it can be
// provisioned ahead of time for a role without CREATE.
func ensureBackfillTable(ctx context.Context, conn *pgconn.PgConn, schema string) error {
	rows, err := queryParams(ctx, conn, "SELECT to_regclass($1) IS NOT NULL", schema+"."+progressTable)
	if err != nil {
		return stateError("inspect backfill table", err)
	}
	if len(rows) == 1 && string(rows[0][0]) == "t" {
		return nil
	}
	ddl := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.%s (
	table_name      text PRIMARY KEY,
	table_oid       oid NOT NULL,
	slot_generation integer NOT NULL,
	status          text NOT NULL,
	upper_key       text,
	last_key        text,
	rows_emitted    bigint NOT NULL DEFAULT 0,
	started_at      timestamptz NOT NULL DEFAULT now(),
	finished_at     timestamptz
)`, schema, progressTable)
	if _, err := conn.Exec(ctx, ddl).ReadAll(); err != nil {
		return stateError("create backfill table", err)
	}
	return nil
}

// loadBackfill starts over when the saved progress describes another table incarnation or another
// slot generation: a recreated table has new rows, and an accepted slot gap has to be repaired.
func loadBackfill(ctx context.Context, conn *pgconn.PgConn, schema string, table backfillTable, slotGeneration uint64) (backfillProgress, error) {
	name, oid, generation := table.qualified(), strconv.FormatUint(uint64(table.oid), 10), strconv.FormatUint(slotGeneration, 10)
	_, err := queryParams(ctx, conn, fmt.Sprintf(`
INSERT INTO %[1]s.%[2]s AS b (table_name, table_oid, slot_generation, status) VALUES ($1, $2::oid, $3::int, $4)
ON CONFLICT (table_name) DO UPDATE
SET table_oid = EXCLUDED.table_oid, slot_generation = EXCLUDED.slot_generation, status = EXCLUDED.status,
    upper_key = NULL, last_key = NULL, rows_emitted = 0, started_at = now(), finished_at = NULL
WHERE b.table_oid <> EXCLUDED.table_oid OR b.slot_generation <> EXCLUDED.slot_generation`, schema, progressTable),
		name, oid, generation, backfillRunning)
	if err != nil {
		return backfillProgress{}, stateError("register backfill", err)
	}

	rows, err := queryParams(ctx, conn, fmt.Sprintf(
		"SELECT status, upper_key, last_key, rows_emitted::text FROM %s.%s WHERE table_name = $1", schema, progressTable), name)
	if err != nil {
		return backfillProgress{}, stateError("read backfill progress", err)
	}
	if len(rows) != 1 {
		return backfillProgress{}, fmt.Errorf("read backfill progress: got %d rows for %s", len(rows), name)
	}
	progress := backfillProgress{status: string(rows[0][0])}
	if progress.upper, err = decodeKey(rows[0][1]); err != nil {
		return backfillProgress{}, fmt.Errorf("read backfill progress: upper key: %w", err)
	}
	if progress.last, err = decodeKey(rows[0][2]); err != nil {
		return backfillProgress{}, fmt.Errorf("read backfill progress: last key: %w", err)
	}
	if progress.rows, err = strconv.ParseInt(string(rows[0][3]), 10, 64); err != nil {
		return backfillProgress{}, fmt.Errorf("read backfill progress: rows: %w", err)
	}
	return progress, nil
}

func saveBackfillUpper(ctx context.Context, conn *pgconn.PgConn, schema string, table backfillTable, upper []string) error {
	encoded, err := json.Marshal(upper)
	if err != nil {
		return fmt.Errorf("encode upper key: %w", err)
	}
	_, err = queryParams(ctx, conn, fmt.Sprintf("UPDATE %s.%s SET upper_key = $2 WHERE table_name = $1", schema, progressTable),
		table.qualified(), string(encoded))
	if err != nil {
		return stateError("save backfill upper key", err)
	}
	return nil
}

func saveBackfillProgress(ctx context.Context, conn *pgconn.PgConn, schema string, table backfillTable, progress backfillProgress) error {
	encoded, err := json.Marshal(progress.last)
	if err != nil {
		return fmt.Errorf("encode last key: %w", err)
	}
	_, err = queryParams(ctx, conn, fmt.Sprintf(
		"UPDATE %s.%s SET last_key = $2, rows_emitted = $3::bigint WHERE table_name = $1", schema, progressTable),
		table.qualified(), string(encoded), strconv.FormatInt(progress.rows, 10))
	if err != nil {
		return stateError("save backfill progress", err)
	}
	return nil
}

func finishBackfill(ctx context.Context, conn *pgconn.PgConn, schema string, table backfillTable) error {
	_, err := queryParams(ctx, conn, fmt.Sprintf(
		"UPDATE %s.%s SET status = $2, finished_at = now() WHERE table_name = $1", schema, progressTable),
		table.qualified(), backfillDone)
	if err != nil {
		return stateError("finish backfill", err)
	}
	return nil
}

func decodeKey(raw []byte) ([]string, error) {
	if raw == nil {
		return nil, nil
	}
	var key []string
	if err := json.Unmarshal(raw, &key); err != nil {
		return nil, err
	}
	return key, nil
}
