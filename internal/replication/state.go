package replication

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
)

const (
	stateTable = "slots"

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
