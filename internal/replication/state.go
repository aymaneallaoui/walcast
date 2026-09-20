package replication

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/jackc/pgx/v5/pgconn"
)

const (
	stateTable = "slots"

	sqlStateInsufficientPrivilege = "42501"
)

var (
	// ErrSlotLost means the state table remembers a slot that no longer exists, so every change
	// made since it vanished is gone. It always comes wrapped with ErrSlotUnusable.
	ErrSlotLost = errors.New("replication: slot was lost while walcast was down")

	// ErrStateStore is fatal: a missing privilege on the state schema does not heal on reconnect.
	ErrStateStore = errors.New("replication: state store is unusable")
)

type slotState struct {
	known      bool
	generation int
}

// ensureStateTable refuses a schema named after the connecting role: the default search_path starts
// with "$user", so creating it would silently capture that role's unqualified tables.
func ensureStateTable(ctx context.Context, conn *pgconn.PgConn, schema string) error {
	rows, err := query(ctx, conn, "SELECT current_user")
	if err != nil {
		return stateError("read current role", schema, err)
	}
	if len(rows) == 1 && string(rows[0][0]) == schema {
		return fmt.Errorf("%w: STATE_SCHEMA %q equals the database role, pick another name", ErrStateStore, schema)
	}

	ddl := fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %[1]s;
CREATE TABLE IF NOT EXISTS %[1]s.%[2]s (
	slot_name    text PRIMARY KEY,
	publication  text NOT NULL,
	generation   integer NOT NULL DEFAULT 0,
	created_at   timestamptz NOT NULL DEFAULT now(),
	recreated_at timestamptz
)`, schema, stateTable)
	if _, err := conn.Exec(ctx, ddl).ReadAll(); err != nil {
		return stateError("create state table", schema, err)
	}
	return nil
}

func loadSlotState(ctx context.Context, conn *pgconn.PgConn, schema, slot string) (slotState, error) {
	rows, err := query(ctx, conn, fmt.Sprintf(
		"SELECT generation FROM %s.%s WHERE slot_name = '%s'", schema, stateTable, slot))
	if err != nil {
		return slotState{}, stateError("read slot state", schema, err)
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

func recordSlot(ctx context.Context, conn *pgconn.PgConn, schema, slot, publication string) error {
	_, err := query(ctx, conn, fmt.Sprintf(
		"INSERT INTO %s.%s (slot_name, publication) VALUES ('%s', '%s') ON CONFLICT (slot_name) DO NOTHING",
		schema, stateTable, slot, publication))
	if err != nil {
		return stateError("record slot", schema, err)
	}
	return nil
}

func recordRecreation(ctx context.Context, conn *pgconn.PgConn, schema, slot string, generation int) error {
	_, err := query(ctx, conn, fmt.Sprintf(
		"UPDATE %s.%s SET generation = %d, recreated_at = now() WHERE slot_name = '%s'",
		schema, stateTable, generation, slot))
	if err != nil {
		return stateError("record slot recreation", schema, err)
	}
	return nil
}

func stateError(action, schema string, err error) error {
	if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok && pgErr.Code == sqlStateInsufficientPrivilege {
		return fmt.Errorf("%w: %s: the database role needs CREATE on the database and write access to schema %s: %w",
			ErrStateStore, action, schema, err)
	}
	return fmt.Errorf("%s: %w", action, err)
}
