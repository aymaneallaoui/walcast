package replication

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/rs/zerolog"

	"github.com/aymaneallaoui/walcast/internal/config"
)

const (
	outputPlugin = "pgoutput"

	sqlStateObjectNotInPrerequisiteState = "55000"
)

// ErrSlotUnusable means streaming cannot continue without losing changes, so retrying is pointless.
var ErrSlotUnusable = errors.New("replication: slot is unusable")

func connect(ctx context.Context, databaseURL string) (*pgconn.PgConn, error) {
	cfg, err := pgconn.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse database url: %w", err)
	}
	cfg.RuntimeParams["replication"] = "database"
	cfg.RuntimeParams["client_encoding"] = "UTF8"
	cfg.RuntimeParams["application_name"] = "walcast"

	conn, err := pgconn.ConnectConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	return conn, nil
}

func ensurePublication(ctx context.Context, conn *pgconn.PgConn, log zerolog.Logger, name string, tables []string) error {
	rows, err := query(ctx, conn, fmt.Sprintf("SELECT puballtables FROM pg_publication WHERE pubname = '%s'", name))
	if err != nil {
		return fmt.Errorf("check publication: %w", err)
	}
	if len(rows) > 0 {
		return warnOnPublicationDrift(ctx, conn, log, name, tables, string(rows[0][0]) == "t")
	}

	target := "ALL TABLES"
	if len(tables) > 0 {
		target = "TABLE " + strings.Join(tables, ", ")
	}
	if _, err := query(ctx, conn, fmt.Sprintf("CREATE PUBLICATION %s FOR %s", name, target)); err != nil {
		return fmt.Errorf("create publication: %w", err)
	}
	log.Info().Str("publication", name).Msg("publication created")
	return nil
}

func warnOnPublicationDrift(ctx context.Context, conn *pgconn.PgConn, log zerolog.Logger, name string, tables []string, allTables bool) error {
	if allTables && len(tables) == 0 {
		return nil
	}

	rows, err := query(ctx, conn, fmt.Sprintf(
		"SELECT schemaname || '.' || tablename FROM pg_publication_tables WHERE pubname = '%s'", name))
	if err != nil {
		return fmt.Errorf("list publication tables: %w", err)
	}
	published := make([]string, len(rows))
	for i, row := range rows {
		published[i] = string(row[0])
	}
	slices.Sort(published)

	if want := qualified(tables); publicationDrifted(allTables, want, published) {
		log.Warn().Str("publication", name).Strs("configured", want).Strs("published", published).Bool("all_tables", allTables).
			Msg("existing publication differs from PUBLICATION_TABLES and is left untouched")
	}
	return nil
}

// publicationDrifted compares scope before contents: an all-tables config against a table-scoped
// publication is drift even when that publication is empty and both lists compare equal.
func publicationDrifted(publishesAll bool, want, published []string) bool {
	wantsAll := len(want) == 0
	if publishesAll || wantsAll {
		return publishesAll != wantsAll
	}
	return !slices.Equal(want, published)
}

func qualified(tables []string) []string {
	out := make([]string, len(tables))
	for i, t := range tables {
		if !strings.Contains(t, ".") {
			t = "public." + t
		}
		out[i] = t
	}
	slices.Sort(out)
	return out
}

type slotAction int

const (
	slotKeep slotAction = iota
	slotAdopt
	slotCreate
	slotRecreate
	slotRefuse
)

// decideSlot is the whole policy for a process start. A slot that the state table remembers but
// Postgres no longer has is recreated only when the operator names the next generation exactly.
func decideSlot(exists bool, state slotState, requested int) slotAction {
	switch {
	case exists && state.known:
		return slotKeep
	case exists:
		return slotAdopt
	case !state.known:
		return slotCreate
	case requested == state.generation+1:
		return slotRecreate
	default:
		return slotRefuse
	}
}

// bootstrapSlot runs once per process. The state table outlives the slot, so a slot that vanished
// while walcast was down is refused instead of being recreated past every change made since.
func bootstrapSlot(ctx context.Context, conn *pgconn.PgConn, log zerolog.Logger, cfg config.Config) error {
	if err := ensureStateTable(ctx, conn, cfg.StateSchema); err != nil {
		return err
	}
	state, err := loadSlotState(ctx, conn, cfg.StateSchema, cfg.SlotName)
	if err != nil {
		return err
	}
	exists, err := checkSlot(ctx, conn, cfg.SlotName)
	if err != nil {
		return err
	}

	switch decideSlot(exists, state, cfg.SlotRecreateGeneration) {
	case slotKeep:
	case slotAdopt:
		err = recordSlot(ctx, conn, cfg.StateSchema, cfg.SlotName)
	case slotCreate:
		// Slot first: a crash before the row is written leaves a slot to adopt, never a false loss.
		if err = createSlot(ctx, conn, log, cfg.SlotName); err == nil {
			err = recordSlot(ctx, conn, cfg.StateSchema, cfg.SlotName)
		}
	case slotRecreate:
		// Generation first: a crash before the slot exists then refuses this same value, where the
		// other order would let it recreate the slot a second time.
		if err = spendGeneration(ctx, conn, cfg.StateSchema, cfg.SlotName, cfg.SlotRecreateGeneration, true); err == nil {
			err = createSlot(ctx, conn, log, cfg.SlotName)
		}
		if err == nil {
			log.Warn().Str("slot", cfg.SlotName).Int("generation", cfg.SlotRecreateGeneration).
				Msg("lost replication slot recreated on request, changes made while it was gone are skipped")
		}
		return err
	case slotRefuse:
		return fmt.Errorf("%w: %s is gone and the changes made since are not recoverable; set SLOT_RECREATE_GENERATION=%d to accept the gap and recreate it",
			ErrSlotLost, cfg.SlotName, state.generation+1)
	}
	if err != nil {
		return err
	}

	// An override that found nothing to recreate is spent anyway, or it would stay armed and let a
	// later loss through without anyone deciding to accept it.
	if cfg.SlotRecreateGeneration == state.generation+1 {
		log.Warn().Str("slot", cfg.SlotName).Int("generation", cfg.SlotRecreateGeneration).
			Msg("SLOT_RECREATE_GENERATION is set but the slot was not lost, the value is now used up")
		return spendGeneration(ctx, conn, cfg.StateSchema, cfg.SlotName, cfg.SlotRecreateGeneration, false)
	}
	return nil
}

// requireSlot guards reconnects: a slot that vanishes between sessions is never recreated.
func requireSlot(ctx context.Context, conn *pgconn.PgConn, name string) error {
	exists, err := checkSlot(ctx, conn, name)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("%w: %s disappeared while walcast was running", ErrSlotUnusable, name)
	}
	return nil
}

func checkSlot(ctx context.Context, conn *pgconn.PgConn, name string) (bool, error) {
	rows, err := query(ctx, conn, fmt.Sprintf(
		"SELECT plugin, COALESCE(wal_status, '') FROM pg_replication_slots WHERE slot_name = '%s'", name))
	if err != nil {
		return false, fmt.Errorf("check slot: %w", err)
	}
	if len(rows) == 0 {
		return false, nil
	}
	plugin, walStatus := string(rows[0][0]), string(rows[0][1])
	if plugin != outputPlugin {
		return false, fmt.Errorf("%w: %s uses plugin %q, want %s", ErrSlotUnusable, name, plugin, outputPlugin)
	}
	if walStatus == "lost" {
		return false, fmt.Errorf("%w: %s was invalidated, its WAL is gone", ErrSlotUnusable, name)
	}
	return true, nil
}

func createSlot(ctx context.Context, conn *pgconn.PgConn, log zerolog.Logger, name string) error {
	_, err := pglogrepl.CreateReplicationSlot(ctx, conn, name, outputPlugin, pglogrepl.CreateReplicationSlotOptions{
		Mode:           pglogrepl.LogicalReplication,
		SnapshotAction: "NOEXPORT_SNAPSHOT",
	})
	if err != nil {
		return fmt.Errorf("create slot: %w", err)
	}
	log.Info().Str("slot", name).Msg("replication slot created")
	return nil
}

func startReplication(ctx context.Context, conn *pgconn.PgConn, slot, publication string) error {
	err := pglogrepl.StartReplication(ctx, conn, slot, 0, pglogrepl.StartReplicationOptions{
		PluginArgs: []string{
			"proto_version '1'",
			fmt.Sprintf("publication_names '%s'", publication),
		},
	})
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == sqlStateObjectNotInPrerequisiteState {
		return fmt.Errorf("%w: %s", ErrSlotUnusable, pgErr.Message)
	}
	if err != nil {
		return fmt.Errorf("start replication: %w", err)
	}
	return nil
}

func query(ctx context.Context, conn *pgconn.PgConn, sql string) ([][][]byte, error) {
	results, err := conn.Exec(ctx, sql).ReadAll()
	if err != nil {
		return nil, err
	}
	if len(results) == 0 {
		return nil, nil
	}
	return results[0].Rows, nil
}
