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

// ensureSlot creates the slot only when allowCreate is set. A slot that vanishes between sessions
// must not be recreated silently: the new one would start past every change made in between.
func ensureSlot(ctx context.Context, conn *pgconn.PgConn, log zerolog.Logger, name string, allowCreate bool) error {
	rows, err := query(ctx, conn, fmt.Sprintf(
		"SELECT plugin, COALESCE(wal_status, '') FROM pg_replication_slots WHERE slot_name = '%s'", name))
	if err != nil {
		return fmt.Errorf("check slot: %w", err)
	}
	if len(rows) > 0 {
		plugin, walStatus := string(rows[0][0]), string(rows[0][1])
		if plugin != outputPlugin {
			return fmt.Errorf("%w: %s uses plugin %q, want %s", ErrSlotUnusable, name, plugin, outputPlugin)
		}
		if walStatus == "lost" {
			return fmt.Errorf("%w: %s was invalidated, its WAL is gone", ErrSlotUnusable, name)
		}
		return nil
	}
	if !allowCreate {
		return fmt.Errorf("%w: %s disappeared while walcast was running", ErrSlotUnusable, name)
	}

	_, err = pglogrepl.CreateReplicationSlot(ctx, conn, name, outputPlugin, pglogrepl.CreateReplicationSlotOptions{
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
