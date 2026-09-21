//go:build integration

package replication_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/aymaneallaoui/walcast/internal/config"
	"github.com/aymaneallaoui/walcast/internal/replication"
)

func backfillOf(rows, bytes int) func(*config.Config) {
	return func(c *config.Config) {
		c.BackfillTables = []string{"public." + c.PublicationTables[0][len("public."):]}
		c.BackfillChunkRows, c.BackfillChunkBytes = rows, bytes
	}
}

// backfillStatus tolerates a missing table: walcast creates it on the first backfill, and a test
// that polls right after starting a session must not depend on an earlier test having done so.
func (h *harness) backfillStatus() (status string, rows int) {
	h.t.Helper()
	if exists := h.exec(fmt.Sprintf("SELECT to_regclass('%s.backfills') IS NOT NULL", h.cfg.StateSchema)); string(exists[0][0]) != "t" {
		return "", 0
	}
	got := h.exec(fmt.Sprintf("SELECT status, rows_emitted FROM %s.backfills WHERE table_name = 'public.%s' AND slot_name = '%s'", h.cfg.StateSchema, h.table, h.cfg.SlotName))
	if len(got) != 1 {
		return "", 0
	}
	rows, _ = strconv.Atoi(string(got[0][1]))
	return string(got[0][0]), rows
}

func (h *harness) reads() int {
	events, _ := h.sink.snapshot()
	n := 0
	for _, ev := range events {
		if ev["op"] == "read" {
			n++
		}
	}
	return n
}

// consumer replays events the way a downstream table would: reads and inserts upsert, an update
// patches what it carries and keeps what it lists as unchanged, a delete removes.
type consumer struct {
	rows  map[float64]map[string]any
	moved map[string]any
}

func (c *consumer) apply(ev map[string]any) {
	row, _ := ev["new"].(map[string]any)
	switch ev["op"] {
	case "read":
		c.rows[row["id"].(float64)] = row
	case "insert":
		if ev["origin"] == "update" && c.moved != nil {
			for column, value := range c.moved {
				if _, carried := row[column]; !carried {
					row[column] = value
				}
			}
		}
		c.rows[row["id"].(float64)] = row
	case "update":
		id := row["id"].(float64)
		if c.rows[id] == nil {
			c.rows[id] = map[string]any{}
		}
		for column, value := range row {
			c.rows[id][column] = value
		}
	case "delete":
		id := ev["old"].(map[string]any)["id"].(float64)
		c.moved = nil
		if ev["origin"] == "update" {
			c.moved = c.rows[id]
		}
		delete(c.rows, id)
	case "truncate":
		c.rows = map[float64]map[string]any{}
	}
}

func replay(events []map[string]any) map[float64]map[string]any {
	c := &consumer{rows: map[float64]map[string]any{}}
	for _, ev := range events {
		c.apply(ev)
	}
	return c.rows
}

func (h *harness) tableState() map[float64]map[string]any {
	h.t.Helper()
	state := map[float64]map[string]any{}
	for _, row := range h.exec(fmt.Sprintf("SELECT id, name, active, bio FROM %s", h.table)) {
		id, _ := strconv.ParseFloat(string(row[0]), 64)
		state[id] = map[string]any{"id": id, "name": text(row[1]), "active": string(row[2]) == "t", "bio": text(row[3])}
	}
	return state
}

func text(v []byte) any {
	if v == nil {
		return nil
	}
	return string(v)
}

func (h *harness) addToastColumn() {
	h.t.Helper()
	h.exec(fmt.Sprintf("ALTER TABLE %s ADD COLUMN bio text", h.table))
	h.exec(fmt.Sprintf("ALTER TABLE %s ALTER COLUMN bio SET STORAGE EXTERNAL", h.table))
}

func (h *harness) seed(n int) {
	h.t.Helper()
	h.exec(fmt.Sprintf("INSERT INTO %s SELECT g, 'seed ' || g, true, repeat(md5(g::text), 100) FROM generate_series(1, %d) g", h.table, n))
}

func TestBackfillDeliversExistingRowsOnceThenStreams(t *testing.T) {
	h := newHarness(t, backfillOf(100, 1<<20))
	h.addToastColumn()
	h.seed(450)

	stop := h.startSession()
	h.waitFor("450 read events", func() bool { return h.reads() >= 450 })
	h.waitFor("backfill to be marked done", func() bool { status, _ := h.backfillStatus(); return status == "done" })
	h.exec(fmt.Sprintf("INSERT INTO %s VALUES (1000, 'live', true, NULL)", h.table))
	h.waitFor("the live insert", func() bool { return h.sink.count() >= 451 })
	stop()

	if _, rows := h.backfillStatus(); rows != 450 {
		t.Fatalf("rows_emitted = %d, want 450", rows)
	}
	events, _ := h.sink.snapshot()
	for _, ev := range events[:450] {
		if ev["op"] != "read" || ev["commit_lsn"] != nil || ev["backfill"] == nil {
			t.Fatalf("backfilled row is not a read with its own id: %v", ev)
		}
	}
	if got, want := replay(events), h.tableState(); !reflect.DeepEqual(got, want) {
		t.Fatalf("consumer holds %d rows, table has %d, or their contents differ", len(got), len(want))
	}

	stop = h.startSession()
	h.exec(fmt.Sprintf("INSERT INTO %s VALUES (1001, 'after restart', true, NULL)", h.table))
	h.waitFor("the insert after restart", func() bool { return h.sink.count() >= 452 })
	stop()
	if got := h.reads(); got != 450 {
		t.Fatalf("a finished backfill ran again after a restart: %d reads", got)
	}
}

// Random writes race the backfill: patches that leave the TOAST column out, full rewrites,
// deletes, inserts and key changes. Whatever the interleaving, a consumer that applies the events
// in order must end up with exactly the table, including every TOAST value it was never sent twice.
func TestBackfillConvergesUnderConcurrentWrites(t *testing.T) {
	const seeded, writes = 1500, 1200
	h := newHarness(t, backfillOf(40, 1<<20))
	h.addToastColumn()
	h.seed(seeded)

	writer, err := pgconn.Connect(context.Background(), h.cfg.DatabaseURL)
	if err != nil {
		t.Fatalf("connect writer: %v", err)
	}
	t.Cleanup(func() { _ = writer.Close(context.Background()) })

	stop := h.startSession()
	rng := rand.New(rand.NewPCG(7, 11))
	nextID := seeded + 1
	for i := range writes {
		id := rng.IntN(seeded) + 1
		var sql string
		switch rng.IntN(10) {
		case 0, 1, 2, 3:
			sql = fmt.Sprintf("UPDATE %s SET name = 'patch %d' WHERE id = %d", h.table, i, id)
		case 4, 5:
			sql = fmt.Sprintf("UPDATE %s SET bio = repeat(md5('%d'), 100) WHERE id = %d", h.table, i, id)
		case 6:
			sql = fmt.Sprintf("DELETE FROM %s WHERE id = %d", h.table, id)
		case 7:
			sql = fmt.Sprintf("UPDATE %s SET id = %d WHERE id = %d", h.table, nextID, id)
			nextID++
		default:
			sql = fmt.Sprintf("INSERT INTO %s VALUES (%d, 'new %d', false, repeat(md5('n%d'), 100))", h.table, nextID, i, i)
			nextID++
		}
		if _, err := writer.Exec(context.Background(), sql).ReadAll(); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	const sentinel = 9_000_000
	h.exec(fmt.Sprintf("INSERT INTO %s VALUES (%d, 'sentinel', true, NULL)", h.table, sentinel))

	h.waitFor("backfill to be marked done", func() bool { status, _ := h.backfillStatus(); return status == "done" })
	h.waitFor("the sentinel", func() bool {
		events, _ := h.sink.snapshot()
		for i := len(events) - 1; i >= 0; i-- {
			if row, _ := events[i]["new"].(map[string]any); row["id"] == float64(sentinel) {
				return true
			}
		}
		return false
	})
	stop()

	events, _ := h.sink.snapshot()
	got, want := replay(events), h.tableState()
	if len(got) != len(want) {
		t.Fatalf("consumer holds %d rows, table has %d", len(got), len(want))
	}
	for id, row := range want {
		if !reflect.DeepEqual(got[id], row) {
			t.Fatalf("row %v differs:\nconsumer %v\ntable    %v", id, abbreviate(got[id]), abbreviate(row))
		}
	}
	t.Logf("%d events, %d of them reads, %d rows converged", len(events), h.reads(), len(want))
}

func abbreviate(row map[string]any) map[string]any {
	out := map[string]any{}
	for column, value := range row {
		if s, ok := value.(string); ok && len(s) > 24 {
			value = s[:24] + "..."
		}
		out[column] = value
	}
	return out
}

func TestBackfillRefusesWhatItCannotDoCorrectly(t *testing.T) {
	tests := []struct {
		name  string
		setup func(h *harness)
		want  string
	}{
		{"no primary key", func(h *harness) {
			h.exec(fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT %s_pkey", h.table, h.table))
		}, "no primary key"},
		{"replica identity full", func(h *harness) {
			h.exec(fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY FULL", h.table))
		}, "replica identity"},
		{"publication without deletes", func(h *harness) {
			h.exec(fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s WITH (publish = 'insert, update')", h.cfg.PublicationName, h.table))
		}, "must publish"},
		{"row filter", func(h *harness) {
			if version, _ := strconv.Atoi(string(h.exec("SHOW server_version_num")[0][0])); version < 150000 {
				h.t.Skip("row filters exist from Postgres 15 on")
			}
			h.exec(fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s WHERE (active)", h.cfg.PublicationName, h.table))
		}, "filters"},
		{"row-level security", func(h *harness) {
			h.exec(fmt.Sprintf("ALTER TABLE %s ENABLE ROW LEVEL SECURITY", h.table))
			h.exec(fmt.Sprintf("ALTER TABLE %s FORCE ROW LEVEL SECURITY", h.table))
			if string(h.exec("SELECT rolsuper OR rolbypassrls FROM pg_roles WHERE rolname = current_user")[0][0]) == "t" {
				h.t.Skip("the test role bypasses row-level security")
			}
		}, "row-level security"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(t, backfillOf(100, 1<<20))
			h.exec(fmt.Sprintf("INSERT INTO %s VALUES (1, 'row', true)", h.table))
			tt.setup(h)
			err := h.runFresh()
			if !errors.Is(err, replication.ErrBackfillRefused) || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err = %v, want ErrBackfillRefused mentioning %q", err, tt.want)
			}
			if h.reads() != 0 {
				t.Fatal("a refused backfill still emitted rows")
			}
		})
	}
}

// A writer that is committed in the WAL but still waiting for a synchronous standby has already
// been decoded, yet no query can see its row. It began before this session tracked anything, so
// the backfill has to wait for it rather than copy the old value over the new one.
func TestBackfillWaitsForAWriterItCannotSee(t *testing.T) {
	h := newHarness(t, nil)
	if string(h.exec("SELECT rolsuper FROM pg_roles WHERE rolname = current_user")[0][0]) != "t" {
		t.Skip("needs a superuser to change synchronous_standby_names")
	}
	h.exec(fmt.Sprintf("INSERT INTO %s VALUES (1, 'old', true)", h.table))

	// The slot has to exist first: a writer that commits before the slot's start point is never
	// decoded by it, and creating a slot waits for running transactions anyway.
	h.startSession()()
	backfillOf(100, 1<<20)(&h.cfg)
	h.cfg.DatabaseURL += "&options=-csynchronous_commit%3Dlocal"

	writer, err := pgconn.Connect(context.Background(), os.Getenv("DATABASE_URL"))
	if err != nil {
		t.Fatalf("connect writer: %v", err)
	}
	h.exec("ALTER SYSTEM SET synchronous_standby_names = 'walcast_missing_standby'")
	h.exec("SELECT pg_reload_conf()")
	t.Cleanup(func() {
		h.exec("ALTER SYSTEM RESET synchronous_standby_names")
		h.exec("SELECT pg_reload_conf()")
		_ = writer.Close(context.Background())
	})
	h.waitFor("the standby setting to load", func() bool {
		return string(h.exec("SHOW synchronous_standby_names")[0][0]) != ""
	})

	committed := make(chan error, 1)
	go func() {
		_, err := writer.Exec(context.Background(), fmt.Sprintf("UPDATE %s SET name = 'new' WHERE id = 1", h.table)).ReadAll()
		committed <- err
	}()
	h.waitFor("the writer to hang on the standby", func() bool {
		return len(h.exec("SELECT 1 FROM pg_stat_activity WHERE wait_event = 'SyncRep'")) == 1
	})
	if got := string(h.exec(fmt.Sprintf("SELECT name FROM %s WHERE id = 1", h.table))[0][0]); got != "old" {
		t.Fatalf("a query already sees %q, the scenario is not set up", got)
	}

	stop := h.startSession()
	h.waitFor("the hung writer's change to be decoded", func() bool {
		events, _ := h.sink.snapshot()
		for _, ev := range events {
			if row, _ := ev["new"].(map[string]any); ev["op"] == "update" && row["name"] == "new" {
				return true
			}
		}
		return false
	})
	time.Sleep(2 * time.Second)
	if h.reads() != 0 {
		t.Fatal("the backfill read the table while a decoded writer was still invisible")
	}

	h.exec("ALTER SYSTEM RESET synchronous_standby_names")
	h.exec("SELECT pg_reload_conf()")
	if err := <-committed; err != nil {
		t.Fatalf("writer: %v", err)
	}
	h.waitFor("backfill to be marked done", func() bool { status, _ := h.backfillStatus(); return status == "done" })
	stop()

	events, _ := h.sink.snapshot()
	if got := replay(events)[1]["name"]; got != "new" {
		t.Fatalf("consumer ends with name %q, want the committed value: %v", got, events)
	}
}

func TestBackfillAllNeverCopiesWalcastsOwnTables(t *testing.T) {
	h := newHarness(t, func(c *config.Config) {
		c.PublicationTables = nil
		c.BackfillTables, c.BackfillChunkRows, c.BackfillChunkBytes = []string{config.BackfillAll}, 100, 1<<20
	})
	h.exec(fmt.Sprintf("INSERT INTO %s VALUES (1, 'row', true)", h.table))
	others := h.exec(fmt.Sprintf(`SELECT count(*) FROM pg_tables WHERE schemaname NOT IN ('pg_catalog', 'information_schema', '%s') AND tablename <> '%s'`,
		h.cfg.StateSchema, h.table))
	if string(others[0][0]) != "0" {
		t.Skip("the database holds other tables, and an all-tables backfill would copy them too")
	}

	stop := h.startSession()
	h.waitFor("the user table to be copied", func() bool { status, _ := h.backfillStatus(); return status == "done" })
	stop()

	events, _ := h.sink.snapshot()
	for _, ev := range events {
		if table, _ := ev["table"].(string); strings.HasPrefix(table, h.cfg.StateSchema+".") {
			t.Fatalf("walcast's own state was emitted: %v", ev)
		}
	}
	t.Cleanup(func() {
		h.exec(fmt.Sprintf("DELETE FROM %s.backfills WHERE table_name NOT IN (SELECT schemaname || '.' || tablename FROM pg_tables)", h.cfg.StateSchema))
	})
}

func TestBackfillRefusesItsOwnStateTables(t *testing.T) {
	h := newHarness(t, func(c *config.Config) {
		c.PublicationTables = nil
		c.BackfillTables, c.BackfillChunkRows, c.BackfillChunkBytes = []string{c.StateSchema + ".slots"}, 100, 1<<20
	})
	if err := h.runFresh(); !errors.Is(err, replication.ErrBackfillRefused) {
		t.Fatalf("err = %v, want ErrBackfillRefused", err)
	}
}

// A key move that arrived while another table was being copied is parked on this table's progress
// row. Row 5 is behind the saved cursor, so only the parked key can make walcast read it.
func TestBackfillReadsParkedKeysBeforeResumingTheScan(t *testing.T) {
	h := newHarness(t, nil)
	h.exec(fmt.Sprintf("INSERT INTO %s SELECT g, 'seed ' || g, true FROM generate_series(1, 20) g", h.table))
	h.startSession()()
	backfillOf(100, 1<<20)(&h.cfg)
	if err := replication.EnsureBackfillTable(context.Background(), h.cfg.DatabaseURL, h.cfg.StateSchema, h.cfg.SlotName); err != nil {
		t.Fatal(err)
	}

	h.exec(fmt.Sprintf(`INSERT INTO %s.backfills (slot_name, table_name, table_oid, slot_generation, status, upper_key, last_key, pending_keys)
VALUES ('%s', 'public.%s', 'public.%s'::regclass::oid, 0, 'running', '["20"]', '["10"]', '[["5"]]')`, h.cfg.StateSchema, h.cfg.SlotName, h.table, h.table))

	stop := h.startSession()
	h.waitFor("backfill to be marked done", func() bool { status, _ := h.backfillStatus(); return status == "done" })
	stop()

	var ids []float64
	events, _ := h.sink.snapshot()
	for _, ev := range events {
		if ev["op"] == "read" {
			ids = append(ids, ev["new"].(map[string]any)["id"].(float64))
		}
	}
	if len(ids) != 11 || ids[0] != 5 || ids[1] != 11 || ids[10] != 20 {
		t.Fatalf("read ids = %v, want the parked row 5 first and then the scan from 11 to 20", ids)
	}
	if got := h.exec(fmt.Sprintf("SELECT pending_keys IS NULL FROM %s.backfills WHERE table_name = 'public.%s' AND slot_name = '%s'", h.cfg.StateSchema, h.table, h.cfg.SlotName)); string(got[0][0]) != "t" {
		t.Fatal("parked keys were not cleared after they were read")
	}
}

// One wide row cuts its chunk short at the byte budget. The row limit has to recover afterwards,
// or every later chunk of the table would stay at that size.
func TestBackfillChunkSizeRecoversAfterAWideRow(t *testing.T) {
	const rows = 400
	h := newHarness(t, backfillOf(50, 64<<10))
	h.addToastColumn()
	h.exec(fmt.Sprintf("INSERT INTO %s VALUES (1, 'wide', true, repeat(md5('wide'), 8000))", h.table))
	h.exec(fmt.Sprintf("INSERT INTO %s SELECT g, 'narrow', true, NULL FROM generate_series(2, %d) g", h.table, rows))

	stop := h.startSession()
	h.waitFor("backfill to be marked done", func() bool { status, _ := h.backfillStatus(); return status == "done" })
	stop()

	chunks := map[any]int{}
	events, _ := h.sink.snapshot()
	for _, ev := range events {
		if ev["op"] == "read" {
			chunks[ev["backfill"]]++
		}
	}
	if got := h.reads(); got != rows {
		t.Fatalf("reads = %d, want %d", got, rows)
	}
	if len(chunks) > 30 {
		t.Fatalf("%d rows took %d chunks: the chunk size never recovered from the wide row", rows, len(chunks))
	}
	t.Logf("%d rows in %d chunks", rows, len(chunks))
}

func TestBackfillMarkerIsFlushedWhenCommitsAreAsynchronous(t *testing.T) {
	h := newHarness(t, nil)
	current, err := strconv.ParseUint(string(h.exec("SELECT current_setting('server_version_num')")[0][0]), 10, 64)
	if err != nil {
		t.Fatal(err)
	}

	// The path for older servers is valid SQL on every supported one, so it is always exercised.
	for _, version := range []uint64{160000, 170000} {
		if version == 170000 && current < version {
			continue
		}
		before := string(h.exec("SELECT pg_current_wal_insert_lsn()")[0][0])
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err := replication.EmitMarker(ctx, h.cfg.DatabaseURL, version, "SET synchronous_commit = off")
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		flushed := h.exec(fmt.Sprintf("SELECT pg_current_wal_flush_lsn() > '%s'::pg_lsn", before))
		if string(flushed[0][0]) != "t" {
			t.Fatalf("server %d: marker written after %s was not flushed when emit returned", version, before)
		}
	}
}

// Two pipelines can copy the same table to different destinations. Progress saved by one must not
// make the other believe its destination already has the rows.
func TestBackfillProgressBelongsToOneSlot(t *testing.T) {
	h := newHarness(t, backfillOf(100, 1<<20))
	h.addToastColumn()
	h.seed(5)
	stop := h.startSession()
	h.waitFor("the first pipeline to finish its backfill", func() bool { status, _ := h.backfillStatus(); return status == "done" })
	stop()

	other := *h
	other.cfg.SlotName = h.cfg.SlotName + "_b"
	other.sink = &captureSink{}
	t.Cleanup(other.dropSlot)
	stop = other.startSession()
	other.waitFor("the second pipeline to read every row for its own destination", func() bool { return other.reads() == 5 })
	stop()
}
