//go:build integration

package replication_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/rs/zerolog"

	"github.com/aymaneallaoui/walcast/internal/app"
	"github.com/aymaneallaoui/walcast/internal/config"
	"github.com/aymaneallaoui/walcast/internal/event"
	"github.com/aymaneallaoui/walcast/internal/replication"
)

const waitTimeout = 30 * time.Second

type sentBatch struct {
	events int
	ack    pglogrepl.LSN
}

type captureSink struct {
	mu      sync.Mutex
	events  []map[string]any
	batches []sentBatch
	stall   time.Duration
	stalled chan struct{}
}

func (c *captureSink) Send(_ context.Context, b *event.Batch, done func(error)) {
	c.mu.Lock()
	stall := c.stall
	c.stall = 0
	c.mu.Unlock()
	if stall > 0 {
		close(c.stalled)
		time.Sleep(stall)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	dec := json.NewDecoder(bytes.NewReader(b.Buf))
	for dec.More() {
		var ev map[string]any
		if err := dec.Decode(&ev); err != nil {
			done(err)
			return
		}
		c.events = append(c.events, ev)
	}
	c.batches = append(c.batches, sentBatch{events: b.Events, ack: b.AckLSN})
	done(nil)
}

func (c *captureSink) Close() error { return nil }

func (c *captureSink) snapshot() ([]map[string]any, []sentBatch) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]map[string]any(nil), c.events...), append([]sentBatch(nil), c.batches...)
}

func (c *captureSink) count() int {
	events, _ := c.snapshot()
	return len(events)
}

type harness struct {
	t     *testing.T
	admin *pgconn.PgConn
	cfg   config.Config
	table string
	sink  *captureSink
}

func newHarness(t *testing.T, tune func(*config.Config)) *harness {
	t.Helper()
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		if os.Getenv("CI") != "" {
			t.Fatal("DATABASE_URL not set in CI: refusing to skip integration tests")
		}
		t.Skip("DATABASE_URL not set")
	}
	admin, err := pgconn.Connect(context.Background(), dbURL)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}

	suffix := time.Now().UnixNano()
	h := &harness{t: t, admin: admin, table: fmt.Sprintf("walcast_it_%d", suffix), sink: &captureSink{}}
	h.cfg = config.Config{
		DatabaseURL:       dbURL,
		ShutdownTimeout:   5 * time.Second,
		SlotName:          fmt.Sprintf("walcast_it_slot_%d", suffix),
		PublicationName:   fmt.Sprintf("walcast_it_pub_%d", suffix),
		PublicationTables: []string{"public." + h.table},
		FeedbackInterval:  200 * time.Millisecond,
		ServerTimeout:     time.Minute,
		BatchMaxBytes:     64 << 10,
		BatchLinger:       5 * time.Millisecond,
		InflightMaxBytes:  64 << 20,
		ReconnectMinDelay: 50 * time.Millisecond,
		ReconnectMaxDelay: 500 * time.Millisecond,
	}
	if tune != nil {
		tune(&h.cfg)
	}
	if err := h.cfg.Validate(); err != nil {
		t.Fatalf("test config invalid: %v", err)
	}

	h.exec(fmt.Sprintf("CREATE TABLE %s (id bigint PRIMARY KEY, name text, active boolean)", h.table))
	t.Cleanup(func() {
		for _, sql := range []string{
			fmt.Sprintf("SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name = '%s' AND active", h.cfg.SlotName),
			fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", h.cfg.PublicationName),
			fmt.Sprintf("DROP TABLE IF EXISTS %s", h.table),
		} {
			if _, err := admin.Exec(context.Background(), sql).ReadAll(); err != nil {
				t.Errorf("cleanup %q: %v", sql, err)
			}
		}
		h.dropSlot()
		_ = admin.Close(context.Background())
	})
	return h
}

func (h *harness) exec(sql string) [][][]byte {
	h.t.Helper()
	results, err := h.admin.Exec(context.Background(), sql).ReadAll()
	if err != nil {
		h.t.Fatalf("%s: %v", sql, err)
	}
	if len(results) == 0 {
		return nil
	}
	return results[0].Rows
}

// dropSlot retries because a terminated walsender releases its slot asynchronously.
func (h *harness) dropSlot() {
	sql := fmt.Sprintf("SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = '%s'", h.cfg.SlotName)
	var err error
	for range 50 {
		if _, err = h.admin.Exec(context.Background(), sql).ReadAll(); err == nil {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	h.t.Errorf("drop slot %s: %v", h.cfg.SlotName, err)
}

func (h *harness) waitFor(what string, cond func() bool) {
	h.t.Helper()
	deadline := time.Now().Add(waitTimeout)
	for !cond() {
		if time.Now().After(deadline) {
			h.t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (h *harness) slotActive() bool {
	return len(h.exec(fmt.Sprintf("SELECT 1 FROM pg_replication_slots WHERE slot_name = '%s' AND active", h.cfg.SlotName))) == 1
}

func (h *harness) confirmedLSN() pglogrepl.LSN {
	rows := h.exec(fmt.Sprintf("SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = '%s'", h.cfg.SlotName))
	lsn, err := pglogrepl.ParseLSN(string(rows[0][0]))
	if err != nil {
		h.t.Fatalf("parse confirmed_flush_lsn: %v", err)
	}
	return lsn
}

func (h *harness) startSession() (stop func() pglogrepl.LSN) {
	h.t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	type result struct {
		flushed pglogrepl.LSN
		err     error
	}
	finished := make(chan result, 1)
	go func() {
		flushed, err := replication.NewRunner(h.cfg, h.sink, zerolog.Nop()).Run(ctx)
		finished <- result{flushed, err}
	}()
	h.t.Cleanup(cancel)
	h.waitFor("slot to become active", func() bool {
		select {
		case res := <-finished:
			h.t.Fatalf("session ended before the slot became active: %v", res.err)
		default:
		}
		return h.slotActive()
	})

	return func() pglogrepl.LSN {
		h.t.Helper()
		cancel()
		select {
		case res := <-finished:
			if res.err != nil {
				h.t.Fatalf("Run returned error on shutdown: %v", res.err)
			}
			return res.flushed
		case <-time.After(waitTimeout):
			h.t.Fatal("Run did not stop after cancel")
			return 0
		}
	}
}

func TestInsertUpdateDeleteArriveInOrderAndAreAcked(t *testing.T) {
	h := newHarness(t, nil)
	stop := h.startSession()

	h.exec(fmt.Sprintf("INSERT INTO %s VALUES (1, 'first', true)", h.table))
	h.exec(fmt.Sprintf("UPDATE %s SET name = 'second' WHERE id = 1", h.table))
	h.exec(fmt.Sprintf("DELETE FROM %s WHERE id = 1", h.table))

	h.waitFor("3 events", func() bool { return h.sink.count() >= 3 })
	events, _ := h.sink.snapshot()
	if len(events) != 3 {
		t.Fatalf("got %d events, want 3: %v", len(events), events)
	}

	wantOps := []string{"insert", "update", "delete"}
	var prevLSN pglogrepl.LSN
	for i, ev := range events {
		if ev["op"] != wantOps[i] || ev["table"] != "public."+h.table {
			t.Errorf("event %d: op=%v table=%v", i, ev["op"], ev["table"])
		}
		if ev["txid"].(float64) == 0 || ev["ts"] == "" {
			t.Errorf("event %d: missing txid or ts: %v", i, ev)
		}
		lsn, err := pglogrepl.ParseLSN(ev["lsn"].(string))
		if err != nil || lsn <= prevLSN {
			t.Errorf("event %d: lsn %v not increasing (prev %s, err %v)", i, ev["lsn"], prevLSN, err)
		}
		prevLSN = lsn
	}

	inserted := events[0]["new"].(map[string]any)
	if inserted["id"] != float64(1) || inserted["name"] != "first" || inserted["active"] != true {
		t.Errorf("insert new = %v", inserted)
	}
	if updated := events[1]["new"].(map[string]any); updated["name"] != "second" {
		t.Errorf("update new = %v", updated)
	}
	if deleted := events[2]["old"].(map[string]any); deleted["id"] != float64(1) {
		t.Errorf("delete old = %v", deleted)
	}

	lastCommit, err := pglogrepl.ParseLSN(events[2]["commit_lsn"].(string))
	if err != nil {
		t.Fatal(err)
	}
	h.waitFor("slot confirmed_flush_lsn to pass the last commit", func() bool { return h.confirmedLSN() >= lastCommit })

	if flushed := stop(); flushed < lastCommit {
		t.Fatalf("flushed %s is behind last commit %s", flushed, lastCommit)
	}
	h.waitFor("slot to be released", func() bool { return !h.slotActive() })
}

func TestLargeTransactionFragmentsCarryNoAckUntilCommit(t *testing.T) {
	const rows = 300
	h := newHarness(t, func(c *config.Config) { c.BatchMaxBytes = 512 })
	stop := h.startSession()

	h.exec(fmt.Sprintf("INSERT INTO %s SELECT g, 'row ' || g, true FROM generate_series(1, %d) g", h.table, rows))

	h.waitFor("all rows", func() bool { return h.sink.count() >= rows })
	events, _ := h.sink.snapshot()
	commitLSN, err := pglogrepl.ParseLSN(events[0]["commit_lsn"].(string))
	if err != nil {
		t.Fatal(err)
	}
	h.waitFor("transaction to be confirmed", func() bool { return h.confirmedLSN() >= commitLSN })
	stop()

	events, batches := h.sink.snapshot()
	if len(events) != rows {
		t.Fatalf("got %d events, want %d", len(events), rows)
	}
	for i, ev := range events {
		if ev["seq"] != float64(i) {
			t.Fatalf("event %d has seq %v: order broken across fragments", i, ev["seq"])
		}
	}

	if len(batches) < 2 {
		t.Fatalf("transaction was not fragmented: %d batches", len(batches))
	}
	delivered := 0
	for i, b := range batches {
		delivered += b.events
		if b.ack != 0 && delivered < rows {
			t.Fatalf("batch %d carries ack %s with only %d of %d rows delivered", i, b.ack, delivered, rows)
		}
	}
}

func TestStalledSinkDoesNotTripWalSenderTimeout(t *testing.T) {
	const stall = 5 * time.Second
	h := newHarness(t, func(c *config.Config) {
		u, err := url.Parse(c.DatabaseURL)
		if err != nil {
			t.Fatal(err)
		}
		q := u.Query()
		q.Set("options", "-cwal_sender_timeout=2s")
		u.RawQuery = q.Encode()
		c.DatabaseURL = u.String()
		c.InflightMaxBytes = 1
	})
	stop := h.startSession()

	h.sink.mu.Lock()
	h.sink.stall = stall
	h.sink.stalled = make(chan struct{})
	h.sink.mu.Unlock()

	h.exec(fmt.Sprintf("INSERT INTO %s VALUES (1, 'a', true)", h.table))
	select {
	case <-h.sink.stalled:
	case <-time.After(waitTimeout):
		t.Fatal("sink never received the first batch")
	}
	h.exec(fmt.Sprintf("INSERT INTO %s VALUES (2, 'b', true)", h.table))

	time.Sleep(stall / 2)
	if !h.slotActive() {
		t.Fatal("walsender dropped the connection while the sink was stalled")
	}

	h.waitFor("both events after the stall", func() bool { return h.sink.count() >= 2 })
	stop()
	if events, _ := h.sink.snapshot(); len(events) != 2 {
		t.Fatalf("got %d events, want exactly 2 (a reconnect would have replayed)", len(events))
	}
}

func TestSupervisorReconnectsAndLosesNothing(t *testing.T) {
	h := newHarness(t, nil)
	ctx, cancel := context.WithCancel(context.Background())
	finished := make(chan error, 1)
	go func() { finished <- app.New(h.cfg, zerolog.Nop(), h.sink).Run(ctx) }()
	t.Cleanup(cancel)
	h.waitFor("slot to become active", h.slotActive)

	h.exec(fmt.Sprintf("INSERT INTO %s VALUES (1, 'before', true)", h.table))
	h.waitFor("first event", func() bool { return h.sink.count() >= 1 })

	h.exec(fmt.Sprintf("SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name = '%s'", h.cfg.SlotName))
	h.exec(fmt.Sprintf("INSERT INTO %s VALUES (2, 'during', true)", h.table))
	h.waitFor("reconnect", h.slotActive)
	h.exec(fmt.Sprintf("INSERT INTO %s VALUES (3, 'after', true)", h.table))

	seen := func() map[float64]bool {
		events, _ := h.sink.snapshot()
		ids := make(map[float64]bool)
		for _, ev := range events {
			ids[ev["new"].(map[string]any)["id"].(float64)] = true
		}
		return ids
	}
	h.waitFor("all three rows", func() bool { return len(seen()) == 3 })

	cancel()
	select {
	case err := <-finished:
		if err != nil {
			t.Fatalf("app returned error: %v", err)
		}
	case <-time.After(waitTimeout):
		t.Fatal("app did not stop after cancel")
	}
}

func TestSupervisorStopsWhenSlotDisappears(t *testing.T) {
	h := newHarness(t, func(c *config.Config) {
		c.ReconnectMinDelay = 2 * time.Second
		c.ReconnectMaxDelay = 4 * time.Second
	})
	ctx, cancel := context.WithCancel(context.Background())
	finished := make(chan error, 1)
	go func() { finished <- app.New(h.cfg, zerolog.Nop(), h.sink).Run(ctx) }()
	t.Cleanup(cancel)
	h.waitFor("slot to become active", h.slotActive)

	h.exec(fmt.Sprintf("SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name = '%s'", h.cfg.SlotName))
	h.dropSlot()

	select {
	case err := <-finished:
		if !errors.Is(err, replication.ErrSlotUnusable) {
			t.Fatalf("err = %v, want ErrSlotUnusable", err)
		}
	case <-time.After(waitTimeout):
		t.Fatal("supervisor kept running after its slot was dropped")
	}
}
