//go:build integration

package replication_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/rs/zerolog"

	"github.com/aymaneallaoui/walcast/internal/config"
	"github.com/aymaneallaoui/walcast/internal/replication"
)

// runFresh behaves like a newly started process: a new Runner remembers nothing in memory.
func (h *harness) runFresh() error {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	_, err := replication.NewRunner(h.cfg, h.sink, zerolog.Nop()).Run(ctx)
	return err
}

func (h *harness) stateGeneration() string {
	h.t.Helper()
	rows := h.exec(fmt.Sprintf("SELECT generation FROM %s.slots WHERE slot_name = '%s'", h.cfg.StateSchema, h.cfg.SlotName))
	if len(rows) != 1 {
		h.t.Fatalf("state rows for %s = %d, want 1", h.cfg.SlotName, len(rows))
	}
	return string(rows[0][0])
}

func TestSlotLostWhileDownIsRefusedUntilTheGenerationMatches(t *testing.T) {
	h := newHarness(t, nil)

	stop := h.startSession()
	h.exec(fmt.Sprintf("INSERT INTO %s VALUES (1, 'before', true)", h.table))
	h.waitFor("first event", func() bool { return h.sink.count() >= 1 })
	stop()
	if got := h.stateGeneration(); got != "0" {
		t.Fatalf("generation after first bootstrap = %s, want 0", got)
	}

	h.dropSlot()
	h.exec(fmt.Sprintf("INSERT INTO %s VALUES (2, 'lost', true)", h.table))

	err := h.runFresh()
	if !errors.Is(err, replication.ErrSlotLost) || !errors.Is(err, replication.ErrSlotUnusable) {
		t.Fatalf("restart without the slot: err = %v, want ErrSlotLost wrapped in ErrSlotUnusable", err)
	}
	if !strings.Contains(err.Error(), "SLOT_RECREATE_GENERATION=1") {
		t.Fatalf("error does not name the override: %v", err)
	}
	if rows := h.exec(fmt.Sprintf("SELECT 1 FROM pg_replication_slots WHERE slot_name = '%s'", h.cfg.SlotName)); len(rows) != 0 {
		t.Fatal("refused start still recreated the slot")
	}

	h.cfg.SlotRecreateGeneration = 1
	stop = h.startSession()
	h.exec(fmt.Sprintf("INSERT INTO %s VALUES (3, 'after', true)", h.table))
	h.waitFor("event after the accepted gap", func() bool { return h.sink.count() >= 2 })
	stop()
	if got := h.stateGeneration(); got != "1" {
		t.Fatalf("generation after recreation = %s, want 1", got)
	}
	events, _ := h.sink.snapshot()
	for _, ev := range events {
		if row, _ := ev["new"].(map[string]any); row["id"] == float64(2) {
			t.Fatal("row written while the slot was gone was delivered, the gap is not real")
		}
	}

	h.dropSlot()
	err = h.runFresh()
	if !errors.Is(err, replication.ErrSlotLost) || !strings.Contains(err.Error(), "SLOT_RECREATE_GENERATION=2") {
		t.Fatalf("a spent generation recreated the slot again: err = %v", err)
	}
}

func TestExistingSlotWithoutStateIsAdopted(t *testing.T) {
	h := newHarness(t, nil)
	h.exec(fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", h.cfg.PublicationName, h.table))
	h.exec(fmt.Sprintf("SELECT pg_create_logical_replication_slot('%s', 'pgoutput')", h.cfg.SlotName))
	h.exec(fmt.Sprintf("INSERT INTO %s VALUES (1, 'queued', true)", h.table))

	stop := h.startSession()
	h.waitFor("event queued before adoption", func() bool { return h.sink.count() >= 1 })
	stop()
	if got := h.stateGeneration(); got != "0" {
		t.Fatalf("generation of an adopted slot = %s, want 0", got)
	}
}

func TestStateTableWritesAreNotStreamed(t *testing.T) {
	h := newHarness(t, func(c *config.Config) { c.PublicationTables = nil })

	stop := h.startSession()
	h.exec(fmt.Sprintf("INSERT INTO %s VALUES (1, 'visible', true)", h.table))
	h.waitFor("event from the user table", func() bool {
		events, _ := h.sink.snapshot()
		for _, ev := range events {
			if ev["table"] == "public."+h.table {
				return true
			}
		}
		return false
	})
	stop()

	events, _ := h.sink.snapshot()
	for _, ev := range events {
		if table, _ := ev["table"].(string); strings.HasPrefix(table, h.cfg.StateSchema+".") {
			t.Fatalf("state table change reached the sink: %v", ev)
		}
	}
}

func TestStateSchemaNamedAfterTheRoleIsRefused(t *testing.T) {
	h := newHarness(t, nil)
	role := string(h.exec("SELECT current_user")[0][0])
	h.cfg.StateSchema = role
	if err := h.cfg.Validate(); err != nil {
		t.Skipf("role %q is not a valid STATE_SCHEMA: %v", role, err)
	}

	err := h.runFresh()
	if !errors.Is(err, replication.ErrStateStore) {
		t.Fatalf("err = %v, want ErrStateStore", err)
	}
	if rows := h.exec(fmt.Sprintf("SELECT 1 FROM pg_namespace WHERE nspname = '%s'", role)); len(rows) != 0 {
		t.Fatalf("schema %s was created although it shadows the role's search_path", role)
	}
}
