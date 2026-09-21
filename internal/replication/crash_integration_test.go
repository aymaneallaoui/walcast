//go:build integration

package replication_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

const (
	crashRows   = 3000
	crashRounds = 3

	// A slow receiver keeps batches queued behind the one in flight. Without that backlog nothing
	// is ever undelivered at kill time, and an ack sent too early would go unnoticed.
	receiverDelay = 15 * time.Millisecond
)

type delivery struct {
	id        int
	commitLSN string
	seq       float64
}

type crashReceiver struct {
	mu         sync.Mutex
	deliveries []delivery
}

func (r *crashReceiver) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	body, err := io.ReadAll(req.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	var batch []delivery
	for _, line := range ndjson(body) {
		var ev struct {
			CommitLSN string  `json:"commit_lsn"`
			Seq       float64 `json:"seq"`
			New       struct {
				ID int `json:"id"`
			} `json:"new"`
		}
		if err := json.Unmarshal(line, &ev); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		batch = append(batch, delivery{id: ev.New.ID, commitLSN: ev.CommitLSN, seq: ev.Seq})
	}
	time.Sleep(receiverDelay)
	r.mu.Lock()
	r.deliveries = append(r.deliveries, batch...)
	r.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}

// ndjson splits a body into its lines instead of streaming it through json.Decoder: under Go 1.27
// the race detector misreports the decoder's own buffer handling, with both accesses on one goroutine.
func ndjson(body []byte) [][]byte {
	var lines [][]byte
	for line := range bytes.Lines(body) {
		if line = bytes.TrimSpace(line); len(line) > 0 {
			lines = append(lines, line)
		}
	}
	return lines
}

func (r *crashReceiver) snapshot() []delivery {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]delivery(nil), r.deliveries...)
}

// progress returns how many rows arrived in id order and the first row that was skipped, if any.
// Rows are committed in id order, so a first arrival that jumps ahead means a committed row is gone.
func (r *crashReceiver) progress() (contiguous, skipped int) {
	seen := make(map[int]struct{})
	for _, d := range r.snapshot() {
		if _, dup := seen[d.id]; dup {
			continue
		}
		if d.id != len(seen)+1 {
			return len(seen), len(seen) + 1
		}
		seen[d.id] = struct{}{}
	}
	return len(seen), 0
}

func (h *harness) waitForRows(recv *crashReceiver, want int, stage string) {
	h.t.Helper()
	h.waitFor(fmt.Sprintf("%d rows %s", want, stage), func() bool {
		got, skipped := recv.progress()
		if skipped != 0 {
			h.t.Fatalf("row %d was committed but never delivered (%s)", skipped, stage)
		}
		return got >= want
	})
}

func buildWalcast(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "walcast")
	out, err := exec.Command("go", "build", "-o", bin, "github.com/aymaneallaoui/walcast/cmd/walcast").CombinedOutput()
	if err != nil {
		t.Fatalf("build walcast: %v\n%s", err, out)
	}
	return bin
}

func (h *harness) startProcess(bin, webhookURL string, stderr *os.File, extraEnv ...string) *exec.Cmd {
	h.t.Helper()
	cmd := exec.Command(bin)
	cmd.Dir = h.t.TempDir()
	cmd.Env = append([]string{
		"DATABASE_URL=" + h.cfg.DatabaseURL,
		"SLOT_NAME=" + h.cfg.SlotName,
		"PUBLICATION_NAME=" + h.cfg.PublicationName,
		"PUBLICATION_TABLES=public." + h.table,
		"STATE_SCHEMA=" + h.cfg.StateSchema,
		"SINK=webhook",
		"WEBHOOK_URL=" + webhookURL,
		"WEBHOOK_SECRET=crash-recovery-test-secret",
		"FEEDBACK_INTERVAL=20ms",
		"RECONNECT_MIN_DELAY=50ms",
		"RECONNECT_MAX_DELAY=500ms",
		"LOG_LEVEL=warn",
	}, extraEnv...)
	cmd.Stderr = stderr
	if err := cmd.Start(); err != nil {
		h.t.Fatalf("start walcast: %v", err)
	}
	h.t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})
	return cmd
}

func TestKillNineLosesNoCommittedRow(t *testing.T) {
	h := newHarness(t, nil)
	bin := buildWalcast(t)
	recv := &crashReceiver{}
	srv := httptest.NewServer(recv)
	t.Cleanup(srv.Close)
	// An *os.File goes to the child as a descriptor, so no goroutine copies output between kills.
	stderr, err := os.Create(filepath.Join(t.TempDir(), "walcast.stderr"))
	if err != nil {
		t.Fatalf("create stderr file: %v", err)
	}
	t.Cleanup(func() {
		if out, readErr := os.ReadFile(stderr.Name()); t.Failed() && readErr == nil {
			t.Logf("walcast stderr:\n%s", out)
		}
		_ = stderr.Close()
	})

	proc := h.startProcess(bin, srv.URL, stderr)
	h.waitFor("slot to become active", h.slotActive)

	writer, err := pgconn.Connect(context.Background(), h.cfg.DatabaseURL)
	if err != nil {
		t.Fatalf("connect writer: %v", err)
	}
	written := make(chan error, 1)
	go func() {
		defer func() { _ = writer.Close(context.Background()) }()
		for id := 1; id <= crashRows; id++ {
			sql := fmt.Sprintf("INSERT INTO %s VALUES (%d, 'row', true)", h.table, id)
			if _, err := writer.Exec(context.Background(), sql).ReadAll(); err != nil {
				written <- err
				return
			}
		}
		written <- nil
	}()

	for round := 1; round <= crashRounds; round++ {
		h.waitForRows(recv, round*crashRows/(crashRounds+1), fmt.Sprintf("before kill %d", round))
		if err := proc.Process.Signal(syscall.SIGKILL); err != nil {
			t.Fatalf("kill -9: %v", err)
		}
		_ = proc.Wait()
		proc = h.startProcess(bin, srv.URL, stderr)
	}

	if err := <-written; err != nil {
		t.Fatalf("writer: %v", err)
	}
	h.waitForRows(recv, crashRows, "after the last restart")

	identity := make(map[int]delivery)
	duplicates := 0
	for _, d := range recv.snapshot() {
		first, seen := identity[d.id]
		if !seen {
			identity[d.id] = d
			continue
		}
		duplicates++
		if first.commitLSN != d.commitLSN || first.seq != d.seq {
			t.Fatalf("replay of row %d changed its dedupe identity: %+v then %+v", d.id, first, d)
		}
	}
	if len(identity) != crashRows {
		t.Fatalf("delivered %d distinct rows, want %d", len(identity), crashRows)
	}
	t.Logf("%d kills, %d rows, %d replayed duplicates, all with a stable commit_lsn and seq", crashRounds, crashRows, duplicates)
}

// eventLog keeps whole events in arrival order, so a test can replay them into a consumer.
type eventLog struct {
	mu     sync.Mutex
	events []map[string]any
}

func (l *eventLog) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	body, err := io.ReadAll(req.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	var batch []map[string]any
	for _, line := range ndjson(body) {
		var ev map[string]any
		if err := json.Unmarshal(line, &ev); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		batch = append(batch, ev)
	}
	time.Sleep(receiverDelay)
	l.mu.Lock()
	l.events = append(l.events, batch...)
	l.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}

func (l *eventLog) snapshot() []map[string]any {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]map[string]any(nil), l.events...)
}

func (l *eventLog) count(match func(map[string]any) bool) int {
	n := 0
	for _, ev := range l.snapshot() {
		if match(ev) {
			n++
		}
	}
	return n
}

// dedupe drops repeated events the way the README tells consumers to: stream events on commit_lsn
// and seq, reads on backfill and seq. Ids that repeated across different rows would lose data here.
func dedupe(events []map[string]any) []map[string]any {
	seen := make(map[string]struct{})
	out := events[:0:0]
	for _, ev := range events {
		id := fmt.Sprintf("c/%v/%v", ev["commit_lsn"], ev["seq"])
		if ev["op"] == "read" {
			id = fmt.Sprintf("b/%v/%v", ev["backfill"], ev["seq"])
		}
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, ev)
	}
	return out
}

func TestKillNineDuringABackfillStillConverges(t *testing.T) {
	const seeded, writes = 3000, 600
	h := newHarness(t, nil)
	h.addToastColumn()
	h.seed(seeded)
	bin := buildWalcast(t)
	log := &eventLog{}
	srv := httptest.NewServer(log)
	t.Cleanup(srv.Close)
	stderr, err := os.Create(filepath.Join(t.TempDir(), "walcast.stderr"))
	if err != nil {
		t.Fatalf("create stderr file: %v", err)
	}
	t.Cleanup(func() {
		if out, readErr := os.ReadFile(stderr.Name()); t.Failed() && readErr == nil {
			t.Logf("walcast stderr:\n%s", out)
		}
		_ = stderr.Close()
	})
	env := []string{"BACKFILL_TABLES=public." + h.table, "BACKFILL_CHUNK_ROWS=100"}
	isRead := func(ev map[string]any) bool { return ev["op"] == "read" }

	proc := h.startProcess(bin, srv.URL, stderr, env...)
	writer, err := pgconn.Connect(context.Background(), h.cfg.DatabaseURL)
	if err != nil {
		t.Fatalf("connect writer: %v", err)
	}
	written := make(chan error, 1)
	go func() {
		defer func() { _ = writer.Close(context.Background()) }()
		for i := range writes {
			id := i*5%seeded + 1
			sql := fmt.Sprintf("UPDATE %s SET name = 'patch %d' WHERE id = %d", h.table, i, id)
			switch i % 4 {
			case 1:
				sql = fmt.Sprintf("UPDATE %s SET bio = repeat(md5('%d'), 100) WHERE id = %d", h.table, i, id)
			case 2:
				sql = fmt.Sprintf("INSERT INTO %s VALUES (%d, 'new %d', false, repeat(md5('n%d'), 100))", h.table, seeded+1+i, i, i)
			case 3:
				sql = fmt.Sprintf("UPDATE %s SET id = %d WHERE id = %d", h.table, 2*seeded+i, id)
			}
			if _, err := writer.Exec(context.Background(), sql).ReadAll(); err != nil {
				written <- fmt.Errorf("%s: %w", sql, err)
				return
			}
		}
		written <- nil
	}()

	for round := 1; round <= crashRounds; round++ {
		want := round * seeded / (crashRounds + 2)
		h.waitFor(fmt.Sprintf("%d reads before kill %d", want, round), func() bool { return log.count(isRead) >= want })
		if err := proc.Process.Signal(syscall.SIGKILL); err != nil {
			t.Fatalf("kill -9: %v", err)
		}
		_ = proc.Wait()
		proc = h.startProcess(bin, srv.URL, stderr, env...)
	}
	if err := <-written; err != nil {
		t.Fatalf("writer: %v", err)
	}
	const sentinel = 9_000_000
	h.exec(fmt.Sprintf("INSERT INTO %s VALUES (%d, 'sentinel', true, NULL)", h.table, sentinel))
	h.waitFor("backfill to be marked done", func() bool { status, _ := h.backfillStatus(); return status == "done" })
	h.waitFor("the sentinel", func() bool {
		return log.count(func(ev map[string]any) bool {
			row, _ := ev["new"].(map[string]any)
			return row["id"] == float64(sentinel)
		}) > 0
	})

	events := log.snapshot()
	got, want := replay(dedupe(events)), h.tableState()
	if len(got) != len(want) {
		t.Fatalf("consumer holds %d rows, table has %d", len(got), len(want))
	}
	for id, row := range want {
		if !reflect.DeepEqual(got[id], row) {
			t.Fatalf("row %v differs:\nconsumer %v\ntable    %v\nhistory:\n%s", id, abbreviate(got[id]), abbreviate(row), history(events, id))
		}
	}
	t.Logf("%d kills, %d events delivered, %d of them reads for %d seeded rows, %d rows converged",
		crashRounds, len(events), log.count(isRead), seeded, len(want))
}

// history lists every delivered event that touched a row, following it back through key moves.
func history(events []map[string]any, id float64) string {
	ids := map[float64]bool{id: true}
	for i := len(events) - 1; i > 0; i-- {
		row, _ := events[i]["new"].(map[string]any)
		old, _ := events[i-1]["old"].(map[string]any)
		if events[i]["origin"] == "update" && events[i]["op"] == "insert" && ids[row["id"].(float64)] && old != nil {
			ids[old["id"].(float64)] = true
		}
	}
	var out strings.Builder
	for i, ev := range events {
		row, _ := ev["new"].(map[string]any)
		old, _ := ev["old"].(map[string]any)
		newID, _ := row["id"].(float64)
		oldID, _ := old["id"].(float64)
		if !ids[newID] && !ids[oldID] {
			continue
		}
		_, hasBio := row["bio"]
		fmt.Fprintf(&out, "  #%d %v origin=%v new=%v old=%v bio=%v unchanged=%v commit_lsn=%v backfill=%v\n",
			i, ev["op"], ev["origin"], row["id"], old["id"], hasBio, ev["unchanged"], ev["commit_lsn"], ev["backfill"])
	}
	return out.String()
}

// stallingLog accepts the batch that carries a key move, then holds every later request. That
// lets the move be acked to Postgres while the row's re-read cannot complete: the exact window in
// which a crash would forget the move, unless the ack was held back before it.
type stallingLog struct {
	eventLog
	armed   atomic.Bool
	stalled atomic.Bool
	// trigger picks the event after whose batch every later request stalls; nil means a key move.
	trigger func(ev map[string]any) bool
	moved   chan struct{}
	release chan struct{}
}

func (l *stallingLog) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if l.stalled.Load() {
		<-l.release
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	before := len(l.snapshot())
	l.eventLog.ServeHTTP(w, req)
	// Armed once: after the restart the move is replayed, and stalling again would block it forever.
	for _, ev := range l.snapshot()[before:] {
		hit := ev["op"] == "insert" && ev["origin"] == "update"
		if l.trigger != nil {
			hit = l.trigger(ev)
		}
		if hit && l.armed.CompareAndSwap(true, false) {
			l.stalled.Store(true)
			close(l.moved)
		}
	}
}

func TestKillNineAfterAnAckedKeyMoveStillDeliversTheRow(t *testing.T) {
	const seeded, movedFrom, movedTo = 3000, 2990, 9999
	h := newHarness(t, nil)
	h.addToastColumn()
	h.seed(seeded)
	bin := buildWalcast(t)
	log := &stallingLog{moved: make(chan struct{}), release: make(chan struct{})}
	log.armed.Store(true)
	srv := httptest.NewServer(log)
	t.Cleanup(srv.Close)
	stderr, err := os.Create(filepath.Join(t.TempDir(), "walcast.stderr"))
	if err != nil {
		t.Fatalf("create stderr file: %v", err)
	}
	t.Cleanup(func() {
		if out, readErr := os.ReadFile(stderr.Name()); t.Failed() && readErr == nil {
			t.Logf("walcast stderr:\n%s", out)
		}
		_ = stderr.Close()
	})
	env := []string{"BACKFILL_TABLES=public." + h.table, "BACKFILL_CHUNK_ROWS=100"}

	proc := h.startProcess(bin, srv.URL, stderr, env...)
	h.waitFor("the first chunk", func() bool {
		return log.count(func(ev map[string]any) bool { return ev["op"] == "read" }) >= 100
	})
	// The old key is far ahead of the scan and the new one is above its upper bound, so neither
	// the consumer nor the scan will ever see this row's TOAST column unless it is read again.
	h.exec(fmt.Sprintf("UPDATE %s SET id = %d WHERE id = %d", h.table, movedTo, movedFrom))

	select {
	case <-log.moved:
	case <-time.After(waitTimeout):
		t.Fatal("the key move was never delivered")
	}
	time.Sleep(500 * time.Millisecond)
	if err := proc.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatalf("kill -9: %v", err)
	}
	_ = proc.Wait()
	log.stalled.Store(false)
	close(log.release)

	h.startProcess(bin, srv.URL, stderr, env...)
	h.waitFor("backfill to be marked done", func() bool { status, _ := h.backfillStatus(); return status == "done" })
	const sentinel = 9_000_000
	h.exec(fmt.Sprintf("INSERT INTO %s VALUES (%d, 'sentinel', true, NULL)", h.table, sentinel))
	h.waitFor("the sentinel", func() bool {
		return log.count(func(ev map[string]any) bool {
			row, _ := ev["new"].(map[string]any)
			return row["id"] == float64(sentinel)
		}) > 0
	})

	events := log.snapshot()
	got, want := replay(dedupe(events)), h.tableState()
	if !reflect.DeepEqual(got[movedTo], want[movedTo]) {
		t.Fatalf("moved row differs:\nconsumer %v\ntable    %v\nhistory:\n%s",
			abbreviate(got[movedTo]), abbreviate(want[movedTo]), history(events, movedTo))
	}
	if len(got) != len(want) {
		t.Fatalf("consumer holds %d rows, table has %d", len(got), len(want))
	}
}

// Two rows move in one transaction and are read again one at a time. The first re-read coming back
// clean says nothing about the second, so the ack has to stay before the move until both are done.
func TestKillNineBetweenTwoRereadsStillDeliversTheSecondRow(t *testing.T) {
	const seeded, firstTo, secondTo = 400, 9990, 9991
	h := newHarness(t, nil)
	h.addToastColumn()
	h.seed(seeded)
	bin := buildWalcast(t)
	log := &stallingLog{moved: make(chan struct{}), release: make(chan struct{})}
	log.trigger = func(ev map[string]any) bool {
		row, _ := ev["new"].(map[string]any)
		return ev["op"] == "read" && row["id"] == float64(firstTo)
	}
	log.armed.Store(true)
	srv := httptest.NewServer(log)
	t.Cleanup(srv.Close)
	stderr, err := os.Create(filepath.Join(t.TempDir(), "walcast.stderr"))
	if err != nil {
		t.Fatalf("create stderr file: %v", err)
	}
	t.Cleanup(func() {
		if out, readErr := os.ReadFile(stderr.Name()); t.Failed() && readErr == nil {
			t.Logf("walcast stderr:\n%s", out)
		}
		_ = stderr.Close()
	})
	env := []string{"BACKFILL_TABLES=public." + h.table, "BACKFILL_CHUNK_ROWS=1"}

	proc := h.startProcess(bin, srv.URL, stderr, env...)
	h.waitFor("the first chunks", func() bool {
		return log.count(func(ev map[string]any) bool { return ev["op"] == "read" }) >= 20
	})
	h.exec(fmt.Sprintf("UPDATE %s SET id = id + 9600 WHERE id IN (390, 391)", h.table))

	select {
	case <-log.moved:
	case <-time.After(waitTimeout):
		t.Fatal("the first moved row was never read again")
	}
	time.Sleep(500 * time.Millisecond)
	if err := proc.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatalf("kill -9: %v", err)
	}
	_ = proc.Wait()
	log.stalled.Store(false)
	close(log.release)

	h.startProcess(bin, srv.URL, stderr, env...)
	h.waitFor("backfill to be marked done", func() bool { status, _ := h.backfillStatus(); return status == "done" })
	const sentinel = 9_000_000
	h.exec(fmt.Sprintf("INSERT INTO %s VALUES (%d, 'sentinel', true, NULL)", h.table, sentinel))
	h.waitFor("the sentinel", func() bool {
		return log.count(func(ev map[string]any) bool {
			row, _ := ev["new"].(map[string]any)
			return row["id"] == float64(sentinel)
		}) > 0
	})

	events := log.snapshot()
	got, want := replay(dedupe(events)), h.tableState()
	for _, id := range []float64{firstTo, secondTo} {
		if !reflect.DeepEqual(got[id], want[id]) {
			t.Fatalf("moved row %v differs:\nconsumer %v\ntable    %v\nhistory:\n%s",
				id, abbreviate(got[id]), abbreviate(want[id]), history(events, id))
		}
	}
	if len(got) != len(want) {
		t.Fatalf("consumer holds %d rows, table has %d", len(got), len(want))
	}
}
