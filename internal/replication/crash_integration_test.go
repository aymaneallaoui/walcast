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
	"sync"
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
	dec := json.NewDecoder(bytes.NewReader(body))
	for dec.More() {
		var ev struct {
			CommitLSN string  `json:"commit_lsn"`
			Seq       float64 `json:"seq"`
			New       struct {
				ID int `json:"id"`
			} `json:"new"`
		}
		if err := dec.Decode(&ev); err != nil {
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

func (h *harness) startProcess(bin, webhookURL string, stderr *os.File) *exec.Cmd {
	h.t.Helper()
	cmd := exec.Command(bin)
	cmd.Dir = h.t.TempDir()
	cmd.Env = []string{
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
	}
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
