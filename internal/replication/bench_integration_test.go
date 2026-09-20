//go:build integration

package replication_test

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/rs/zerolog"

	"github.com/aymaneallaoui/walcast/internal/config"
	"github.com/aymaneallaoui/walcast/internal/event"
	"github.com/aymaneallaoui/walcast/internal/replication"
)

// countingSink acknowledges at once, so these benchmarks measure walcast and Postgres, not a sink.
type countingSink struct {
	events atomic.Int64
	bytes  atomic.Int64
}

func (c *countingSink) Send(_ context.Context, b *event.Batch, done func(error)) {
	c.events.Add(int64(b.Events))
	c.bytes.Add(int64(len(b.Buf)))
	done(nil)
}

func (c *countingSink) Close() error { return nil }

type benchEnv struct {
	admin *pgconn.PgConn
	cfg   config.Config
	table string
}

func newBenchEnv(b *testing.B, tune func(*config.Config)) *benchEnv {
	b.Helper()
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		b.Skip("DATABASE_URL not set")
	}
	admin, err := pgconn.Connect(context.Background(), dbURL)
	if err != nil {
		b.Fatalf("connect: %v", err)
	}
	suffix := time.Now().UnixNano()
	e := &benchEnv{admin: admin, table: fmt.Sprintf("walcast_bench_%d", suffix)}
	e.cfg = config.Config{
		DatabaseURL: dbURL, ShutdownTimeout: 10 * time.Second, SettleTimeout: time.Minute,
		SlotName: fmt.Sprintf("walcast_bench_slot_%d", suffix), PublicationName: fmt.Sprintf("walcast_bench_pub_%d", suffix),
		PublicationTables: []string{"public." + e.table}, StateSchema: "walcast_state",
		FeedbackInterval: time.Second, ServerTimeout: time.Minute,
		BatchMaxBytes: 64 << 10, BatchLinger: 5 * time.Millisecond, InflightMaxBytes: 64 << 20,
		ReconnectMinDelay: 50 * time.Millisecond, ReconnectMaxDelay: time.Second,
		BackfillChunkRows: 2000, BackfillChunkBytes: 4 << 20,
	}
	if tune != nil {
		tune(&e.cfg)
	}
	e.exec(b, fmt.Sprintf("CREATE TABLE %s (id bigint PRIMARY KEY, name text, email text, active boolean, balance numeric, created timestamptz)", e.table))
	b.Cleanup(func() {
		for _, sql := range []string{
			fmt.Sprintf("SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name = '%s' AND active", e.cfg.SlotName),
			fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", e.cfg.PublicationName),
			fmt.Sprintf("DROP TABLE IF EXISTS %s", e.table),
		} {
			_, _ = admin.Exec(context.Background(), sql).ReadAll()
		}
		for range 50 {
			if _, err := admin.Exec(context.Background(), fmt.Sprintf(
				"SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = '%s'", e.cfg.SlotName)).ReadAll(); err == nil {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		_ = admin.Close(context.Background())
	})
	return e
}

func (e *benchEnv) exec(b *testing.B, sql string) {
	b.Helper()
	if _, err := e.admin.Exec(context.Background(), sql).ReadAll(); err != nil {
		b.Fatalf("%s: %v", sql, err)
	}
}

func (e *benchEnv) insert(b *testing.B, from, to int) {
	b.Helper()
	e.exec(b, fmt.Sprintf(`INSERT INTO %s SELECT g, 'user ' || g, 'user' || g || '@example.com', g %% 2 = 0, g * 1.25, now() FROM generate_series(%d, %d) g`, e.table, from, to))
}

func (e *benchEnv) run(b *testing.B, snk *countingSink) (stop func()) {
	b.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	finished := make(chan error, 1)
	go func() {
		_, err := replication.NewRunner(e.cfg, snk, zerolog.Nop()).Run(ctx)
		finished <- err
	}()
	return func() {
		cancel()
		if err := <-finished; err != nil {
			b.Fatalf("Run: %v", err)
		}
	}
}

func await(b *testing.B, snk *countingSink, want int64) {
	b.Helper()
	deadline := time.Now().Add(5 * time.Minute)
	for snk.events.Load() < want {
		if time.Now().After(deadline) {
			b.Fatalf("got %d of %d events", snk.events.Load(), want)
		}
		time.Sleep(time.Millisecond)
	}
}

// Streaming: rows are committed while walcast is connected, in transactions of 1000 rows.
func BenchmarkEndToEnd_Stream(b *testing.B) {
	const rows, perTx = 200_000, 1000
	e, snk := newBenchEnv(b, nil), &countingSink{}
	stop := e.run(b, snk)
	defer stop()
	for warm := -1; snk.events.Load() == 0; warm-- {
		e.insert(b, warm, warm)
		time.Sleep(50 * time.Millisecond)
	}

	for b.Loop() {
		base := int(snk.events.Load())
		start, bytes := time.Now(), snk.bytes.Load()
		for from := 1; from <= rows; from += perTx {
			e.insert(b, base*10+from, base*10+from+perTx-1)
		}
		await(b, snk, int64(base+rows))
		elapsed := time.Since(start).Seconds()
		b.ReportMetric(rows/elapsed, "events/s")
		b.ReportMetric(float64(snk.bytes.Load()-bytes)/elapsed/(1<<20), "MB/s")
	}
}

// Catch-up: the rows are already in the WAL when walcast connects, so Postgres never waits for a
// writer and the number shows what the decode and encode path can take.
func BenchmarkEndToEnd_CatchUp(b *testing.B) {
	const rows = 200_000
	e, snk := newBenchEnv(b, nil), &countingSink{}
	// The slot has to exist before the rows are written, or they are not in its WAL.
	stop := e.run(b, snk)
	for warm := -1; snk.events.Load() == 0; warm-- {
		e.insert(b, warm, warm)
		time.Sleep(50 * time.Millisecond)
	}
	stop()
	for b.Loop() {
		b.StopTimer()
		base := int(snk.events.Load())
		e.insert(b, base*10+1, base*10+rows)
		b.StartTimer()
		start, bytes := time.Now(), snk.bytes.Load()
		stop := e.run(b, snk)
		await(b, snk, int64(base+rows))
		elapsed := time.Since(start).Seconds()
		stop()
		b.ReportMetric(rows/elapsed, "events/s")
		b.ReportMetric(float64(snk.bytes.Load()-bytes)/elapsed/(1<<20), "MB/s")
	}
}

func BenchmarkEndToEnd_Backfill(b *testing.B) {
	const rows = 200_000
	for _, chunk := range []int{500, 2000, 10000} {
		b.Run(fmt.Sprintf("chunk_rows=%d", chunk), func(b *testing.B) {
			for b.Loop() {
				b.StopTimer()
				e := newBenchEnv(b, func(c *config.Config) {
					c.BackfillTables = []string{c.PublicationTables[0]}
					c.BackfillChunkRows = chunk
				})
				snk := &countingSink{}
				e.insert(b, 1, rows)
				b.StartTimer()
				start := time.Now()
				stop := e.run(b, snk)
				await(b, snk, rows)
				elapsed := time.Since(start).Seconds()
				stop()
				b.ReportMetric(rows/elapsed, "rows/s")
				b.ReportMetric(float64(snk.bytes.Load())/elapsed/(1<<20), "MB/s")
			}
		})
	}
}
