package replication

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/rs/zerolog"

	"github.com/aymaneallaoui/walcast/internal/config"
	"github.com/aymaneallaoui/walcast/internal/event"
)

const testTimeout = 5 * time.Second

func testConfig() config.Config {
	return config.Config{
		ShutdownTimeout:  2 * time.Second,
		FeedbackInterval: 10 * time.Millisecond,
		ServerTimeout:    time.Minute,
		BatchMaxBytes:    64 << 10,
		BatchLinger:      time.Millisecond,
		InflightMaxBytes: 64 << 20,
	}
}

type fakeStream struct {
	msgs chan pgproto3.BackendMessage

	mu       sync.Mutex
	statuses []pglogrepl.LSN
}

func newFakeStream(msgs ...pgproto3.BackendMessage) *fakeStream {
	f := &fakeStream{msgs: make(chan pgproto3.BackendMessage, 64)}
	for _, m := range msgs {
		f.msgs <- m
	}
	return f
}

func (f *fakeStream) Receive(ctx context.Context) (pgproto3.BackendMessage, error) {
	select {
	case m := <-f.msgs:
		return m, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (f *fakeStream) SendStatus(flushed pglogrepl.LSN, _ bool, _ time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.statuses = append(f.statuses, flushed)
	return nil
}

func (f *fakeStream) reported() []pglogrepl.LSN {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]pglogrepl.LSN(nil), f.statuses...)
}

type recordingSink struct {
	mu      sync.Mutex
	payload []byte
	acks    []pglogrepl.LSN
	hold    chan struct{}
	err     error
}

func (r *recordingSink) Send(ctx context.Context, b *event.Batch, done func(error)) {
	if r.hold != nil {
		select {
		case <-r.hold:
		case <-ctx.Done():
			done(ctx.Err())
			return
		}
	}
	r.mu.Lock()
	r.payload = append(r.payload, b.Buf...)
	r.acks = append(r.acks, b.AckLSN)
	r.mu.Unlock()
	done(r.err)
}

func (r *recordingSink) Close() error { return nil }

func (r *recordingSink) sent() (string, []pglogrepl.LSN) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return string(r.payload), append([]pglogrepl.LSN(nil), r.acks...)
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(testTimeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func startSession(t *testing.T, cfg config.Config, st stream, snk *recordingSink) (cancel func(), result func() (pglogrepl.LSN, error)) {
	t.Helper()
	ctx, stop := context.WithCancel(context.Background())
	type outcome struct {
		lsn pglogrepl.LSN
		err error
	}
	finished := make(chan outcome, 1)
	go func() {
		lsn, err := newSession(cfg, zerolog.Nop(), st).run(ctx, snk)
		finished <- outcome{lsn, err}
	}()
	t.Cleanup(stop)

	return stop, func() (pglogrepl.LSN, error) {
		t.Helper()
		select {
		case o := <-finished:
			return o.lsn, o.err
		case <-time.After(testTimeout):
			t.Fatal("session did not finish")
			return 0, nil
		}
	}
}

func TestSession_handleXLogData(t *testing.T) {
	t.Run("commit on a dirty batch sets its ack", func(t *testing.T) {
		s := newSession(testConfig(), zerolog.Nop(), newFakeStream())
		for _, payload := range [][]byte{relationMsg(), beginMsg(90, 7), insertMsg("1", "a"), commitMsg(90, 100)} {
			if err := s.handleXLogData(xlogData(50, payload).Data[1:]); err != nil {
				t.Fatal(err)
			}
		}
		if s.inTx || s.cur.AckLSN != 100 || s.cur.Events != 1 || !s.sealable() {
			t.Fatalf("inTx=%v ack=%s events=%d sealable=%v", s.inTx, s.cur.AckLSN, s.cur.Events, s.sealable())
		}
	})

	t.Run("fragment sealed inside a transaction carries no ack", func(t *testing.T) {
		cfg := testConfig()
		cfg.BatchMaxBytes = 1
		s := newSession(cfg, zerolog.Nop(), newFakeStream())
		for _, payload := range [][]byte{relationMsg(), beginMsg(90, 7), insertMsg("1", "a")} {
			if err := s.handleXLogData(xlogData(50, payload).Data[1:]); err != nil {
				t.Fatal(err)
			}
		}
		if s.pending == nil || s.pending.AckLSN != 0 || s.ledger.Depth() != 1 {
			t.Fatalf("pending=%v depth=%d", s.pending, s.ledger.Depth())
		}
	})

	t.Run("empty transaction advances the idle ledger directly", func(t *testing.T) {
		s := newSession(testConfig(), zerolog.Nop(), newFakeStream())
		for _, payload := range [][]byte{beginMsg(90, 7), commitMsg(90, 100)} {
			if err := s.handleXLogData(xlogData(50, payload).Data[1:]); err != nil {
				t.Fatal(err)
			}
		}
		if s.ledger.Flushed() != 100 || s.cur.Dirty() {
			t.Fatalf("flushed=%s dirty=%v", s.ledger.Flushed(), s.cur.Dirty())
		}
	})

	t.Run("empty transaction behind in-flight work rides a batch instead", func(t *testing.T) {
		s := newSession(testConfig(), zerolog.Nop(), newFakeStream())
		s.ledger.Add(80, 1)
		for _, payload := range [][]byte{beginMsg(90, 7), commitMsg(90, 100)} {
			if err := s.handleXLogData(xlogData(50, payload).Data[1:]); err != nil {
				t.Fatal(err)
			}
		}
		if s.ledger.Flushed() != 0 || s.cur.AckLSN != 100 {
			t.Fatalf("flushed=%s ack=%s", s.ledger.Flushed(), s.cur.AckLSN)
		}
	})

	t.Run("change for an unknown relation is unencodable", func(t *testing.T) {
		s := newSession(testConfig(), zerolog.Nop(), newFakeStream())
		err := s.handleXLogData(xlogData(50, insertMsg("1", "a")).Data[1:])
		if !errors.Is(err, event.ErrUnencodable) {
			t.Fatalf("err = %v, want ErrUnencodable", err)
		}
	})
}

func TestSession_handleKeepalive(t *testing.T) {
	t.Run("advances only outside a transaction with a clean batch", func(t *testing.T) {
		s := newSession(testConfig(), zerolog.Nop(), newFakeStream())
		s.inTx = true
		if err := s.handleKeepalive(keepalive(500, false).Data[1:]); err != nil {
			t.Fatal(err)
		}
		if got := s.ledger.Flushed(); got != 0 {
			t.Fatalf("flushed = %s inside a transaction", got)
		}

		s.inTx = false
		if err := s.handleKeepalive(keepalive(500, false).Data[1:]); err != nil {
			t.Fatal(err)
		}
		if got := s.ledger.Flushed(); got != 500 {
			t.Fatalf("flushed = %s, want 0/1F4", got)
		}
	})

	t.Run("reply request forces immediate feedback", func(t *testing.T) {
		s := newSession(testConfig(), zerolog.Nop(), newFakeStream())
		if err := s.handleKeepalive(keepalive(1, true).Data[1:]); err != nil {
			t.Fatal(err)
		}
		if !s.nextFeedback.IsZero() {
			t.Fatal("nextFeedback not reset")
		}
	})
}

func TestSession_run(t *testing.T) {
	committedInsert := func() []pgproto3.BackendMessage {
		return []pgproto3.BackendMessage{
			xlogData(10, relationMsg()), xlogData(20, beginMsg(90, 7)),
			xlogData(30, insertMsg("1", "a")), xlogData(40, commitMsg(90, 100)),
		}
	}

	t.Run("reports an LSN only after the sink delivered it", func(t *testing.T) {
		snk := &recordingSink{hold: make(chan struct{})}
		st := newFakeStream(committedInsert()...)
		cancel, result := startSession(t, testConfig(), st, snk)

		eventually(t, "feedback while the sink is blocked", func() bool { return len(st.reported()) >= 3 })
		for _, lsn := range st.reported() {
			if lsn != 0 {
				t.Fatalf("reported %s before delivery", lsn)
			}
		}

		close(snk.hold)
		eventually(t, "ack after delivery", func() bool {
			r := st.reported()
			return len(r) > 0 && r[len(r)-1] == 100
		})

		cancel()
		if lsn, err := result(); err != nil || lsn != 100 {
			t.Fatalf("lsn=%s err=%v", lsn, err)
		}
		if payload, acks := snk.sent(); payload == "" || len(acks) != 1 || acks[0] != 100 {
			t.Fatalf("payload=%q acks=%v", payload, acks)
		}
	})

	t.Run("shutdown flushes the decoded batch before returning", func(t *testing.T) {
		cfg := testConfig()
		cfg.BatchLinger = time.Hour
		snk := &recordingSink{}
		st := newFakeStream(committedInsert()...)
		cancel, result := startSession(t, cfg, st, snk)

		eventually(t, "messages consumed", func() bool { return len(st.msgs) == 0 })
		time.Sleep(5 * time.Millisecond)
		cancel()

		if lsn, err := result(); err != nil || lsn != 100 {
			t.Fatalf("lsn=%s err=%v", lsn, err)
		}
		if r := st.reported(); r[len(r)-1] != 100 {
			t.Fatalf("final status = %s, want 0/64", r[len(r)-1])
		}
	})

	t.Run("sink failure ends the session without acking", func(t *testing.T) {
		snk := &recordingSink{err: errors.New("disk full")}
		st := newFakeStream(committedInsert()...)
		_, result := startSession(t, testConfig(), st, snk)

		lsn, err := result()
		if err == nil || lsn != 0 {
			t.Fatalf("lsn=%s err=%v, want an error and no ack", lsn, err)
		}
	})

	t.Run("silent server ends the session", func(t *testing.T) {
		cfg := testConfig()
		cfg.ServerTimeout = 30 * time.Millisecond
		_, result := startSession(t, cfg, newFakeStream(), &recordingSink{})

		if _, err := result(); !errors.Is(err, ErrServerSilent) {
			t.Fatalf("err = %v, want ErrServerSilent", err)
		}
	})

	t.Run("drain gives up on a stuck sink and cancels it", func(t *testing.T) {
		cfg := testConfig()
		cfg.ShutdownTimeout = 50 * time.Millisecond
		snk := &recordingSink{hold: make(chan struct{})}
		st := newFakeStream(committedInsert()...)
		cancel, result := startSession(t, cfg, st, snk)

		eventually(t, "feedback while blocked", func() bool { return len(st.reported()) >= 1 })
		cancel()

		if lsn, _ := result(); lsn != 0 {
			t.Fatalf("lsn = %s, nothing was delivered", lsn)
		}
	})
}

func BenchmarkSession_handleXLogData(b *testing.B) {
	cfg := testConfig()
	cfg.BatchMaxBytes = 1 << 30
	s := newSession(cfg, zerolog.Nop(), newFakeStream())
	for _, payload := range [][]byte{relationMsg(), beginMsg(90, 7)} {
		if err := s.handleXLogData(xlogData(50, payload).Data[1:]); err != nil {
			b.Fatal(err)
		}
	}
	data := xlogData(60, insertMsg("7", "aymane")).Data[1:]

	b.ReportAllocs()
	for b.Loop() {
		s.cur.Buf = s.cur.Buf[:0]
		if err := s.handleXLogData(data); err != nil {
			b.Fatal(err)
		}
	}
}
