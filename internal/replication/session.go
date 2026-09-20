package replication

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/rs/zerolog"

	"github.com/aymaneallaoui/walcast/internal/config"
	"github.com/aymaneallaoui/walcast/internal/event"
	"github.com/aymaneallaoui/walcast/internal/ledger"
	"github.com/aymaneallaoui/walcast/internal/sink"
)

const (
	queueCap           = 256
	maxInflightBatches = 1024
	closeTimeout       = 5 * time.Second
)

var ErrServerSilent = errors.New("replication: server went silent")

// Runner runs replication sessions one at a time against a single sink.
type Runner struct {
	cfg  config.Config
	sink sink.Sink
	log  zerolog.Logger

	established  bool
	lastDispatch <-chan struct{}
}

func NewRunner(cfg config.Config, snk sink.Sink, log zerolog.Logger) *Runner {
	return &Runner{cfg: cfg, sink: snk, log: log}
}

// Run streams one session until ctx is cancelled (nil error) or the session fails. It returns
// the highest LSN confirmed durable; the slot, not this value, decides where streaming resumes.
func (r *Runner) Run(ctx context.Context) (pglogrepl.LSN, error) {
	if err := r.cfg.Validate(); err != nil {
		return 0, err
	}
	if err := r.awaitLastDispatcher(ctx); err != nil {
		return 0, err
	}

	conn, err := connect(ctx, r.cfg.DatabaseURL)
	if err != nil {
		return 0, err
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), closeTimeout)
		defer cancel()
		_ = conn.Close(closeCtx)
	}()

	if err := ensurePublication(ctx, conn, r.log, r.cfg.PublicationName, r.cfg.PublicationTables); err != nil {
		return 0, err
	}
	if err := ensureSlot(ctx, conn, r.log, r.cfg.SlotName, !r.established); err != nil {
		return 0, err
	}
	r.established = true
	if err := startReplication(ctx, conn, r.cfg.SlotName, r.cfg.PublicationName); err != nil {
		return 0, err
	}
	r.log.Info().Str("slot", r.cfg.SlotName).Msg("replication started")

	s := newSession(r.cfg, r.log, pgStream{conn})
	r.lastDispatch = s.dispatched
	return s.run(ctx, r.sink)
}

// awaitLastDispatcher keeps two sessions from using the sink concurrently when the previous
// one still has a Send or a done callback outstanding past its drain deadline.
func (r *Runner) awaitLastDispatcher(ctx context.Context) error {
	if r.lastDispatch == nil {
		return nil
	}
	select {
	case <-r.lastDispatch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type session struct {
	cfg    config.Config
	log    zerolog.Logger
	stream stream
	ledger *ledger.Ledger
	enc    *event.Encoder

	queue      chan *event.Batch
	dispatched chan struct{}

	cur           *event.Batch
	pending       *event.Batch
	inTx          bool
	lingerAt      time.Time
	nextFeedback  time.Time
	lastServerMsg time.Time
}

func newSession(cfg config.Config, log zerolog.Logger, st stream) *session {
	now := time.Now()
	return &session{
		cfg:           cfg,
		log:           log,
		stream:        st,
		ledger:        ledger.New(0),
		enc:           event.NewEncoder(),
		queue:         make(chan *event.Batch, queueCap),
		dispatched:    make(chan struct{}),
		cur:           event.NewBatch(),
		nextFeedback:  now.Add(cfg.FeedbackInterval),
		lastServerMsg: now,
	}
}

func (s *session) run(ctx context.Context, snk sink.Sink) (pglogrepl.LSN, error) {
	sinkCtx, cancelSink := context.WithCancel(context.Background())
	defer cancelSink()
	loopCtx, stopLoop := context.WithCancel(ctx)
	defer stopLoop()
	go s.dispatch(sinkCtx, snk, stopLoop)

	err := s.loop(loopCtx)
	if err != nil {
		s.ledger.Fail(err)
	}
	if !s.drain(err == nil) {
		cancelSink()
		s.log.Warn().Msg("drain timed out, unacked events will replay")
	}

	if statusErr := s.sendStatus(); statusErr != nil && err == nil {
		s.log.Warn().Err(statusErr).Msg("final status update failed")
	}
	if err == nil {
		err = s.ledger.Err()
	}
	return s.ledger.Flushed(), err
}

// dispatch closes dispatched only once every done callback has fired, not merely when the
// queue is empty: an asynchronous sink returns from Send long before delivery settles.
// A failed delivery calls stopLoop so the owner goroutine leaves its blocking receive at once
// instead of noticing the failure only at the next feedback deadline.
func (s *session) dispatch(ctx context.Context, snk sink.Sink, stopLoop context.CancelFunc) {
	var outstanding sync.WaitGroup
	defer func() {
		outstanding.Wait()
		close(s.dispatched)
	}()

	for b := range s.queue {
		if s.ledger.Err() != nil || ctx.Err() != nil {
			b.Release()
			continue
		}
		outstanding.Add(1)
		snk.Send(ctx, b, func(err error) {
			defer outstanding.Done()
			if err != nil {
				s.ledger.Fail(fmt.Errorf("deliver batch: %w", err))
				stopLoop()
			} else {
				s.ledger.Done(b.Seq)
			}
			b.Release()
		})
	}
}

func (s *session) loop(ctx context.Context) error {
	for {
		if err := s.ledger.Err(); err != nil {
			return err
		}
		if ctx.Err() != nil {
			return nil
		}
		if time.Since(s.lastServerMsg) > s.cfg.ServerTimeout {
			return fmt.Errorf("%w for %s", ErrServerSilent, s.cfg.ServerTimeout)
		}
		if !time.Now().Before(s.nextFeedback) {
			if err := s.sendStatus(); err != nil {
				return fmt.Errorf("send status: %w", err)
			}
		}
		if s.pending != nil {
			s.enqueue(ctx)
			continue
		}

		deadline := s.nextFeedback
		if s.sealable() && s.lingerAt.Before(deadline) {
			deadline = s.lingerAt
		}
		recvCtx, cancel := context.WithDeadline(ctx, deadline)
		msg, err := s.stream.Receive(recvCtx)
		cancel()
		if err != nil {
			if ledgerErr := s.ledger.Err(); ledgerErr != nil {
				return ledgerErr
			}
			if ctx.Err() != nil {
				return nil
			}
			if !pgconn.Timeout(err) && !errors.Is(err, context.DeadlineExceeded) {
				return fmt.Errorf("receive: %w", err)
			}
		} else if err := s.handle(msg); err != nil {
			return err
		}

		if s.sealable() && !time.Now().Before(s.lingerAt) {
			s.seal()
		}
	}
}

// enqueue never blocks past the next feedback time, so a stalled sink cannot starve
// status updates and trip wal_sender_timeout.
func (s *session) enqueue(ctx context.Context) {
	var queue chan<- *event.Batch
	if s.admits(s.pending) {
		queue = s.queue
	}

	timer := time.NewTimer(time.Until(s.nextFeedback))
	defer timer.Stop()

	select {
	case queue <- s.pending:
		s.pending = nil
	case <-s.ledger.Notify():
	case <-timer.C:
	case <-ctx.Done():
	}
}

func (s *session) admits(b *event.Batch) bool {
	othersBytes := s.ledger.Bytes() - int64(len(b.Buf))
	return othersBytes < s.cfg.InflightMaxBytes && s.ledger.Depth() <= maxInflightBatches
}

func (s *session) handle(msg pgproto3.BackendMessage) error {
	s.lastServerMsg = time.Now()
	switch m := msg.(type) {
	case *pgproto3.ErrorResponse:
		return pgconn.ErrorResponseToPgError(m)
	case *pgproto3.CopyData:
		if len(m.Data) == 0 {
			return nil
		}
		switch m.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			return s.handleKeepalive(m.Data[1:])
		case pglogrepl.XLogDataByteID:
			return s.handleXLogData(m.Data[1:])
		}
	}
	return nil
}

func (s *session) handleKeepalive(data []byte) error {
	ka, err := pglogrepl.ParsePrimaryKeepaliveMessage(data)
	if err != nil {
		return fmt.Errorf("parse keepalive: %w", err)
	}
	if !s.inTx && !s.cur.Dirty() {
		s.ledger.AdvanceIdle(ka.ServerWALEnd)
	}
	if ka.ReplyRequested {
		s.nextFeedback = time.Time{}
	}
	return nil
}

func (s *session) handleXLogData(data []byte) error {
	xld, err := pglogrepl.ParseXLogData(data)
	if err != nil {
		return fmt.Errorf("parse xlogdata: %w", err)
	}
	msg, err := pglogrepl.Parse(xld.WALData)
	if err != nil {
		return fmt.Errorf("parse pgoutput message: %w", err)
	}

	wasDirty := s.cur.Dirty()
	switch m := msg.(type) {
	case *pglogrepl.RelationMessage:
		s.enc.Relation(m)
	case *pglogrepl.BeginMessage:
		s.enc.Begin(m)
		s.inTx = true
	case *pglogrepl.InsertMessage:
		err = s.enc.Insert(s.cur, xld.WALStart, m)
	case *pglogrepl.UpdateMessage:
		err = s.enc.Update(s.cur, xld.WALStart, m)
	case *pglogrepl.DeleteMessage:
		err = s.enc.Delete(s.cur, xld.WALStart, m)
	case *pglogrepl.TruncateMessage:
		err = s.enc.Truncate(s.cur, xld.WALStart, m)
	case *pglogrepl.CommitMessage:
		s.inTx = false
		if s.cur.Dirty() || !s.ledger.AdvanceIdle(m.TransactionEndLSN) {
			s.cur.AckLSN = m.TransactionEndLSN
		}
	}
	if err != nil {
		return fmt.Errorf("encode event at %s: %w", xld.WALStart, err)
	}

	if !wasDirty && s.cur.Dirty() {
		s.lingerAt = time.Now().Add(s.cfg.BatchLinger)
	}
	if len(s.cur.Buf) >= s.cfg.BatchMaxBytes {
		s.seal()
	}
	return nil
}

func (s *session) sealable() bool {
	return !s.inTx && s.pending == nil && s.cur.Dirty()
}

func (s *session) seal() {
	s.cur.Seq = s.ledger.Add(s.cur.AckLSN, len(s.cur.Buf))
	s.pending = s.cur
	s.cur = event.NewBatch()
}

// drain reports whether everything handed to the sink settled before ShutdownTimeout. With
// flush set it first queues what is already decoded and keeps feedback flowing while it waits.
func (s *session) drain(flush bool) bool {
	ctx, cancel := context.WithTimeout(context.Background(), s.cfg.ShutdownTimeout)
	defer cancel()
	ticker := time.NewTicker(s.cfg.FeedbackInterval)
	defer ticker.Stop()

	for flush && ctx.Err() == nil {
		if s.pending == nil {
			if !s.sealable() {
				break
			}
			s.seal()
		}
		select {
		case s.queue <- s.pending:
			s.pending = nil
		case <-ticker.C:
			_ = s.sendStatus()
		case <-ctx.Done():
		}
	}
	close(s.queue)

	for {
		select {
		case <-s.dispatched:
			return true
		case <-ticker.C:
			if flush {
				_ = s.sendStatus()
			}
		case <-ctx.Done():
			return false
		}
	}
}

func (s *session) sendStatus() error {
	silent := time.Since(s.lastServerMsg) > s.cfg.ServerTimeout/2
	err := s.stream.SendStatus(s.ledger.Flushed(), silent, min(s.cfg.FeedbackInterval, closeTimeout))
	s.nextFeedback = time.Now().Add(s.cfg.FeedbackInterval)
	return err
}
