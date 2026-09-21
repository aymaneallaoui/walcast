package replication

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/rs/zerolog"

	"github.com/aymaneallaoui/walcast/internal/config"
	"github.com/aymaneallaoui/walcast/internal/event"
	"github.com/aymaneallaoui/walcast/internal/ledger"
	"github.com/aymaneallaoui/walcast/internal/metrics"
	"github.com/aymaneallaoui/walcast/internal/sink"
)

const (
	queueCap           = 256
	maxInflightBatches = 1024
	closeTimeout       = 5 * time.Second

	// While a chunk waits for its delivery to be noticed, the owner loop must not sit in Receive
	// until the next feedback deadline, or every chunk on a quiet stream would take that long. At
	// 5ms this poll alone was a fifth of a chunk's cycle; it only runs while a chunk is awaited.
	chunkPollInterval = time.Millisecond
)

var (
	ErrServerSilent = errors.New("replication: server went silent")

	// ErrSinkStuck is fatal: only a restart drops a sink client whose deliveries never settle.
	ErrSinkStuck = errors.New("replication: previous session's deliveries never settled")
)

// Runner runs replication sessions one at a time against a single sink.
type Runner struct {
	cfg  config.Config
	sink sink.Sink
	log  zerolog.Logger

	established  bool
	lastDispatch <-chan struct{}

	metrics *metrics.Metrics
	// live is the session whose positions the gauges report. They are registered once and read
	// through it, because every reconnect starts a session with a ledger of its own.
	live atomic.Pointer[session]
}

func NewRunner(cfg config.Config, snk sink.Sink, log zerolog.Logger) *Runner {
	return (&Runner{cfg: cfg, sink: snk, log: log}).WithMetrics(metrics.New())
}

// WithMetrics replaces the private metrics a Runner starts with by the ones the process exposes.
func (r *Runner) WithMetrics(m *metrics.Metrics) *Runner {
	r.metrics = m
	gauge := func(name string, read func(*session) float64) {
		m.Gauge(name, func() float64 {
			if s := r.live.Load(); s != nil {
				return read(s)
			}
			return 0
		})
	}
	gauge("walcast_streaming", func(*session) float64 { return 1 })
	gauge("walcast_inflight_batches", func(s *session) float64 { return float64(s.ledger.Depth()) })
	gauge("walcast_inflight_bytes", func(s *session) float64 { return float64(s.ledger.Bytes()) })
	gauge("walcast_received_lsn", func(s *session) float64 { return float64(s.received.Load()) })
	gauge("walcast_delivered_lsn", func(s *session) float64 { return float64(s.ledger.Delivered()) })
	gauge("walcast_reported_lsn", func(s *session) float64 { return float64(s.reported.Load()) })
	return r
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
	if err := r.ensureSlot(ctx, conn); err != nil {
		return 0, err
	}
	backfilling := len(r.cfg.BackfillTables) > 0
	systemID := ""
	if backfilling {
		sys, err := pglogrepl.IdentifySystem(ctx, conn)
		if err != nil {
			return 0, fmt.Errorf("identify system: %w", err)
		}
		systemID = sys.SystemID
	}
	if err := startReplication(ctx, conn, r.cfg.SlotName, r.cfg.PublicationName, backfilling); err != nil {
		return 0, err
	}
	r.log.Info().Str("slot", r.cfg.SlotName).Msg("replication started")

	s := newSession(r.cfg, r.log, pgStream{conn}, r.metrics)
	if backfilling {
		if s.link, err = newBackfillLink(); err != nil {
			return 0, err
		}
		s.systemID, s.backfilling = systemID, true
	}
	r.lastDispatch = s.dispatched
	r.live.Store(s)
	defer r.live.Store(nil)
	return s.run(ctx, r.sink)
}

func (r *Runner) ensureSlot(ctx context.Context, conn *pgconn.PgConn) error {
	if r.established {
		return requireSlot(ctx, conn, r.cfg.SlotName)
	}
	if err := bootstrapSlot(ctx, conn, r.log, r.cfg); err != nil {
		return err
	}
	r.established = true
	return nil
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
	default:
		r.log.Warn().Dur("timeout", r.cfg.SettleTimeout).Msg("previous session still has deliveries in flight, reconnecting once they settle")
	}

	timer := time.NewTimer(r.cfg.SettleTimeout)
	defer timer.Stop()
	select {
	case <-r.lastDispatch:
		return nil
	case <-timer.C:
		return fmt.Errorf("%w within %s", ErrSinkStuck, r.cfg.SettleTimeout)
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
	txCommitNanos int64
	xid           uint32

	link      *backfillLink
	systemID  string
	tracker   *tracker
	reads     *chunkReads
	awaited   *awaitedChunk
	commitLSN pglogrepl.LSN
	keyMoves  []keyMove
	// backfilling is true from the start of a session with a backfill configured until the worker's
	// stop marker: a key move replayed after a crash is decoded before any tracker exists.
	backfilling bool

	lingerAt      time.Time
	nextFeedback  time.Time
	lastServerMsg time.Time

	// Cold fields stay last, behind everything the receive path touches per message. received feeds
	// a gauge and is published per sealed batch and per keepalive, which is fresh enough for a scrape.
	metrics  *metrics.Metrics
	received atomic.Uint64
	reported atomic.Uint64
	walStart pglogrepl.LSN
}

// chunkReads is a chunk being written into the stream at its marker's position. It spans several
// loop iterations because only one sealed batch can be pending at a time.
type chunkReads struct {
	chunk   *chunk
	lsn     pglogrepl.LSN
	next    int
	emitted int
	retry   [][]string
	key     []byte
	// id names the chunk by its marker's LSN, which no other delivery shares. Counters would not
	// do: they restart with every worker, and a resumed scan would reuse the first chunk's ids.
	id string
}

// keyMove is the insert half of a key-changing update that left out an unchanged TOAST column. The
// consumer can only complete it from the old row, which it may never have been sent, and the scan
// may already be past the new key. So the row is read again, and until then the ack stays before
// its transaction: a crash then replays the move instead of forgetting it.
type keyMove struct {
	table string
	key   []byte
}

type awaitedChunk struct {
	result chunkResult
	lsn    pglogrepl.LSN
}

func newSession(cfg config.Config, log zerolog.Logger, st stream, m *metrics.Metrics) *session {
	now := time.Now()
	enc := event.NewEncoder()
	enc.IgnoreTables(cfg.StateSchema, stateTable, progressTable)
	return &session{
		cfg:           cfg,
		log:           log,
		stream:        st,
		ledger:        ledger.New(0),
		enc:           enc,
		metrics:       m,
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

	var backfill sync.WaitGroup
	if s.link != nil {
		backfill.Go(func() {
			if err := runBackfill(loopCtx, s.cfg, s.log, s.link, s.systemID); err != nil {
				s.ledger.Fail(err)
				stopLoop()
			}
		})
	}

	err := s.loop(loopCtx)
	stopLoop()
	backfill.Wait()
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
	if ledgerErr := s.ledger.Err(); ledgerErr != nil {
		err = ledgerErr
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
		sent := time.Now()
		snk.Send(ctx, b, func(err error) {
			defer outstanding.Done()
			if err != nil {
				s.metrics.DeliveryFailures.Inc()
				s.ledger.Fail(fmt.Errorf("deliver batch: %w", err))
				stopLoop()
			} else {
				s.observeDelivery(b, sent)
				s.ledger.Done(b.Seq)
			}
			b.Release()
		})
	}
}

// observeDelivery runs on the sink's goroutine, once per batch and before the batch goes back to
// the pool. Nothing here is on the path that decodes WAL.
func (s *session) observeDelivery(b *event.Batch, sent time.Time) {
	now := time.Now()
	s.metrics.Delivery.Observe(now.Sub(sent))
	if b.CommitNanos != 0 {
		s.metrics.EndToEnd.Observe(time.Duration(now.UnixNano() - b.CommitNanos))
	}
	s.metrics.BatchesDelivered.Inc()
	bytes := len(b.Buf)
	for _, rec := range b.Records {
		if rec.Skipped {
			bytes -= b.Size(rec)
			s.metrics.EventsSkipped.Inc()
			continue
		}
		if counter := s.metrics.EventsDelivered[rec.Op]; counter != nil {
			counter.Inc()
		}
	}
	s.metrics.BytesDelivered.Add(bytes)
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
		// Neither branch reads the socket, so silence cannot be told from a server that talks: the
		// clock only runs while listening. A dead connection still shows up as a failed status send.
		if s.pending != nil {
			s.enqueue(ctx)
			s.lastServerMsg = time.Now()
			continue
		}
		if s.reads != nil {
			if err := s.emitReads(); err != nil {
				return err
			}
			s.lastServerMsg = time.Now()
			continue
		}
		s.reportChunk()

		deadline := s.nextFeedback
		if s.awaited != nil {
			deadline = time.Now().Add(chunkPollInterval)
		}
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
	s.publishReceived(max(s.walStart, ka.ServerWALEnd))
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
	s.walStart = xld.WALStart

	wasDirty := s.cur.Dirty()
	recorded := len(s.cur.Records)
	switch m := msg.(type) {
	case *pglogrepl.RelationMessage:
		s.enc.Relation(m)
	case *pglogrepl.BeginMessage:
		s.enc.Begin(m)
		s.inTx, s.txCommitNanos = true, m.CommitTime.UnixNano()
		s.xid, s.commitLSN = m.Xid, m.FinalLSN
	case *pglogrepl.LogicalDecodingMessage:
		s.handleMarker(m)
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
	if len(s.cur.Records) > recorded && s.inTx && s.cur.CommitNanos == 0 {
		s.cur.CommitNanos = s.txCommitNanos
	}
	if s.backfilling {
		s.observe(s.cur.Records[recorded:])
	}

	if !wasDirty && s.cur.Dirty() {
		s.lingerAt = time.Now().Add(s.cfg.BatchLinger)
	}
	if len(s.cur.Buf) >= s.cfg.BatchMaxBytes {
		s.seal()
	}
	return nil
}

// handleMarker reacts to the backfill worker's logical messages. They arrive at an exact stream
// position, never inside a transaction, and a marker from another session is ignored.
func (s *session) handleMarker(m *pglogrepl.LogicalDecodingMessage) {
	if s.link == nil || m.Prefix != markerPrefix || m.Transactional {
		return
	}
	var mk marker
	if err := json.Unmarshal(m.Content, &mk); err != nil || mk.Session != s.link.session {
		return
	}

	switch mk.Kind {
	case markerTrack:
		s.tracker = newTracker(mk.Table, mk.Generation, mk.Ref)
		select {
		case <-s.link.tracked:
		default:
		}
		s.link.tracked <- mk.Generation
	case markerStop:
		s.tracker, s.backfilling, s.keyMoves = nil, false, nil
		s.ledger.Release()
	case markerChunk:
		c := s.link.take(mk.Generation, mk.Chunk)
		if c == nil {
			return
		}
		if s.tracker == nil || s.tracker.generation != mk.Generation || s.tracker.overflowed {
			s.awaited = &awaitedChunk{result: chunkResult{generation: mk.Generation, number: mk.Chunk, invalid: true}}
			return
		}
		s.reads = &chunkReads{chunk: c, lsn: m.LSN, id: fmt.Sprintf("%d.%s", c.tableOID, m.LSN)}
	}
}

// emitReads writes chunk rows into the current batch until it is full. The batch that holds the
// last row is acked at the marker's LSN, exactly as a commit acks its transaction.
func (s *session) emitReads() error {
	r, wasDirty := s.reads, s.cur.Dirty()
	for r.next < len(r.chunk.rows) && len(s.cur.Buf) < s.cfg.BatchMaxBytes {
		row := r.chunk.rows[r.next]
		key, err := r.chunk.table.AppendKey(r.key[:0], row)
		if err != nil {
			return fmt.Errorf("backfill row key: %w", err)
		}
		r.key = key
		switch s.tracker.verdict(key, r.chunk.xmin) {
		case rowDrop:
			s.metrics.BackfillDropped.Inc()
		case rowRetry:
			s.metrics.BackfillRetried.Inc()
			r.retry = append(r.retry, r.chunk.keys[r.next])
		case rowEmit:
			if err := s.enc.Read(s.cur, r.lsn, r.chunk.table, row, r.id, uint64(r.next), r.chunk.ts); err != nil {
				return fmt.Errorf("encode backfill row: %w", err)
			}
			r.emitted++
			s.metrics.BackfillRows.Inc()
		}
		r.next++
	}
	if !wasDirty && s.cur.Dirty() {
		s.lingerAt = time.Now().Add(s.cfg.BatchLinger)
	}

	if r.next < len(r.chunk.rows) {
		s.seal()
		return nil
	}
	if s.cur.Dirty() || !s.ledger.AdvanceIdle(r.lsn) {
		s.cur.AckLSN = r.lsn
	}
	// The end of a chunk is a flush point: the worker sends nothing more until this batch is
	// delivered, so lingering for more events would only add the linger to every chunk.
	if s.sealable() {
		s.seal()
	}
	s.tracker.prune(r.chunk.xmin)
	s.metrics.BackfillChunks.Inc()
	s.awaited = &awaitedChunk{lsn: r.lsn, result: chunkResult{
		generation: r.chunk.generation, number: r.chunk.number, emitted: r.emitted, retry: r.retry,
		moves: s.takeKeyMoves(),
	}}
	s.reads = nil
	return nil
}

// reportChunk tells the worker once everything up to the chunk's marker is delivered. Progress is
// saved only after this, so a crash repeats a chunk and never skips one.
func (s *session) reportChunk() {
	a := s.awaited
	if a == nil {
		return
	}
	if !a.result.invalid {
		if !s.inTx && !s.cur.Dirty() {
			s.ledger.AdvanceIdle(a.lsn)
		}
		if s.ledger.Delivered() < a.lsn {
			return
		}
	}
	select {
	case s.link.results <- a.result:
		s.awaited = nil
		// A chunk that came back clean, with no key move seen since, leaves nothing to replay for.
		if !a.result.invalid && len(a.result.retry) == 0 && len(a.result.moves) == 0 && len(s.keyMoves) == 0 {
			s.ledger.Release()
		}
	default:
	}
}

func (s *session) observe(records []event.Record) {
	for _, rec := range records {
		key := s.cur.Key(rec)
		if s.tracker != nil {
			s.tracker.observe(rec, key, s.xid)
		}
		if rec.Op == event.OpInsert && rec.Partial {
			s.keyMoves = append(s.keyMoves, keyMove{table: rec.Table, key: append([]byte{}, key...)})
			s.ledger.Hold(s.commitLSN)
		}
	}
}

// takeKeyMoves hands every move to the worker, which knows what each table needs: read the row
// again now, park the key for a table waiting its turn, or drop it for one that is done.
func (s *session) takeKeyMoves() []keyMove {
	moves := s.keyMoves
	s.keyMoves = nil
	return moves
}

func (s *session) sealable() bool {
	return !s.inTx && s.pending == nil && s.cur.Dirty()
}

func (s *session) seal() {
	s.publishReceived(s.walStart)
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
	flushed := s.ledger.Flushed()
	err := s.stream.SendStatus(flushed, silent, min(s.cfg.FeedbackInterval, closeTimeout))
	s.nextFeedback = time.Now().Add(s.cfg.FeedbackInterval)
	if err == nil {
		s.reported.Store(uint64(flushed))
	}
	return err
}

// publishReceived never moves back: a keepalive can report a position past a batch still lingering.
func (s *session) publishReceived(lsn pglogrepl.LSN) {
	if uint64(lsn) > s.received.Load() {
		s.received.Store(uint64(lsn))
	}
}
