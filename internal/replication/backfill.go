package replication

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/rs/zerolog"

	"github.com/aymaneallaoui/walcast/internal/config"
	"github.com/aymaneallaoui/walcast/internal/event"
)

const (
	markerPrefix = "walcast"

	markerTrack = "track"
	markerChunk = "chunk"
	markerStop  = "stop"

	backfillRunning = "running"
	backfillDone    = "done"

	gatePollInterval = time.Second
	// A chunk that has not come back by then is abandoned with its session: xid widening assumes a
	// chunk never lives long enough for the xid counter to move half its range.
	chunkMaxAge = 10 * time.Minute
	// maxQueryParams is what a Bind message can carry: its parameter count is a 16-bit integer.
	maxQueryParams = 65535
)

// ErrBackfillRefused is fatal: the table or publication cannot be backfilled correctly as configured.
var ErrBackfillRefused = errors.New("replication: backfill refused")

// marker travels through the WAL as a logical message, so the owner goroutine meets it at an exact
// position in the stream. The session nonce makes a marker replayed after a reconnect recognisable.
type marker struct {
	Session    string `json:"s"`
	Kind       string `json:"k"`
	Generation uint64 `json:"g"`
	Chunk      uint64 `json:"c,omitempty"`
	Table      string `json:"t,omitempty"`
	Ref        uint64 `json:"r,omitempty"`
}

type chunk struct {
	generation uint64
	number     uint64
	tableName  string
	tableOID   uint32
	table      *event.Table
	xmin       uint64
	ts         []byte
	rows       [][][]byte
	keys       [][]string
	last       []string
}

type chunkResult struct {
	generation uint64
	number     uint64
	emitted    int
	retry      [][]string
	moves      []keyMove
	invalid    bool
}

// backfillLink is the only state shared between the worker and the owner goroutine. A chunk is
// registered here before its marker is emitted, so the marker can never be decoded ahead of its rows.
type backfillLink struct {
	session string

	mu      sync.Mutex
	pending *chunk

	tracked chan uint64
	results chan chunkResult
}

func newBackfillLink() (*backfillLink, error) {
	nonce := make([]byte, 8)
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("backfill session nonce: %w", err)
	}
	return &backfillLink{
		session: hex.EncodeToString(nonce),
		tracked: make(chan uint64, 1),
		results: make(chan chunkResult, 1),
	}, nil
}

func (l *backfillLink) register(c *chunk) {
	l.mu.Lock()
	l.pending = c
	l.mu.Unlock()
}

func (l *backfillLink) take(generation, number uint64) *chunk {
	l.mu.Lock()
	defer l.mu.Unlock()
	c := l.pending
	if c == nil || c.generation != generation || c.number != number {
		return nil
	}
	l.pending = nil
	return c
}

type backfillTable struct {
	schema, name string
	oid          uint32
	columns      []event.Column
	keyColumns   []string
	desc         *event.Table
}

func (t backfillTable) qualified() string { return t.schema + "." + t.name }

type backfillWorker struct {
	cfg           config.Config
	log           zerolog.Logger
	link          *backfillLink
	conn          *pgconn.PgConn
	systemID      string
	generation    uint64
	chunkRows     int
	names         []string
	serverVersion uint64
}

func runBackfill(ctx context.Context, cfg config.Config, log zerolog.Logger, link *backfillLink, systemID string) error {
	conn, err := connectSQL(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("backfill: %w", err)
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), closeTimeout)
		defer cancel()
		_ = conn.Close(closeCtx)
	}()

	w := &backfillWorker{cfg: cfg, log: log, link: link, conn: conn, systemID: systemID, chunkRows: cfg.BackfillChunkRows}
	if err := w.run(ctx); err != nil && ctx.Err() == nil {
		return fmt.Errorf("backfill: %w", err)
	}
	return nil
}

func (w *backfillWorker) run(ctx context.Context) error {
	if err := w.checkSource(ctx); err != nil {
		return err
	}
	if err := ensureBackfillTable(ctx, w.conn, w.cfg.StateSchema, w.cfg.SlotName); err != nil {
		return err
	}
	slotGeneration, err := w.scalar(ctx, fmt.Sprintf("SELECT generation FROM %s.%s WHERE slot_name = $1", w.cfg.StateSchema, stateTable), w.cfg.SlotName)
	if err != nil {
		return fmt.Errorf("read slot generation: %w", err)
	}
	names, err := w.tableNames(ctx)
	if err != nil {
		return err
	}
	w.names = names

	for _, name := range names {
		table, err := w.describe(ctx, name)
		if err != nil {
			return err
		}
		progress, err := loadBackfill(ctx, w.conn, w.cfg.StateSchema, w.cfg.SlotName, table, slotGeneration)
		if err != nil {
			return err
		}
		if progress.status == backfillDone {
			continue
		}
		if err := w.backfill(ctx, table, progress); err != nil {
			return err
		}
	}
	// Always sent, even when every table was already done: until it is decoded the session holds
	// acks back for key moves, and it has no other way to learn that no backfill is running.
	return w.emit(ctx, marker{Kind: markerStop, Generation: w.generation})
}

// checkSource refuses a standby, where no xid can be taken, and a second connection that landed on
// another server than the stream, which a load-balanced DATABASE_URL would allow.
func (w *backfillWorker) checkSource(ctx context.Context) error {
	rows, err := w.query(ctx, "SELECT pg_is_in_recovery(), (SELECT system_identifier::text FROM pg_control_system())")
	if err != nil {
		return fmt.Errorf("inspect source: %w", err)
	}
	if w.serverVersion, err = w.scalar(ctx, "SELECT current_setting('server_version_num')"); err != nil {
		return fmt.Errorf("inspect source: server version: %w", err)
	}
	if string(rows[0][0]) == "t" {
		return fmt.Errorf("%w: the source is a standby, backfill needs a writable primary", ErrBackfillRefused)
	}
	if got := string(rows[0][1]); w.systemID != "" && got != w.systemID {
		return fmt.Errorf("%w: the SQL connection reached system %s but the stream comes from %s", ErrBackfillRefused, got, w.systemID)
	}
	return nil
}

// tableNames never returns walcast's own state tables: an all-tables publication contains them,
// and the encoder's ignore list only covers the stream, not rows read by a backfill.
func (w *backfillWorker) tableNames(ctx context.Context) ([]string, error) {
	own := []string{w.cfg.StateSchema + "." + stateTable, w.cfg.StateSchema + "." + progressTable}
	if len(w.cfg.BackfillTables) != 1 || w.cfg.BackfillTables[0] != config.BackfillAll {
		names := qualified(w.cfg.BackfillTables)
		for _, name := range names {
			if slices.Contains(own, name) {
				return nil, fmt.Errorf("%w: %s is walcast's own state and is never emitted", ErrBackfillRefused, name)
			}
		}
		return names, nil
	}
	rows, err := w.query(ctx, "SELECT schemaname || '.' || tablename FROM pg_publication_tables WHERE pubname = $1 ORDER BY 1", w.cfg.PublicationName)
	if err != nil {
		return nil, fmt.Errorf("list publication tables: %w", err)
	}
	var names []string
	for _, row := range rows {
		if name := string(row[0]); !slices.Contains(own, name) {
			names = append(names, name)
		}
	}
	return names, nil
}

// describe checks everything the correctness argument rests on: every change must reach the
// stream, the scan must see every row, and both sides must key a row the same way.
func (w *backfillWorker) describe(ctx context.Context, qualifiedName string) (backfillTable, error) {
	schema, name, _ := strings.Cut(qualifiedName, ".")
	refuse := func(format string, args ...any) (backfillTable, error) {
		return backfillTable{}, fmt.Errorf("%w: %s: %s", ErrBackfillRefused, qualifiedName, fmt.Sprintf(format, args...))
	}

	rows, err := w.query(ctx, `
SELECT c.oid::text, c.relkind::text, c.relreplident::text, c.relhassubclass,
       c.relrowsecurity AND NOT (r.rolsuper OR r.rolbypassrls OR (pg_get_userbyid(c.relowner) = current_user AND NOT c.relforcerowsecurity)),
       p.pubinsert AND p.pubupdate AND p.pubdelete AND p.pubtruncate,
       EXISTS (SELECT 1 FROM pg_publication_tables t WHERE t.pubname = p.pubname AND t.schemaname = n.nspname AND t.tablename = c.relname),
       current_setting('server_version_num')::int
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
JOIN pg_roles r ON r.rolname = current_user
CROSS JOIN pg_publication p
WHERE n.nspname = $1 AND c.relname = $2 AND p.pubname = $3`, schema, name, w.cfg.PublicationName)
	if err != nil {
		return backfillTable{}, fmt.Errorf("inspect %s: %w", qualifiedName, err)
	}
	if len(rows) == 0 {
		return refuse("table or publication %s not found", w.cfg.PublicationName)
	}
	row := rows[0]
	switch {
	case string(row[1]) != "r":
		return refuse("only ordinary tables can be backfilled, this one has relkind %q", row[1])
	case string(row[3]) == "t":
		return refuse("it has child tables, and their rows are published under other names")
	case string(row[2]) != "d":
		return refuse("replica identity must be DEFAULT, so the stream and the scan key rows by the same primary key")
	case string(row[4]) == "t":
		return refuse("row-level security hides rows from this role")
	case string(row[5]) != "t":
		return refuse("publication %s must publish insert, update, delete and truncate", w.cfg.PublicationName)
	case string(row[6]) != "t":
		return refuse("it is not part of publication %s", w.cfg.PublicationName)
	}
	oid, err := strconv.ParseUint(string(row[0]), 10, 32)
	if err != nil {
		return backfillTable{}, fmt.Errorf("inspect %s: oid: %w", qualifiedName, err)
	}
	if version, _ := strconv.Atoi(string(row[7])); version >= 150000 {
		filtered, err := w.scalar(ctx, `
SELECT count(*) FROM pg_publication_rel pr JOIN pg_publication p ON p.oid = pr.prpubid
WHERE p.pubname = $1 AND pr.prrelid = $2::oid AND (pr.prqual IS NOT NULL OR pr.prattrs IS NOT NULL)`, w.cfg.PublicationName, string(row[0]))
		if err != nil {
			return backfillTable{}, fmt.Errorf("inspect %s: publication filters: %w", qualifiedName, err)
		}
		if filtered != 0 {
			return refuse("publication %s filters its rows or columns", w.cfg.PublicationName)
		}
	}

	table := backfillTable{schema: schema, name: name, oid: uint32(oid)}
	cols, err := w.query(ctx, `
SELECT a.attname, a.atttypid::text, i.indkey IS NOT NULL AND a.attnum = ANY (i.indkey)
FROM pg_attribute a
LEFT JOIN pg_index i ON i.indrelid = a.attrelid AND i.indisprimary
WHERE a.attrelid = $1::oid AND a.attnum > 0 AND NOT a.attisdropped AND a.attgenerated = ''
ORDER BY a.attnum`, string(row[0]))
	if err != nil {
		return backfillTable{}, fmt.Errorf("inspect %s: columns: %w", qualifiedName, err)
	}
	for _, c := range cols {
		typeOID, err := strconv.ParseUint(string(c[1]), 10, 32)
		if err != nil {
			return backfillTable{}, fmt.Errorf("inspect %s: column type: %w", qualifiedName, err)
		}
		table.columns = append(table.columns, event.Column{Name: string(c[0]), OID: uint32(typeOID), Key: string(c[2]) == "t"})
	}
	keys, err := w.query(ctx, `
SELECT a.attname
FROM pg_index i
CROSS JOIN LATERAL unnest(i.indkey::int2[]) WITH ORDINALITY AS k(attnum, ord)
JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum
WHERE i.indrelid = $1::oid AND i.indisprimary
ORDER BY k.ord`, string(row[0]))
	if err != nil {
		return backfillTable{}, fmt.Errorf("inspect %s: primary key: %w", qualifiedName, err)
	}
	if len(keys) == 0 {
		return refuse("it has no primary key to scan and to key rows by")
	}
	for _, k := range keys {
		table.keyColumns = append(table.keyColumns, string(k[0]))
	}
	table.desc = event.NewTable(schema, name, table.columns)
	return table, nil
}

func (w *backfillWorker) backfill(ctx context.Context, table backfillTable, progress backfillProgress) error {
	if progress.upper == nil {
		upper, err := w.upperKey(ctx, table)
		if err != nil {
			return err
		}
		if upper == nil {
			w.log.Info().Str("table", table.qualified()).Msg("backfill: table is empty")
			return finishBackfill(ctx, w.conn, w.cfg.StateSchema, w.cfg.SlotName, table)
		}
		progress.upper = upper
		if err := saveBackfillUpper(ctx, w.conn, w.cfg.StateSchema, w.cfg.SlotName, table, upper); err != nil {
			return err
		}
	}
	w.log.Info().Str("table", table.qualified()).Int64("rows_so_far", progress.rows).Msg("backfill started")

	fence, err := w.startTracking(ctx, table)
	if err != nil {
		return err
	}
	// Key moves that arrived while another table was being copied were parked on this table's
	// progress row. They are behind the saved cursor or above the upper bound, so they go first.
	if len(progress.pending) > 0 {
		emitted, fresh, err := w.deliver(ctx, table, &chunk{generation: w.generation, tableName: table.qualified(), tableOID: table.oid, table: table.desc}, fence, progress.pending)
		if err != nil {
			return err
		}
		if fresh != 0 {
			return fmt.Errorf("key tracker was reset while reading parked keys of %s", table.qualified())
		}
		progress.rows += int64(emitted)
		if err := clearPendingKeys(ctx, w.conn, w.cfg.StateSchema, w.cfg.SlotName, table); err != nil {
			return err
		}
	}
	for number := uint64(1); ; number++ {
		c, _, err := w.readChunk(ctx, table, number, fence, progress.last, progress.upper, nil)
		if err != nil {
			return err
		}
		last := c.last
		// The empty chunk at the end of the scan still goes through the stream: it collects the
		// key moves seen since the last chunk, which the scan itself will never come back to.
		finished := len(c.rows) == 0
		emitted, fresh, err := w.deliver(ctx, table, c, fence, nil)
		if err != nil {
			return err
		}
		if fresh != 0 {
			fence = fresh
			number--
			continue
		}
		if finished {
			progress.rows += int64(emitted)
			break
		}
		progress.last, progress.rows = last, progress.rows+int64(emitted)
		if err := saveBackfillProgress(ctx, w.conn, w.cfg.StateSchema, w.cfg.SlotName, table, progress); err != nil {
			return err
		}
	}
	w.log.Info().Str("table", table.qualified()).Int64("rows", progress.rows).Msg("backfill finished")
	return finishBackfill(ctx, w.conn, w.cfg.StateSchema, w.cfg.SlotName, table)
}

// deliver hands a chunk to the stream, then reads again every key that came back unresolved: rows
// the stream only patched, and key moves. It returns once each of them has been emitted, superseded
// by a complete stream image, or deleted. A non-zero fence means the tracker was reset and the
// whole chunk must be read again.
func (w *backfillWorker) deliver(ctx context.Context, table backfillTable, c *chunk, fence uint64, queue [][]string) (emitted int, fresh uint64, err error) {
	var (
		batch  = rereadBatch(w.cfg.BackfillChunkRows, len(table.keyColumns))
		number = c.number
	)
	for attempt := 0; ; attempt++ {
		w.link.register(c)
		if err := w.emit(ctx, marker{Kind: markerChunk, Generation: c.generation, Chunk: c.number}); err != nil {
			return 0, 0, err
		}
		result, err := w.awaitResult(ctx, c)
		if err != nil {
			return 0, 0, err
		}
		if result.invalid {
			w.log.Warn().Str("table", table.qualified()).Msg("backfill: key tracker was reset, reading the chunk again")
			fresh, err := w.startTracking(ctx, table)
			return 0, fresh, err
		}
		emitted += result.emitted
		queue = append(queue, result.retry...)
		for _, move := range result.moves {
			if move.table != table.qualified() {
				// Another table's move cannot be read now, its keys are not being tracked. Parking
				// it durably lets the ack move on instead of holding WAL until that table's turn.
				if err := w.park(ctx, move); err != nil {
					return 0, 0, err
				}
				continue
			}
			key, err := keyValues(table.keyColumns, table.qualified(), move.key)
			if err != nil {
				return 0, 0, err
			}
			queue = append(queue, key)
		}
		if attempt > 0 && attempt%50 == 0 {
			w.log.Warn().Str("table", table.qualified()).Int("keys", len(queue)).Int("attempts", attempt).
				Msg("backfill: rows keep changing faster than they can be read, still retrying")
		}

		// A re-read is exact: it asks for a bounded batch of keys and never lets the row limit or
		// the byte budget drop the tail, because a key that silently falls out is a row never sent.
		for c = nil; c == nil && len(queue) > 0; {
			n := min(batch, len(queue))
			next, truncated, err := w.readChunk(ctx, table, number, fence, nil, nil, queue[:n])
			switch {
			case err != nil:
				return 0, 0, err
			case truncated && n > 1:
				batch = n / 2
			case len(next.rows) == 0:
				queue = queue[n:]
			default:
				c, queue = next, queue[n:]
			}
		}
		if c == nil {
			return emitted, 0, nil
		}
	}
}

// rereadBatch keeps an exact re-read, one parameter per key column, under the protocol's limit.
func rereadBatch(rows, keyColumns int) int {
	return max(1, min(rows, maxQueryParams/max(1, keyColumns)))
}

// startTracking asks the owner goroutine, through the stream itself, to start a fresh key tracker,
// and only then takes the fence xid. Every transaction decoded before tracking began was assigned
// its xid earlier, so it is below the fence; snapshot xmax would not do, it ignores hung writers.
func (w *backfillWorker) startTracking(ctx context.Context, table backfillTable) (uint64, error) {
	ref, err := w.scalar(ctx, "SELECT pg_snapshot_xmax(pg_current_snapshot())::text")
	if err != nil {
		return 0, fmt.Errorf("read xid reference: %w", err)
	}
	w.generation++
	if err := w.emit(ctx, marker{Kind: markerTrack, Generation: w.generation, Table: table.qualified(), Ref: ref}); err != nil {
		return 0, err
	}
	timeout := time.NewTimer(chunkMaxAge)
	defer timeout.Stop()
	for {
		select {
		case generation := <-w.link.tracked:
			if generation != w.generation {
				continue
			}
			fence, err := w.scalar(ctx, "SELECT pg_current_xact_id()::text")
			if err != nil {
				return 0, fmt.Errorf("take fence xid: %w", err)
			}
			return fence, nil
		case <-timeout.C:
			return 0, fmt.Errorf("tracking marker for %s was not decoded within %s", table.qualified(), chunkMaxAge)
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
}

func (w *backfillWorker) awaitResult(ctx context.Context, c *chunk) (chunkResult, error) {
	timeout := time.NewTimer(chunkMaxAge)
	defer timeout.Stop()
	for {
		select {
		case result := <-w.link.results:
			if result.generation == c.generation && result.number == c.number {
				return result, nil
			}
		case <-timeout.C:
			return chunkResult{}, fmt.Errorf("chunk %d was not delivered within %s", c.number, chunkMaxAge)
		case <-ctx.Done():
			return chunkResult{}, ctx.Err()
		}
	}
}

// emit flushes the marker too: a walsender only streams flushed WAL, so an unflushed marker waited
// for the WAL writer, 204ms per chunk. Before Postgres 17 a synchronous commit does the flushing.
func (w *backfillWorker) emit(ctx context.Context, m marker) error {
	m.Session = w.link.session
	payload, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("encode marker: %w", err)
	}
	sql := "SELECT pg_logical_emit_message(false, $1, $2, true)"
	if w.serverVersion < 170000 {
		sql = "SELECT set_config('synchronous_commit', 'local', true), pg_logical_emit_message(false, $1, $2), pg_current_xact_id()"
	}
	if _, err := w.query(ctx, sql, markerPrefix, string(payload)); err != nil {
		return fmt.Errorf("emit %s marker: %w", m.Kind, err)
	}
	return nil
}

func (w *backfillWorker) upperKey(ctx context.Context, table backfillTable) ([]string, error) {
	keys := quoteIdents(table.keyColumns)
	desc := make([]string, len(keys))
	for i, k := range keys {
		desc[i] = k + " DESC"
	}
	rows, err := w.query(ctx, fmt.Sprintf("SELECT %s FROM %s ORDER BY %s LIMIT 1",
		strings.Join(keys, ", "), quoteTable(table), strings.Join(desc, ", ")))
	if err != nil {
		return nil, fmt.Errorf("read upper key of %s: %w", table.qualified(), err)
	}
	if len(rows) == 0 {
		return nil, nil
	}
	return toStrings(rows[0]), nil
}

// readChunk reads under one REPEATABLE READ snapshot and waits until that snapshot's xmin has
// passed the fence: below it, a writer that was decoded before tracking began could still be
// invisible, and its key would be missing from the tracker.
func (w *backfillWorker) readChunk(ctx context.Context, table backfillTable, number, fence uint64, after, upper []string, only [][]string) (*chunk, bool, error) {
	warned := false
	for {
		if err := w.exec(ctx, "BEGIN ISOLATION LEVEL REPEATABLE READ, READ ONLY"); err != nil {
			return nil, false, fmt.Errorf("begin chunk snapshot: %w", err)
		}
		xmin, err := w.scalar(ctx, "SELECT pg_snapshot_xmin(pg_current_snapshot())::text")
		if err == nil && xmin >= fence {
			c, truncated, err := w.selectChunk(ctx, table, after, upper, only)
			if rollbackErr := w.exec(ctx, "ROLLBACK"); err == nil {
				err = rollbackErr
			}
			if err != nil {
				return nil, false, err
			}
			c.generation, c.number, c.xmin = w.generation, number, xmin
			return c, truncated, nil
		}
		if rollbackErr := w.exec(ctx, "ROLLBACK"); err == nil {
			err = rollbackErr
		}
		if err != nil {
			return nil, false, fmt.Errorf("chunk snapshot: %w", err)
		}
		if !warned {
			warned = true
			w.log.Warn().Str("table", table.qualified()).Uint64("oldest_xid", xmin).Uint64("needs", fence).
				Msg("backfill paused: a write transaction older than this session is still open")
		}
		select {
		case <-time.After(gatePollInterval):
		case <-ctx.Done():
			return nil, false, ctx.Err()
		}
	}
}

// selectChunk reports truncated when the byte budget made it stop before the query's last row.
func (w *backfillWorker) selectChunk(ctx context.Context, table backfillTable, after, upper []string, only [][]string) (*chunk, bool, error) {
	names := make([]string, len(table.columns))
	for i, c := range table.columns {
		names[i] = c.Name
	}
	keys := quoteIdents(table.keyColumns)
	keyRow := "(" + strings.Join(keys, ", ") + ")"

	var (
		where []string
		args  []string
	)
	placeholders := func(values []string) string {
		marks := make([]string, len(values))
		for i, v := range values {
			args = append(args, v)
			marks[i] = "$" + strconv.Itoa(len(args))
		}
		return "(" + strings.Join(marks, ", ") + ")"
	}
	if after != nil {
		where = append(where, keyRow+" > "+placeholders(after))
	}
	if upper != nil {
		where = append(where, keyRow+" <= "+placeholders(upper))
	}
	if len(only) > 0 {
		tuples := make([]string, len(only))
		for i, key := range only {
			tuples[i] = placeholders(key)
		}
		where = append(where, keyRow+" IN ("+strings.Join(tuples, ", ")+")")
	}
	sql := fmt.Sprintf("SELECT %s, %s FROM %s", strings.Join(quoteIdents(names), ", "), strings.Join(keys, ", "), quoteTable(table))
	if len(where) > 0 {
		sql += " WHERE " + strings.Join(where, " AND ")
	}
	limit := w.chunkRows
	if len(only) > 0 {
		limit = len(only)
	}
	sql += fmt.Sprintf(" ORDER BY %s LIMIT %d", strings.Join(keys, ", "), limit)

	params := make([][]byte, len(args))
	for i, a := range args {
		params[i] = []byte(a)
	}
	reader := w.conn.ExecParams(ctx, sql, params, nil, nil, nil)
	c := &chunk{tableName: table.qualified(), tableOID: table.oid, table: table.desc, ts: time.Now().UTC().AppendFormat(nil, time.RFC3339Nano)}
	var (
		size      int
		truncated bool
	)
	width := len(table.columns)
	for reader.NextRow() {
		if size >= w.cfg.BackfillChunkBytes && len(c.rows) > 0 {
			truncated = true
			continue
		}
		values := reader.Values()
		row := make([][]byte, width)
		for i := range width {
			if values[i] != nil {
				row[i] = append([]byte{}, values[i]...)
				size += len(row[i])
			}
		}
		c.last = toStrings(values[width:])
		c.rows = append(c.rows, row)
		c.keys = append(c.keys, c.last)
	}
	if _, err := reader.Close(); err != nil {
		return nil, false, fmt.Errorf("read chunk of %s: %w", table.qualified(), err)
	}
	// A chunk cut short by the byte budget reads fewer rows next time instead of fetching and
	// discarding the same tail again.
	// One wide row must not slow the rest of the table down for good: the limit shrinks when the
	// budget cuts a chunk short and doubles back towards the configured size when it does not.
	switch {
	case len(only) > 0:
	case truncated:
		w.chunkRows = max(1, len(c.rows))
	default:
		w.chunkRows = min(w.cfg.BackfillChunkRows, w.chunkRows*2)
	}
	return c, truncated, nil
}

// park saves a key move on the progress row of a table that is waiting for its turn. A table with
// no row has not started, and will meet the moved row in its own scan.
func (w *backfillWorker) park(ctx context.Context, move keyMove) error {
	// A table that is no longer configured never gets its turn, so its parked keys would only grow.
	if !slices.Contains(w.names, move.table) {
		return nil
	}
	schema, name, _ := strings.Cut(move.table, ".")
	rows, err := w.query(ctx, `
SELECT a.attname FROM pg_index i
CROSS JOIN LATERAL unnest(i.indkey::int2[]) WITH ORDINALITY AS k(attnum, ord)
JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum
JOIN pg_class c ON c.oid = i.indrelid JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = $1 AND c.relname = $2 AND i.indisprimary ORDER BY k.ord`, schema, name)
	if err != nil {
		return fmt.Errorf("park key move of %s: %w", move.table, err)
	}
	key, err := keyValues(toStrings(flatten(rows)), move.table, move.key)
	if err != nil {
		return err
	}
	return parkPendingKey(ctx, w.conn, w.cfg.StateSchema, w.cfg.SlotName, move.table, key)
}

func flatten(rows [][][]byte) [][]byte {
	out := make([][]byte, len(rows))
	for i, row := range rows {
		out[i] = row[0]
	}
	return out
}

// keyValues turns a key as the encoder renders it back into the text values a query needs.
func keyValues(keyColumns []string, table string, encoded []byte) ([]string, error) {
	var fields map[string]any
	dec := json.NewDecoder(bytes.NewReader(encoded))
	dec.UseNumber()
	if err := dec.Decode(&fields); err != nil {
		return nil, fmt.Errorf("decode key %s of %s: %w", encoded, table, err)
	}
	values := make([]string, len(keyColumns))
	for i, column := range keyColumns {
		switch v := fields[column].(type) {
		case string:
			values[i] = v
		case json.Number:
			values[i] = v.String()
		case bool:
			values[i] = strconv.FormatBool(v)
		default:
			return nil, fmt.Errorf("decode key %s of %s: column %s has no usable value", encoded, table, column)
		}
	}
	return values, nil
}

func (w *backfillWorker) exec(ctx context.Context, sql string) error {
	_, err := w.conn.Exec(ctx, sql).ReadAll()
	return err
}

func (w *backfillWorker) query(ctx context.Context, sql string, args ...string) ([][][]byte, error) {
	return queryParams(ctx, w.conn, sql, args...)
}

func (w *backfillWorker) scalar(ctx context.Context, sql string, args ...string) (uint64, error) {
	rows, err := w.query(ctx, sql, args...)
	if err != nil {
		return 0, err
	}
	if len(rows) != 1 || len(rows[0]) != 1 {
		return 0, fmt.Errorf("%q returned no single value", sql)
	}
	return strconv.ParseUint(string(rows[0][0]), 10, 64)
}

func queryParams(ctx context.Context, conn *pgconn.PgConn, sql string, args ...string) ([][][]byte, error) {
	params := make([][]byte, len(args))
	for i, a := range args {
		params[i] = []byte(a)
	}
	result := conn.ExecParams(ctx, sql, params, nil, nil, nil).Read()
	return result.Rows, result.Err
}

func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

func quoteIdents(names []string) []string {
	out := make([]string, len(names))
	for i, n := range names {
		out[i] = quoteIdent(n)
	}
	return out
}

func quoteTable(t backfillTable) string {
	return quoteIdent(t.schema) + "." + quoteIdent(t.name)
}

func toStrings(values [][]byte) []string {
	out := make([]string, len(values))
	for i, v := range values {
		out[i] = string(v)
	}
	return out
}
