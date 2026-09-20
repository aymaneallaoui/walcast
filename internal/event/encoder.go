package event

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pglogrepl"
)

const (
	OpInsert   = "insert"
	OpUpdate   = "update"
	OpDelete   = "delete"
	OpTruncate = "truncate"

	columnFlagKey       = 1
	replicaIdentityFull = 'f'
)

// ErrUnencodable marks deterministic failures: replaying the same WAL fails the same way.
var ErrUnencodable = errors.New("event: unencodable change")

type column struct {
	key   []byte
	name  []byte
	oid   uint32
	isKey bool
}

type relation struct {
	schema  string
	relname string
	name    string
	table   []byte
	keyed   bool
	cols    []column
}

type Encoder struct {
	rels      map[uint32]*relation
	commitLSN pglogrepl.LSN
	xid       uint32
	seq       uint64
	ts        []byte
	unchanged [][]byte
	recStart  int
	op        string

	ignoreSchema string
	ignoreName   string
	ignored      map[uint32]struct{}
}

func NewEncoder() *Encoder {
	return &Encoder{rels: make(map[uint32]*relation)}
}

// IgnoreTable drops every change to one table: walcast's own state table, which an all-tables
// publication would otherwise stream. It is never a whole schema, so no user table can vanish.
func (e *Encoder) IgnoreTable(schema, name string) {
	e.ignoreSchema, e.ignoreName = schema, name
}

// Relation keeps ignored tables out of rels, so the per-event lookup costs what it did before
// IgnoreTable existed and the ignore check runs only when that lookup misses.
func (e *Encoder) Relation(m *pglogrepl.RelationMessage) {
	if e.ignoreName != "" && m.Namespace == e.ignoreSchema && m.RelationName == e.ignoreName {
		if e.ignored == nil {
			e.ignored = make(map[uint32]struct{})
		}
		e.ignored[m.RelationID] = struct{}{}
		delete(e.rels, m.RelationID)
		return
	}
	delete(e.ignored, m.RelationID)

	name := m.Namespace + "." + m.RelationName
	rel := &relation{
		schema:  m.Namespace,
		relname: m.RelationName,
		name:    name,
		table:   appendString(nil, []byte(name)),
		cols:    make([]column, len(m.Columns)),
	}
	for i, c := range m.Columns {
		name := appendString(nil, []byte(c.Name))
		rel.cols[i] = column{
			key:   append(append([]byte{}, name...), ':'),
			name:  name,
			oid:   c.DataType,
			isKey: c.Flags&columnFlagKey != 0,
		}
		rel.keyed = rel.keyed || rel.cols[i].isKey
	}
	rel.keyed = rel.keyed && m.ReplicaIdentity != replicaIdentityFull
	e.rels[m.RelationID] = rel
}

func (e *Encoder) Begin(m *pglogrepl.BeginMessage) {
	e.commitLSN = m.FinalLSN
	e.xid = m.Xid
	e.seq = 0
	e.ts = m.CommitTime.UTC().AppendFormat(e.ts[:0], time.RFC3339Nano)
}

func (e *Encoder) Insert(b *Batch, lsn pglogrepl.LSN, m *pglogrepl.InsertMessage) error {
	rel, err := e.relation(m.RelationID)
	if rel == nil {
		return err
	}
	e.header(b, rel, OpInsert, "", lsn)
	if err := e.tuple(b, rel, ",\"new\":", m.Tuple, false); err != nil {
		return err
	}
	return e.footer(b, rel, m.Tuple, nil)
}

func (e *Encoder) Update(b *Batch, lsn pglogrepl.LSN, m *pglogrepl.UpdateMessage) error {
	rel, err := e.relation(m.RelationID)
	if rel == nil {
		return err
	}
	if rel.keyed && m.OldTupleType == pglogrepl.UpdateMessageTupleTypeKey && keyChanged(rel, m.OldTuple, m.NewTuple) {
		return e.keyChange(b, rel, lsn, m)
	}
	e.header(b, rel, OpUpdate, "", lsn)
	if m.OldTuple != nil {
		keyOnly := m.OldTupleType == pglogrepl.UpdateMessageTupleTypeKey
		if err := e.tuple(b, rel, ",\"old\":", m.OldTuple, keyOnly); err != nil {
			return err
		}
	}
	if err := e.tuple(b, rel, ",\"new\":", m.NewTuple, false); err != nil {
		return err
	}
	return e.footer(b, rel, m.NewTuple, m.OldTuple)
}

// keyChange turns an update of the row identity into a delete under the old key and an insert
// under the new one, so each key's stream stays self-consistent for consumers partitioned by key.
func (e *Encoder) keyChange(b *Batch, rel *relation, lsn pglogrepl.LSN, m *pglogrepl.UpdateMessage) error {
	e.header(b, rel, OpDelete, OpUpdate, lsn)
	if err := e.tuple(b, rel, ",\"old\":", m.OldTuple, true); err != nil {
		return err
	}
	if err := e.footer(b, rel, m.OldTuple, nil); err != nil {
		return err
	}

	e.header(b, rel, OpInsert, OpUpdate, lsn)
	if err := e.tuple(b, rel, ",\"new\":", m.NewTuple, false); err != nil {
		return err
	}
	return e.footer(b, rel, m.NewTuple, m.OldTuple)
}

func keyChanged(rel *relation, old, updated *pglogrepl.TupleData) bool {
	if old == nil || updated == nil || len(old.Columns) != len(updated.Columns) || len(old.Columns) != len(rel.cols) {
		return false
	}
	for i := range rel.cols {
		was, is := old.Columns[i], updated.Columns[i]
		if rel.cols[i].isKey && is.DataType == pglogrepl.TupleDataTypeText && !bytes.Equal(was.Data, is.Data) {
			return true
		}
	}
	return false
}

func (e *Encoder) Delete(b *Batch, lsn pglogrepl.LSN, m *pglogrepl.DeleteMessage) error {
	rel, err := e.relation(m.RelationID)
	if rel == nil {
		return err
	}
	e.header(b, rel, OpDelete, "", lsn)
	keyOnly := m.OldTupleType == pglogrepl.DeleteMessageTupleTypeKey
	if err := e.tuple(b, rel, ",\"old\":", m.OldTuple, keyOnly); err != nil {
		return err
	}
	return e.footer(b, rel, m.OldTuple, nil)
}

func (e *Encoder) Truncate(b *Batch, lsn pglogrepl.LSN, m *pglogrepl.TruncateMessage) error {
	for _, id := range m.RelationIDs {
		rel, err := e.relation(id)
		if err != nil {
			return err
		}
		if rel == nil {
			continue
		}
		e.header(b, rel, OpTruncate, "", lsn)
		if err := e.footer(b, rel, nil, nil); err != nil {
			return err
		}
	}
	return nil
}

// relation returns nil without an error for an ignored table. The miss path lives in its own
// function so that this one stays within the inlining budget.
func (e *Encoder) relation(id uint32) (*relation, error) {
	if rel, ok := e.rels[id]; ok {
		return rel, nil
	}
	return nil, e.missing(id)
}

func (e *Encoder) missing(id uint32) error {
	if _, ignored := e.ignored[id]; ignored {
		return nil
	}
	return fmt.Errorf("%w: unknown relation id %d", ErrUnencodable, id)
}

func (e *Encoder) header(b *Batch, rel *relation, op, origin string, lsn pglogrepl.LSN) {
	e.recStart = len(b.Buf)
	e.op = op
	buf := b.Buf
	buf = append(buf, "{\"table\":"...)
	buf = append(buf, rel.table...)
	buf = append(buf, ",\"op\":\""...)
	buf = append(buf, op...)
	buf = append(buf, "\",\"lsn\":\""...)
	buf = appendLSN(buf, lsn)
	buf = append(buf, "\",\"commit_lsn\":\""...)
	buf = appendLSN(buf, e.commitLSN)
	buf = append(buf, "\",\"seq\":"...)
	buf = strconv.AppendUint(buf, e.seq, 10)
	buf = append(buf, ",\"txid\":"...)
	buf = strconv.AppendUint(buf, uint64(e.xid), 10)
	buf = append(buf, ",\"ts\":\""...)
	buf = append(buf, e.ts...)
	buf = append(buf, '"')
	if origin != "" {
		buf = append(buf, ",\"origin\":\""...)
		buf = append(buf, origin...)
		buf = append(buf, '"')
	}
	b.Buf = buf
	e.unchanged = e.unchanged[:0]
}

func (e *Encoder) footer(b *Batch, rel *relation, identity, previous *pglogrepl.TupleData) error {
	buf := b.Buf
	if len(e.unchanged) > 0 {
		buf = append(buf, ",\"unchanged\":["...)
		for i, name := range e.unchanged {
			if i > 0 {
				buf = append(buf, ',')
			}
			buf = append(buf, name...)
		}
		buf = append(buf, ']')
	}
	buf = append(buf, '}')
	keyStart := len(b.Keys)
	keys, err := appendKey(b.Keys, rel, identity, previous)
	if err != nil {
		return err
	}
	b.Keys = keys
	b.Records = append(b.Records, Record{
		Op:     e.op,
		Schema: rel.schema, Name: rel.relname,
		Table:      rel.name,
		valueStart: e.recStart, valueEnd: len(buf),
		keyStart: keyStart, keyEnd: len(b.Keys),
	})
	buf = append(buf, '\n')
	b.Buf = buf
	b.Events++
	e.seq++
	return nil
}

// appendKey falls back to the table name when a row has no stable identity (no key columns,
// REPLICA IDENTITY FULL, truncate). Postgres logs the old key when a key column is TOASTed and
// unchanged, so previous supplies what the new tuple omits: a key is never invented.
func appendKey(dst []byte, rel *relation, identity, previous *pglogrepl.TupleData) ([]byte, error) {
	if !rel.keyed || identity == nil {
		return append(dst, rel.table...), nil
	}
	dst = append(dst, '{')
	first := true
	for i, c := range identity.Columns {
		col := &rel.cols[i]
		if !col.isKey {
			continue
		}
		if c.DataType != pglogrepl.TupleDataTypeText && previous != nil && i < len(previous.Columns) {
			c = previous.Columns[i]
		}
		if c.DataType != pglogrepl.TupleDataTypeText {
			return nil, fmt.Errorf("%w: key column %s of %s has no value in the change", ErrUnencodable, col.name, rel.name)
		}
		if !first {
			dst = append(dst, ',')
		}
		first = false
		dst = append(dst, col.key...)
		dst = appendValue(dst, col.oid, c.Data)
	}
	return append(dst, '}'), nil
}

func (e *Encoder) tuple(b *Batch, rel *relation, field string, t *pglogrepl.TupleData, keyOnly bool) error {
	if t == nil {
		return fmt.Errorf("%w: missing tuple data", ErrUnencodable)
	}
	if len(t.Columns) != len(rel.cols) {
		return fmt.Errorf("%w: tuple has %d columns, relation has %d", ErrUnencodable, len(t.Columns), len(rel.cols))
	}

	buf := b.Buf
	buf = append(buf, field...)
	buf = append(buf, '{')
	first := true
	for i, c := range t.Columns {
		col := &rel.cols[i]
		if keyOnly && !col.isKey {
			continue
		}
		if c.DataType == pglogrepl.TupleDataTypeToast {
			e.unchanged = append(e.unchanged, col.name)
			continue
		}
		if !first {
			buf = append(buf, ',')
		}
		first = false
		buf = append(buf, col.key...)
		switch c.DataType {
		case pglogrepl.TupleDataTypeNull:
			buf = append(buf, "null"...)
		case pglogrepl.TupleDataTypeText:
			buf = appendValue(buf, col.oid, c.Data)
		default:
			return fmt.Errorf("%w: binary tuple format in column %s", ErrUnencodable, col.name)
		}
	}
	buf = append(buf, '}')
	b.Buf = buf
	return nil
}

func appendLSN(dst []byte, lsn pglogrepl.LSN) []byte {
	start := len(dst)
	dst = strconv.AppendUint(dst, uint64(lsn)>>32, 16)
	dst = append(dst, '/')
	dst = strconv.AppendUint(dst, uint64(uint32(lsn)), 16)
	for i := start; i < len(dst); i++ {
		if dst[i] >= 'a' {
			dst[i] -= 'a' - 'A'
		}
	}
	return dst
}
