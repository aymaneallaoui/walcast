package event

import (
	"sync"

	"github.com/jackc/pglogrepl"
)

const (
	initialBatchCap = 64 << 10
	maxPooledCap    = 4 * initialBatchCap
)

// Record locates one event inside a Batch. Value excludes the trailing newline and Key is the
// row's replica identity as compact JSON, or the quoted table name when the row has none.
type Record struct {
	Op           string
	Schema, Name string
	Table        string
	// Partial marks an update that left out an unchanged TOAST column: a consumer that has never
	// seen the row cannot rebuild it from this event alone.
	Partial bool
	// Skipped is set by a sink that chose not to send this record, so delivery is not counted for it.
	Skipped              bool
	valueStart, valueEnd int
	keyStart, keyEnd     int
}

type Batch struct {
	Buf     []byte
	Keys    []byte
	Records []Record
	Events  int
	Seq     uint64
	AckLSN  pglogrepl.LSN
	// CommitNanos is when the oldest transaction with an event in this batch committed on the source,
	// in Unix nanoseconds: no event in the batch waited longer. Backfill reads leave it zero.
	CommitNanos int64
}

func (b *Batch) Value(r Record) []byte { return b.Buf[r.valueStart:r.valueEnd] }

func (b *Batch) Key(r Record) []byte { return b.Keys[r.keyStart:r.keyEnd] }

// Size is what a record takes in Buf, its line terminator included.
func (b *Batch) Size(r Record) int { return r.valueEnd - r.valueStart + 1 }

var batchPool = sync.Pool{
	New: func() any { return &Batch{Buf: make([]byte, 0, initialBatchCap)} },
}

func NewBatch() *Batch {
	return batchPool.Get().(*Batch)
}

// Release must be called only after the sink's done callback, since sinks read Buf asynchronously.
// Oversized buffers are dropped so one huge row cannot pin its capacity in the pool.
func (b *Batch) Release() {
	if cap(b.Buf) > maxPooledCap {
		return
	}
	b.Buf = b.Buf[:0]
	b.Keys = b.Keys[:0]
	b.Records = b.Records[:0]
	b.Events = 0
	b.Seq = 0
	b.AckLSN = 0
	b.CommitNanos = 0
	batchPool.Put(b)
}

func (b *Batch) Dirty() bool {
	return b.Events > 0 || b.AckLSN != 0
}
