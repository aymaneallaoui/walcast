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
	Table                string
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
}

func (b *Batch) Value(r Record) []byte { return b.Buf[r.valueStart:r.valueEnd] }

func (b *Batch) Key(r Record) []byte { return b.Keys[r.keyStart:r.keyEnd] }

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
	batchPool.Put(b)
}

func (b *Batch) Dirty() bool {
	return b.Events > 0 || b.AckLSN != 0
}
