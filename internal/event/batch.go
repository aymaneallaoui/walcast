package event

import (
	"sync"

	"github.com/jackc/pglogrepl"
)

const (
	initialBatchCap = 64 << 10
	maxPooledCap    = 4 * initialBatchCap
)

type Batch struct {
	Buf    []byte
	Events int
	Seq    uint64
	AckLSN pglogrepl.LSN
}

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
	b.Events = 0
	b.Seq = 0
	b.AckLSN = 0
	batchPool.Put(b)
}

func (b *Batch) Dirty() bool {
	return b.Events > 0 || b.AckLSN != 0
}
