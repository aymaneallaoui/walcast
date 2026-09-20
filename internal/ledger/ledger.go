package ledger

import (
	"errors"
	"sync"
	"sync/atomic"

	"github.com/jackc/pglogrepl"
)

const compactAt = 1024

type entry struct {
	ack  pglogrepl.LSN
	size int
	done bool
}

// Ledger tracks in-flight batches in send order. The flushed LSN only moves across
// contiguous completed batches, so an out-of-order completion can never ack a gap.
type Ledger struct {
	mu      sync.Mutex
	entries []entry
	head    int
	base    uint64
	err     error

	flushed atomic.Uint64
	hold    atomic.Uint64
	bytes   atomic.Int64
	notify  chan struct{}
}

func New(flushed pglogrepl.LSN) *Ledger {
	l := &Ledger{notify: make(chan struct{}, 1)}
	l.flushed.Store(uint64(flushed))
	return l
}

func (l *Ledger) Add(ack pglogrepl.LSN, size int) uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.head == len(l.entries) || l.head >= compactAt {
		n := copy(l.entries, l.entries[l.head:])
		l.entries = l.entries[:n]
		l.base += uint64(l.head)
		l.head = 0
	}
	l.entries = append(l.entries, entry{ack: ack, size: size})
	l.bytes.Add(int64(size))
	return l.base + uint64(len(l.entries)-1)
}

func (l *Ledger) Done(seq uint64) {
	l.mu.Lock()
	idx := int(seq - l.base)
	if seq >= l.base && idx < len(l.entries) && !l.entries[idx].done {
		l.entries[idx].done = true
		l.bytes.Add(-int64(l.entries[idx].size))
		for l.head < len(l.entries) && l.entries[l.head].done {
			if ack := uint64(l.entries[l.head].ack); ack > l.flushed.Load() {
				l.flushed.Store(ack)
			}
			l.head++
		}
	}
	l.mu.Unlock()
	l.signal()
}

// Fail keeps every distinct failure, not just the first: a fatal delivery error that lands after
// a connection error must still reach the supervisor, or it would reconnect instead of stopping.
func (l *Ledger) Fail(err error) {
	l.mu.Lock()
	if !errors.Is(l.err, err) {
		l.err = errors.Join(l.err, err)
	}
	l.mu.Unlock()
	l.signal()
}

func (l *Ledger) Err() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.err
}

// AdvanceIdle acks a position with no events behind it; it is a no-op while any batch is in flight.
func (l *Ledger) AdvanceIdle(lsn pglogrepl.LSN) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.head != len(l.entries) {
		return false
	}
	if uint64(lsn) > l.flushed.Load() {
		l.flushed.Store(uint64(lsn))
	}
	return true
}

// Flushed is what may be reported to Postgres: everything delivered, capped by a hold.
func (l *Ledger) Flushed() pglogrepl.LSN {
	flushed, hold := l.flushed.Load(), l.hold.Load()
	if hold != 0 && hold < flushed {
		return pglogrepl.LSN(hold)
	}
	return pglogrepl.LSN(flushed)
}

// Delivered ignores the hold. Work that waits for a delivery must use it, or a hold would stall
// the very delivery that lifts it.
func (l *Ledger) Delivered() pglogrepl.LSN {
	return pglogrepl.LSN(l.flushed.Load())
}

// Hold keeps Flushed at or below lsn although later batches are delivered, so a restart replays
// from there. It only ever moves down until Release: the earliest unresolved position wins.
func (l *Ledger) Hold(lsn pglogrepl.LSN) {
	for {
		cur := l.hold.Load()
		if cur != 0 && cur <= uint64(lsn) {
			return
		}
		if l.hold.CompareAndSwap(cur, uint64(lsn)) {
			return
		}
	}
}

func (l *Ledger) Release() {
	l.hold.Store(0)
}

// Depth counts batches not yet retired, including completed ones stuck behind an unfinished
// head. Bounding it bounds both ledger memory and the replay window after a restart.
func (l *Ledger) Depth() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.entries) - l.head
}

func (l *Ledger) Bytes() int64 {
	return l.bytes.Load()
}

func (l *Ledger) Notify() <-chan struct{} {
	return l.notify
}

func (l *Ledger) signal() {
	select {
	case l.notify <- struct{}{}:
	default:
	}
}
