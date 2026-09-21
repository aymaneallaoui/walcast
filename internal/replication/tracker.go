package replication

import "github.com/aymaneallaoui/walcast/internal/event"

const maxTrackedKeys = 1 << 20

// trackedKeysLimit is a variable only so that a test can reach an overflow with a handful of rows.
var trackedKeysLimit = maxTrackedKeys

type rowVerdict int

const (
	rowEmit rowVerdict = iota
	rowDrop
	rowRetry
)

type trackedKey struct {
	xid     uint64
	partial bool
}

// tracker remembers which keys of the table being backfilled the stream has touched, and by which
// transaction. It is owned by the session's owner goroutine and needs no locking.
type tracker struct {
	table      string
	generation uint64
	ref        uint64
	keys       map[string]trackedKey
	truncated  uint64
	overflowed bool
}

func newTracker(table string, generation, ref uint64) *tracker {
	return &tracker{table: table, generation: generation, ref: ref, keys: make(map[string]trackedKey)}
}

// widenXid rebuilds the 64-bit xid behind the 32-bit one a Begin message carries, picking the epoch
// that puts it closest to ref. Chunks are short-lived, so the two are never half the xid space apart.
func widenXid(xid uint32, ref uint64) uint64 {
	const epoch = 1 << 32
	wide := ref&^(epoch-1) | uint64(xid)
	switch {
	case wide > ref && wide-ref > epoch/2 && wide >= epoch:
		wide -= epoch
	case ref > wide && ref-wide > epoch/2:
		wide += epoch
	}
	return wide
}

func (t *tracker) observe(rec event.Record, key []byte, xid uint32) {
	if rec.Table != t.table || t.overflowed {
		return
	}
	wide := widenXid(xid, t.ref)
	if rec.Op == event.OpTruncate {
		t.truncated = max(t.truncated, wide)
		return
	}
	prev, seen := t.keys[string(key)]
	if !seen && len(t.keys) >= trackedKeysLimit {
		t.overflowed = true
		t.keys = nil
		return
	}
	// One complete image inside the window is a baseline for every later patch of that row. An
	// insert is another row, moved onto the key, and takes nothing from the one that was there.
	inherits := seen && !prev.partial && rec.Op == event.OpUpdate
	t.keys[string(key)] = trackedKey{xid: wide, partial: rec.Partial && !inherits}
}

// verdict decides what happens to a chunk row read under a snapshot with the given xmin. Every
// transaction that snapshot could not see has an xid at or above xmin, so a key touched by one is
// stale in the chunk. It is dropped when the stream already delivered a complete image, and read
// again when the stream only delivered a patch that lacks an unchanged TOAST column.
func (t *tracker) verdict(key []byte, xmin uint64) rowVerdict {
	if t.truncated >= xmin && t.truncated != 0 {
		return rowDrop
	}
	touched, ok := t.keys[string(key)]
	switch {
	case !ok || touched.xid < xmin:
		return rowEmit
	case touched.partial:
		return rowRetry
	default:
		return rowDrop
	}
}

func (t *tracker) prune(xmin uint64) {
	for key, touched := range t.keys {
		if touched.xid < xmin {
			delete(t.keys, key)
		}
	}
}
