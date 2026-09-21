package replication

import (
	"fmt"
	"testing"

	"github.com/aymaneallaoui/walcast/internal/event"
)

const trackedTable = "public.users"

type observed struct {
	rec event.Record
	xid uint32
}

func change(op string, partial bool) event.Record {
	return event.Record{Op: op, Table: trackedTable, Partial: partial}
}

func TestWidenXid(t *testing.T) {
	const epoch = uint64(1) << 32
	tests := []struct {
		name string
		xid  uint32
		ref  uint64
		want uint64
	}{
		{"same epoch", 744, 750, 744},
		{"ahead of the reference", 760, 750, 760},
		{"second epoch", 744, epoch + 750, epoch + 744},
		{"reference just wrapped, xid from before the wrap", 0xFFFFFFF0, epoch + 5, 0xFFFFFFF0},
		{"xid just wrapped, reference from before the wrap", 5, 0xFFFFFFF0, epoch + 5},
		{"first epoch never goes negative", 0xFFFFFFF0, 5, 0xFFFFFFF0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := widenXid(tt.xid, tt.ref); got != tt.want {
				t.Fatalf("widenXid(%d, %d) = %d, want %d", tt.xid, tt.ref, got, tt.want)
			}
		})
	}
}

func TestTrackerVerdict(t *testing.T) {
	const xmin = 744
	key := []byte(`{"id":1}`)
	tests := []struct {
		name    string
		changes []observed
		want    rowVerdict
	}{
		{name: "untouched key is emitted", want: rowEmit},
		{name: "change the snapshot already saw is emitted", changes: []observed{{change(event.OpUpdate, false), 743}}, want: rowEmit},
		{name: "decoded but invisible writer wins over the chunk row", changes: []observed{{change(event.OpUpdate, false), 744}}, want: rowDrop},
		{name: "delete is a complete image", changes: []observed{{change(event.OpDelete, false), 750}}, want: rowDrop},
		{name: "patch without a toast column leaves no baseline", changes: []observed{{change(event.OpUpdate, true), 750}}, want: rowRetry},
		{name: "complete image followed by a patch is a baseline", changes: []observed{{change(event.OpInsert, false), 745}, {change(event.OpUpdate, true), 750}}, want: rowDrop},
		{name: "patch followed by a complete image is a baseline", changes: []observed{{change(event.OpUpdate, true), 745}, {change(event.OpUpdate, false), 750}}, want: rowDrop},
		{name: "two patches are still no baseline", changes: []observed{{change(event.OpUpdate, true), 745}, {change(event.OpUpdate, true), 750}}, want: rowRetry},
		{name: "a row moved onto a deleted key has no baseline from the row that was there", changes: []observed{{change(event.OpDelete, false), 745}, {change(event.OpInsert, true), 750}}, want: rowRetry},
		{name: "a row moved onto a key does not inherit the old occupant's image", changes: []observed{{change(event.OpInsert, false), 745}, {change(event.OpDelete, false), 746}, {change(event.OpInsert, true), 750}}, want: rowRetry},
		{name: "invisible truncate drops every row", changes: []observed{{change(event.OpTruncate, false), 750}}, want: rowDrop},
		{name: "truncate the snapshot saw is irrelevant", changes: []observed{{change(event.OpTruncate, false), 700}}, want: rowEmit},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := newTracker(trackedTable, 1, xmin)
			for _, c := range tt.changes {
				tr.observe(c.rec, key, c.xid)
			}
			if got := tr.verdict(key, xmin); got != tt.want {
				t.Fatalf("verdict = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestTrackerIgnoresOtherTables(t *testing.T) {
	tr := newTracker(trackedTable, 1, 744)
	tr.observe(event.Record{Op: event.OpUpdate, Table: "public.orders"}, []byte(`{"id":1}`), 750)
	if got := tr.verdict([]byte(`{"id":1}`), 744); got != rowEmit {
		t.Fatalf("a change to another table with the same key suppressed the row: verdict = %d", got)
	}
}

func TestTrackerPruneKeepsWhatALaterSnapshotStillNeeds(t *testing.T) {
	tr := newTracker(trackedTable, 1, 744)
	tr.observe(change(event.OpUpdate, false), []byte(`{"id":1}`), 740)
	tr.observe(change(event.OpUpdate, false), []byte(`{"id":2}`), 760)
	tr.prune(750)
	if len(tr.keys) != 1 {
		t.Fatalf("tracked keys after prune = %d, want 1", len(tr.keys))
	}
	if got := tr.verdict([]byte(`{"id":2}`), 750); got != rowDrop {
		t.Fatalf("verdict for the surviving key = %d, want drop", got)
	}
}

func TestTrackerOverflowIsSticky(t *testing.T) {
	tr := newTracker(trackedTable, 1, 744)
	for i := range maxTrackedKeys + 1 {
		tr.observe(change(event.OpInsert, false), fmt.Appendf(nil, `{"id":%d}`, i), 750)
	}
	if !tr.overflowed {
		t.Fatal("tracker did not report overflow")
	}
	tr.observe(change(event.OpInsert, false), []byte(`{"id":1}`), 751)
	if tr.keys != nil {
		t.Fatal("an overflowed tracker kept collecting keys")
	}
}
