package ledger

import (
	"errors"
	"testing"

	"github.com/jackc/pglogrepl"
)

func TestFlushedMovesOnlyAcrossContiguousDoneBatches(t *testing.T) {
	l := New(10)
	a := l.Add(100, 1)
	b := l.Add(200, 1)
	c := l.Add(300, 1)

	l.Done(c)
	l.Done(b)
	if got := l.Flushed(); got != 10 {
		t.Fatalf("flushed = %d with first batch still in flight, want 10", got)
	}

	l.Done(a)
	if got := l.Flushed(); got != 300 {
		t.Fatalf("flushed = %d, want 300", got)
	}
}

func TestFragmentsDoNotAckUntilCommitBatchIsDone(t *testing.T) {
	l := New(0)
	frag1 := l.Add(0, 64)
	frag2 := l.Add(0, 64)
	commit := l.Add(500, 8)

	l.Done(frag1)
	l.Done(frag2)
	if got := l.Flushed(); got != 0 {
		t.Fatalf("flushed = %d after fragments only, want 0", got)
	}

	l.Done(commit)
	if got := l.Flushed(); got != 500 {
		t.Fatalf("flushed = %d, want 500", got)
	}
}

func TestAdvanceIdleRefusedWhileInFlight(t *testing.T) {
	l := New(0)
	seq := l.Add(100, 1)

	if l.AdvanceIdle(900) {
		t.Fatal("AdvanceIdle succeeded with a batch in flight")
	}
	l.Done(seq)

	if !l.AdvanceIdle(900) || l.Flushed() != 900 {
		t.Fatalf("flushed = %d, want 900", l.Flushed())
	}
	if l.AdvanceIdle(50); l.Flushed() != 900 {
		t.Fatalf("flushed moved backwards to %d", l.Flushed())
	}
}

func TestBytesAndDoubleDone(t *testing.T) {
	l := New(0)
	seq := l.Add(1, 100)
	l.Add(2, 50)

	l.Done(seq)
	l.Done(seq)
	if got := l.Bytes(); got != 50 {
		t.Fatalf("bytes = %d, want 50", got)
	}
}

func TestSequencesSurviveCompaction(t *testing.T) {
	l := New(0)
	for i := range 3 * compactAt {
		seq := l.Add(pglogrepl.LSN(i+1), 1)
		if seq != uint64(i) {
			t.Fatalf("seq = %d, want %d", seq, i)
		}
		l.Done(seq)
	}
	if got := l.Flushed(); got != 3*compactAt {
		t.Fatalf("flushed = %d, want %d", got, 3*compactAt)
	}
}

func TestFailIsStickyAndSignals(t *testing.T) {
	l := New(0)
	first := errors.New("first")
	l.Fail(first)
	l.Fail(errors.New("second"))

	if !errors.Is(l.Err(), first) {
		t.Fatalf("err = %v, want first", l.Err())
	}
	select {
	case <-l.Notify():
	default:
		t.Fatal("Fail did not signal")
	}
}

func TestDoneNeverMovesFlushedBackwards(t *testing.T) {
	l := New(0)
	l.AdvanceIdle(900)
	seq := l.Add(100, 1)
	l.Done(seq)

	if got := l.Flushed(); got != 900 {
		t.Fatalf("flushed = %d, want 900", got)
	}
}

func TestDepthCountsCompletedBatchesBehindUnfinishedHead(t *testing.T) {
	l := New(0)
	head := l.Add(1, 1)
	l.Done(l.Add(2, 1))
	l.Done(l.Add(3, 1))

	if got := l.Depth(); got != 3 {
		t.Fatalf("depth = %d, want 3", got)
	}
	l.Done(head)
	if got := l.Depth(); got != 0 {
		t.Fatalf("depth = %d after head completed, want 0", got)
	}
}
