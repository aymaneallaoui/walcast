package sink

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aymaneallaoui/walcast/internal/event"
)

type failingWriter struct{ err error }

func (w failingWriter) Write([]byte) (int, error) { return 0, w.err }

type blockingWriter struct {
	release chan struct{}
	wrote   chan []byte
}

func (w blockingWriter) Write(p []byte) (int, error) {
	<-w.release
	w.wrote <- append([]byte(nil), p...)
	return len(p), nil
}

func TestWriter_Send(t *testing.T) {
	t.Run("gives up on cancel, refuses new work while stuck, then recovers", func(t *testing.T) {
		w := blockingWriter{release: make(chan struct{}), wrote: make(chan []byte, 2)}
		snk := NewWriter(w)
		ctx, cancel := context.WithCancel(context.Background())
		batch := &event.Batch{Buf: []byte("first\n")}

		errs := make(chan error, 1)
		go snk.Send(ctx, batch, func(err error) { errs <- err })
		cancel()
		select {
		case err := <-errs:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("err = %v, want context.Canceled", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("Send ignored cancellation")
		}

		copy(batch.Buf, "XXXXX")
		snk.Send(context.Background(), &event.Batch{Buf: []byte("second\n")}, func(err error) {
			if !errors.Is(err, ErrWriterBusy) {
				t.Fatalf("err = %v, want ErrWriterBusy", err)
			}
		})

		close(w.release)
		if got := string(<-w.wrote); got != "first\n" {
			t.Fatalf("abandoned write saw %q: it must not share the released batch buffer", got)
		}

		deadline := time.Now().Add(2 * time.Second)
		for {
			var got error
			snk.Send(context.Background(), &event.Batch{Buf: []byte("third\n")}, func(err error) { got = err })
			if got == nil {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("writer never recovered: %v", got)
			}
			time.Sleep(time.Millisecond)
		}
	})

	t.Run("writes the batch then reports success", func(t *testing.T) {
		var out bytes.Buffer
		var got error
		calls := 0

		NewWriter(&out).Send(context.Background(), &event.Batch{Buf: []byte("{}\n")}, func(err error) { got, calls = err, calls+1 })

		if out.String() != "{}\n" || got != nil || calls != 1 {
			t.Fatalf("out=%q err=%v calls=%d", out.String(), got, calls)
		}
	})

	t.Run("reports the write error", func(t *testing.T) {
		want := errors.New("broken pipe")
		var got error

		NewWriter(failingWriter{want}).Send(context.Background(), &event.Batch{Buf: []byte("x")}, func(err error) { got = err })

		if !errors.Is(got, want) {
			t.Fatalf("err = %v, want %v", got, want)
		}
	})

	t.Run("acks an empty batch without writing", func(t *testing.T) {
		calls := 0
		NewWriter(failingWriter{errors.New("must not be called")}).Send(context.Background(), &event.Batch{}, func(err error) {
			if err != nil {
				t.Fatalf("err = %v", err)
			}
			calls++
		})
		if calls != 1 {
			t.Fatalf("done called %d times", calls)
		}
	})
}
