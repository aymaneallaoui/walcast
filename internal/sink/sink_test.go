package sink

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/aymaneallaoui/walcast/internal/event"
)

type failingWriter struct{ err error }

func (w failingWriter) Write([]byte) (int, error) { return 0, w.err }

func TestWriter_Send(t *testing.T) {
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
