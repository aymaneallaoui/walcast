package sink

import (
	"context"
	"io"

	"github.com/aymaneallaoui/walcast/internal/event"
)

// Sink delivers batches. done must be called exactly once per Send, with nil only when the
// batch is as durable as the sink can make it; Send must give up once ctx is cancelled.
type Sink interface {
	Send(ctx context.Context, b *event.Batch, done func(error))
	Close() error
}

// Writer is a best-effort sink: a successful write says nothing about downstream consumption.
type Writer struct {
	w io.Writer
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{w: w}
}

func (s *Writer) Send(_ context.Context, b *event.Batch, done func(error)) {
	if len(b.Buf) == 0 {
		done(nil)
		return
	}
	_, err := s.w.Write(b.Buf)
	done(err)
}

func (s *Writer) Close() error {
	return nil
}
