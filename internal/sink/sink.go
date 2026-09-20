package sink

import (
	"context"
	"errors"
	"io"

	"github.com/aymaneallaoui/walcast/internal/event"
)

// ErrWriterBusy means an earlier write is still blocked, for instance on a pipe nobody reads.
var ErrWriterBusy = errors.New("sink: previous write has not returned")

// Sink delivers batches. done must be called exactly once per Send, with nil only when the
// batch is as durable as the sink can make it; Send must give up once ctx is cancelled.
type Sink interface {
	Send(ctx context.Context, b *event.Batch, done func(error))
	Close() error
}

// Writer is a best-effort sink: a successful write says nothing about downstream consumption.
type Writer struct {
	w    io.Writer
	buf  []byte
	idle chan struct{}
}

func NewWriter(w io.Writer) *Writer {
	s := &Writer{w: w, idle: make(chan struct{}, 1)}
	s.idle <- struct{}{}
	return s
}

// Send writes from a private copy because a blocked Write cannot be interrupted: on cancel it is
// abandoned, and the batch buffer goes back to the pool while that write may still be reading.
func (s *Writer) Send(ctx context.Context, b *event.Batch, done func(error)) {
	if len(b.Buf) == 0 {
		done(nil)
		return
	}
	select {
	case <-s.idle:
	default:
		done(ErrWriterBusy)
		return
	}

	s.buf = append(s.buf[:0], b.Buf...)
	result := make(chan error, 1)
	go func() {
		_, err := s.w.Write(s.buf)
		s.idle <- struct{}{}
		result <- err
	}()

	select {
	case err := <-result:
		done(err)
	case <-ctx.Done():
		done(ctx.Err())
	}
}

func (s *Writer) Close() error {
	return nil
}
