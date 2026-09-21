package replication

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

// stream is the replication connection as the owner goroutine uses it. Receive takes a deadline, not
// a context: a context per message cost a fifth of that goroutine's CPU in timers and watchers.
type stream interface {
	Receive(deadline time.Time) (pgproto3.BackendMessage, error)
	Interrupt()
	SendStatus(flushed pglogrepl.LSN, replyRequested bool, timeout time.Duration) error
}

type pgStream struct {
	conn        *pgconn.PgConn
	deadline    time.Time
	interrupted atomic.Bool
}

// Receive returns a pgconn timeout error at the deadline and leaves the connection usable.
func (p *pgStream) Receive(deadline time.Time) (pgproto3.BackendMessage, error) {
	if !deadline.Equal(p.deadline) {
		if err := p.conn.Conn().SetReadDeadline(deadline); err != nil {
			return nil, err
		}
		p.deadline = deadline
		// Interrupt may have run between the caller's last check and the line above.
		if p.interrupted.Load() {
			p.Interrupt()
		}
	}
	return p.conn.ReceiveMessage(context.Background())
}

// Interrupt makes the current and every later Receive time out at once. It is safe to call from
// any goroutine.
func (p *pgStream) Interrupt() {
	p.interrupted.Store(true)
	_ = p.conn.Conn().SetReadDeadline(time.Unix(1, 0))
}

// SendStatus reports one LSN as write, flush and apply: pglogrepl replaces a zero flush position
// with the write position, which would ack undelivered WAL. It also ignores its context, so the
// write deadline is the only thing that stops a dead peer from blocking this goroutine forever.
func (p *pgStream) SendStatus(flushed pglogrepl.LSN, replyRequested bool, timeout time.Duration) error {
	netConn := p.conn.Conn()
	if err := netConn.SetWriteDeadline(time.Now().Add(timeout)); err != nil {
		return err
	}
	defer func() { _ = netConn.SetWriteDeadline(time.Time{}) }()

	return pglogrepl.SendStandbyStatusUpdate(context.Background(), p.conn, pglogrepl.StandbyStatusUpdate{
		WALWritePosition: flushed,
		WALFlushPosition: flushed,
		WALApplyPosition: flushed,
		ReplyRequested:   replyRequested,
	})
}
