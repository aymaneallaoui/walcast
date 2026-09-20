package replication

import (
	"context"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

type stream interface {
	Receive(ctx context.Context) (pgproto3.BackendMessage, error)
	SendStatus(flushed pglogrepl.LSN, replyRequested bool, timeout time.Duration) error
}

type pgStream struct {
	conn *pgconn.PgConn
}

func (p pgStream) Receive(ctx context.Context) (pgproto3.BackendMessage, error) {
	return p.conn.ReceiveMessage(ctx)
}

// SendStatus reports one LSN as write, flush and apply: pglogrepl replaces a zero flush position
// with the write position, which would ack undelivered WAL. It also ignores its context, so the
// write deadline is the only thing that stops a dead peer from blocking this goroutine forever.
func (p pgStream) SendStatus(flushed pglogrepl.LSN, replyRequested bool, timeout time.Duration) error {
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
