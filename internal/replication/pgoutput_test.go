package replication

import (
	"encoding/binary"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgproto3"
)

const testRelID = 42

func cstring(buf []byte, s string) []byte {
	return append(append(buf, s...), 0)
}

func xlogData(walStart pglogrepl.LSN, payload []byte) *pgproto3.CopyData {
	buf := []byte{pglogrepl.XLogDataByteID}
	buf = binary.BigEndian.AppendUint64(buf, uint64(walStart))
	buf = binary.BigEndian.AppendUint64(buf, uint64(walStart))
	buf = binary.BigEndian.AppendUint64(buf, 0)
	return &pgproto3.CopyData{Data: append(buf, payload...)}
}

func keepalive(walEnd pglogrepl.LSN, replyRequested bool) *pgproto3.CopyData {
	buf := []byte{pglogrepl.PrimaryKeepaliveMessageByteID}
	buf = binary.BigEndian.AppendUint64(buf, uint64(walEnd))
	buf = binary.BigEndian.AppendUint64(buf, 0)
	if replyRequested {
		return &pgproto3.CopyData{Data: append(buf, 1)}
	}
	return &pgproto3.CopyData{Data: append(buf, 0)}
}

func relationMsg() []byte {
	buf := []byte{'R'}
	buf = binary.BigEndian.AppendUint32(buf, testRelID)
	buf = cstring(buf, "public")
	buf = cstring(buf, "users")
	buf = append(buf, 'd')
	buf = binary.BigEndian.AppendUint16(buf, 2)
	for i, col := range []struct {
		name string
		oid  uint32
	}{{"id", 20}, {"name", 25}} {
		flags := byte(0)
		if i == 0 {
			flags = 1
		}
		buf = append(buf, flags)
		buf = cstring(buf, col.name)
		buf = binary.BigEndian.AppendUint32(buf, col.oid)
		buf = binary.BigEndian.AppendUint32(buf, 0xFFFFFFFF)
	}
	return buf
}

func beginMsg(finalLSN pglogrepl.LSN, xid uint32) []byte {
	buf := []byte{'B'}
	buf = binary.BigEndian.AppendUint64(buf, uint64(finalLSN))
	buf = binary.BigEndian.AppendUint64(buf, 0)
	return binary.BigEndian.AppendUint32(buf, xid)
}

func commitMsg(commitLSN, endLSN pglogrepl.LSN) []byte {
	buf := []byte{'C', 0}
	buf = binary.BigEndian.AppendUint64(buf, uint64(commitLSN))
	buf = binary.BigEndian.AppendUint64(buf, uint64(endLSN))
	return binary.BigEndian.AppendUint64(buf, 0)
}

func insertMsg(values ...string) []byte {
	buf := []byte{'I'}
	buf = binary.BigEndian.AppendUint32(buf, testRelID)
	buf = append(buf, 'N')
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(values)))
	for _, v := range values {
		buf = append(buf, 't')
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(v)))
		buf = append(buf, v...)
	}
	return buf
}

const unchangedToast = "\x00toast"

// updateMsg builds an update without an old tuple; the unchangedToast value stands for a column
// Postgres left out because it is TOASTed and did not change.
func updateMsg(values ...string) []byte {
	buf := []byte{'U'}
	buf = binary.BigEndian.AppendUint32(buf, testRelID)
	buf = append(buf, 'N')
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(values)))
	for _, v := range values {
		if v == unchangedToast {
			buf = append(buf, 'u')
			continue
		}
		buf = append(buf, 't')
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(v)))
		buf = append(buf, v...)
	}
	return buf
}

func logicalMsg(lsn pglogrepl.LSN, prefix string, content []byte) []byte {
	buf := []byte{'M', 0}
	buf = binary.BigEndian.AppendUint64(buf, uint64(lsn))
	buf = cstring(buf, prefix)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(content)))
	return append(buf, content...)
}
