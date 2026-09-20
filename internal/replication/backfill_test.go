package replication

import (
	"encoding/binary"
	"encoding/json"
	"strings"
	"testing"

	"github.com/jackc/pglogrepl"
	"github.com/rs/zerolog"

	"github.com/aymaneallaoui/walcast/internal/event"
)

func backfillSession(t *testing.T) *session {
	t.Helper()
	s := newSession(testConfig(), zerolog.Nop(), newFakeStream())
	link, err := newBackfillLink()
	if err != nil {
		t.Fatal(err)
	}
	s.link, s.backfilling = link, true
	return s
}

func (s *session) feed(t *testing.T, payloads ...[]byte) {
	t.Helper()
	for _, payload := range payloads {
		if err := s.handleXLogData(xlogData(50, payload).Data[1:]); err != nil {
			t.Fatal(err)
		}
	}
}

func markerMsg(t *testing.T, lsn pglogrepl.LSN, m marker) []byte {
	t.Helper()
	content, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return logicalMsg(lsn, markerPrefix, content)
}

func usersChunk(generation, number, xmin uint64, ids ...string) *chunk {
	c := &chunk{
		generation: generation, number: number, xmin: xmin, backfillID: "16384.1.1", ts: []byte("2026-09-20T12:00:00Z"),
		tableName: "public.users",
		table:     event.NewTable("public", "users", []event.Column{{Name: "id", OID: 20, Key: true}, {Name: "name", OID: 25}}),
	}
	for _, id := range ids {
		c.rows = append(c.rows, [][]byte{[]byte(id), []byte("from the chunk")})
		c.keys = append(c.keys, []string{id})
	}
	return c
}

// The scenario the design exists for: changes that the chunk's snapshot could not see were already
// decoded when the marker arrives, so the chunk's copies of those rows are stale.
func TestSession_mergesChunkAtItsMarker(t *testing.T) {
	const xmin, markerLSN = 744, pglogrepl.LSN(500)
	s := backfillSession(t)
	track := marker{Session: s.link.session, Kind: markerTrack, Generation: 1, Table: "public.users", Ref: xmin}
	s.feed(t, markerMsg(t, 100, track), relationMsg(),
		beginMsg(200, 743), updateMsg("4", "seen by the snapshot"), commitMsg(200, 210),
		beginMsg(300, 744), updateMsg("1", "decoded but invisible"), commitMsg(300, 310),
		beginMsg(400, 750), updateMsg("2", unchangedToast), commitMsg(400, 410),
	)
	if got := <-s.link.tracked; got != 1 {
		t.Fatalf("tracking ack = %d, want generation 1", got)
	}
	streamed := s.cur.Events

	s.link.register(usersChunk(1, 1, xmin, "1", "2", "3", "4"))
	s.feed(t, markerMsg(t, markerLSN, marker{Session: s.link.session, Kind: markerChunk, Generation: 1, Chunk: 1}))
	if s.reads == nil {
		t.Fatal("chunk marker did not start a merge")
	}
	if err := s.emitReads(); err != nil {
		t.Fatal(err)
	}

	reads := s.cur.Records[streamed:]
	var ids []string
	for _, rec := range reads {
		if rec.Op != event.OpRead {
			t.Fatalf("record op = %q, want read", rec.Op)
		}
		ids = append(ids, string(s.cur.Key(rec)))
	}
	if got, want := strings.Join(ids, " "), `{"id":3} {"id":4}`; got != want {
		t.Fatalf("emitted reads = %s, want %s: row 1 has a complete newer image, row 2 only a patch", got, want)
	}
	if s.cur.AckLSN != markerLSN {
		t.Fatalf("batch ack = %s, want the marker's LSN %s", s.cur.AckLSN, markerLSN)
	}
	if a := s.awaited; a == nil || a.result.emitted != 2 || len(a.result.retry) != 1 || a.result.retry[0][0] != "2" {
		t.Fatalf("awaited = %+v, want 2 emitted and key 2 to be read again", a)
	}
	value := string(s.cur.Value(reads[0]))
	if strings.Contains(value, "commit_lsn") || !strings.Contains(value, `"backfill":"16384.1.1"`) {
		t.Fatalf("read event must carry a backfill id and no commit_lsn: %s", value)
	}
}

func TestSession_handleMarker(t *testing.T) {
	t.Run("marker from another session is ignored", func(t *testing.T) {
		s := backfillSession(t)
		s.link.register(usersChunk(1, 1, 744, "1"))
		s.feed(t, markerMsg(t, 100, marker{Session: "someone-else", Kind: markerChunk, Generation: 1, Chunk: 1}))
		if s.reads != nil || s.awaited != nil {
			t.Fatal("a replayed marker from a dead session was acted on")
		}
	})

	t.Run("chunk without a live tracker is handed back as invalid", func(t *testing.T) {
		s := backfillSession(t)
		s.link.register(usersChunk(1, 1, 744, "1"))
		s.feed(t, markerMsg(t, 100, marker{Session: s.link.session, Kind: markerChunk, Generation: 1, Chunk: 1}))
		if s.reads != nil || s.awaited == nil || !s.awaited.result.invalid {
			t.Fatalf("reads=%v awaited=%+v, want an invalid result and no merge", s.reads, s.awaited)
		}
	})

	t.Run("chunk from an older tracking generation is invalid", func(t *testing.T) {
		s := backfillSession(t)
		s.feed(t, markerMsg(t, 100, marker{Session: s.link.session, Kind: markerTrack, Generation: 2, Table: "public.users", Ref: 744}))
		s.link.register(usersChunk(1, 1, 744, "1"))
		s.feed(t, markerMsg(t, 110, marker{Session: s.link.session, Kind: markerChunk, Generation: 1, Chunk: 1}))
		if s.awaited == nil || !s.awaited.result.invalid {
			t.Fatal("a chunk read before the tracker was reset must not be merged")
		}
	})

	t.Run("without a backfill, foreign messages are harmless", func(t *testing.T) {
		s := newSession(testConfig(), zerolog.Nop(), newFakeStream())
		s.feed(t, logicalMsg(100, markerPrefix, []byte("not json")), logicalMsg(110, "other", nil))
		if s.tracker != nil || s.reads != nil {
			t.Fatal("a logical message changed session state")
		}
	})
}

func TestSession_chunkSpansBatchesAndOnlyTheLastIsAcked(t *testing.T) {
	const markerLSN = pglogrepl.LSN(500)
	s := backfillSession(t)
	s.cfg.BatchMaxBytes = 1
	s.feed(t, markerMsg(t, 100, marker{Session: s.link.session, Kind: markerTrack, Generation: 1, Table: "public.users", Ref: 744}))
	s.link.register(usersChunk(1, 1, 744, "1", "2"))
	s.feed(t, markerMsg(t, markerLSN, marker{Session: s.link.session, Kind: markerChunk, Generation: 1, Chunk: 1}))

	if err := s.emitReads(); err != nil {
		t.Fatal(err)
	}
	if s.pending == nil || s.pending.AckLSN != 0 || s.reads == nil {
		t.Fatalf("first batch: pending=%v reads=%v, want a sealed batch without an ack and rows left", s.pending, s.reads)
	}
	s.pending = nil
	if err := s.emitReads(); err != nil {
		t.Fatal(err)
	}
	if s.reads != nil || s.cur.AckLSN != markerLSN {
		t.Fatalf("last batch: reads=%v ack=%s, want the merge finished and acked at %s", s.reads, s.cur.AckLSN, markerLSN)
	}
}

func TestKeyValues(t *testing.T) {
	table := backfillTable{schema: "public", name: "orders", keyColumns: []string{"region", "id", "live"}}
	tests := []struct {
		name    string
		encoded string
		want    []string
		wantErr bool
	}{
		{
			name: "composite key in index order, not json order", encoded: `{"id":9007199254740993,"live":true,"region":"eu \"west\""}`,
			want: []string{`eu "west"`, "9007199254740993", "true"},
		},
		{name: "numeric rendered as a string", encoded: `{"region":"NaN","id":"12.50","live":false}`, want: []string{"NaN", "12.50", "false"}},
		{name: "missing column", encoded: `{"id":1,"live":true}`, wantErr: true},
		{name: "not json", encoded: `public.orders`, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := keyValues(table, []byte(tt.encoded))
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr && strings.Join(got, "|") != strings.Join(tt.want, "|") {
				t.Fatalf("values = %q, want %q", got, tt.want)
			}
		})
	}
}

// keyMoveMsg is an update that changes the key and leaves the second column as unchanged TOAST.
func keyMoveMsg(oldID, newID string) []byte {
	buf := []byte{'U'}
	buf = binary.BigEndian.AppendUint32(buf, testRelID)
	buf = append(buf, 'K')
	buf = binary.BigEndian.AppendUint16(buf, 2)
	buf = append(buf, 't')
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(oldID)))
	buf = append(buf, oldID...)
	buf = append(buf, 'n', 'N')
	buf = binary.BigEndian.AppendUint16(buf, 2)
	buf = append(buf, 't')
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(newID)))
	buf = append(buf, newID...)
	return append(buf, 'u')
}

func TestSession_keyMoveHoldsTheAckUntilItsRowIsReadAgain(t *testing.T) {
	s := backfillSession(t)
	s.ledger.Done(s.ledger.Add(1000, 1))

	// Decoded before any tracker exists, as it is when a crash replays it.
	s.feed(t, relationMsg(), beginMsg(300, 750), keyMoveMsg("1", "9"), commitMsg(300, 310))
	if got := s.ledger.Flushed(); got != 300 {
		t.Fatalf("reported position = %s, want it held at the move's commit LSN 0/12C", got)
	}
	if got := s.ledger.Delivered(); got != 1000 {
		t.Fatalf("delivered position = %s, a hold must not hide deliveries", got)
	}

	s.feed(t, markerMsg(t, 400, marker{Session: s.link.session, Kind: markerTrack, Generation: 1, Table: "public.users", Ref: 744}))
	s.link.register(usersChunk(1, 1, 744))
	s.feed(t, markerMsg(t, 500, marker{Session: s.link.session, Kind: markerChunk, Generation: 1, Chunk: 1}))
	if err := s.emitReads(); err != nil {
		t.Fatal(err)
	}
	if a := s.awaited; a == nil || len(a.result.orphans) != 1 || string(a.result.orphans[0]) != `{"id":9}` {
		t.Fatalf("awaited = %+v, want the moved key handed to the worker", a)
	}
	s.reportChunk()
	if got := s.ledger.Flushed(); got != 300 {
		t.Fatalf("reported position = %s, the hold was lifted before the row was read again", got)
	}

	s.link.register(usersChunk(1, 1, 760, "9"))
	<-s.link.results
	s.feed(t, markerMsg(t, 600, marker{Session: s.link.session, Kind: markerChunk, Generation: 1, Chunk: 1}))
	if err := s.emitReads(); err != nil {
		t.Fatal(err)
	}
	s.cur.Seq = s.ledger.Add(s.cur.AckLSN, len(s.cur.Buf))
	s.ledger.Done(s.cur.Seq)
	s.reportChunk()
	if got := s.ledger.Flushed(); got < 600 {
		t.Fatalf("reported position = %s, want the hold released once the moved row was delivered", got)
	}
}

func TestSession_stopMarkerReleasesTheHold(t *testing.T) {
	s := backfillSession(t)
	s.ledger.Done(s.ledger.Add(1000, 1))
	s.feed(t, relationMsg(), beginMsg(300, 750), keyMoveMsg("1", "9"), commitMsg(300, 310),
		markerMsg(t, 400, marker{Session: s.link.session, Kind: markerStop}))
	if got := s.ledger.Flushed(); got != 1000 || s.backfilling {
		t.Fatalf("reported = %s backfilling = %v, want the hold gone once no backfill is running", got, s.backfilling)
	}

	s.feed(t, beginMsg(1100, 751), keyMoveMsg("9", "10"), commitMsg(1100, 1110))
	s.ledger.Done(s.ledger.Add(2000, 1))
	if got := s.ledger.Flushed(); got != 2000 {
		t.Fatalf("reported = %s, a key move after the backfill must not hold acks", got)
	}
}
