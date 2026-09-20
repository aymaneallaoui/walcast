package event

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
)

const usersRelID = 42

func usersRelation() *pglogrepl.RelationMessage {
	return &pglogrepl.RelationMessage{
		RelationID:   usersRelID,
		Namespace:    "public",
		RelationName: "users",
		Columns: []*pglogrepl.RelationMessageColumn{
			{Flags: 1, Name: "id", DataType: oidInt8},
			{Name: "name", DataType: 25},
			{Name: "active", DataType: oidBool},
			{Name: "balance", DataType: oidNumeric},
			{Name: "meta", DataType: oidJSONB},
			{Name: "bio", DataType: 25},
		},
	}
}

func text(v string) *pglogrepl.TupleDataColumn {
	return &pglogrepl.TupleDataColumn{DataType: pglogrepl.TupleDataTypeText, Data: []byte(v)}
}

func null() *pglogrepl.TupleDataColumn {
	return &pglogrepl.TupleDataColumn{DataType: pglogrepl.TupleDataTypeNull}
}

func toast() *pglogrepl.TupleDataColumn {
	return &pglogrepl.TupleDataColumn{DataType: pglogrepl.TupleDataTypeToast}
}

func tuple(cols ...*pglogrepl.TupleDataColumn) *pglogrepl.TupleData {
	return &pglogrepl.TupleData{ColumnNum: uint16(len(cols)), Columns: cols}
}

func newTestEncoder() *Encoder {
	e := NewEncoder()
	e.Relation(usersRelation())
	e.Begin(&pglogrepl.BeginMessage{
		FinalLSN:   0x16B3748,
		CommitTime: time.Date(2026, 9, 20, 10, 0, 0, 0, time.UTC),
		Xid:        742,
	})
	return e
}

func decode(t *testing.T, b *Batch) map[string]any {
	t.Helper()
	var got map[string]any
	if err := json.Unmarshal(b.Buf, &got); err != nil {
		t.Fatalf("event is not valid JSON: %v\n%s", err, b.Buf)
	}
	return got
}

func TestInsertEncodesTypedValues(t *testing.T) {
	e := newTestEncoder()
	b := &Batch{}

	err := e.Insert(b, 0x16B3700, &pglogrepl.InsertMessage{
		RelationID: usersRelID,
		Tuple:      tuple(text("7"), text("ay\"ma\nne"), text("t"), text("12.50"), text(`{"k":[1,2]}`), null()),
	})
	if err != nil {
		t.Fatal(err)
	}

	got := decode(t, b)
	want := map[string]any{
		"table": "public.users", "op": "insert",
		"lsn": "0/16B3700", "commit_lsn": "0/16B3748",
		"seq": float64(0), "txid": float64(742), "ts": "2026-09-20T10:00:00Z",
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s = %v, want %v", k, got[k], v)
		}
	}

	row := got["new"].(map[string]any)
	if row["id"] != float64(7) || row["name"] != "ay\"ma\nne" || row["active"] != true || row["balance"] != "12.50" || row["bio"] != nil {
		t.Errorf("unexpected row: %v", row)
	}
	if meta := row["meta"].(map[string]any); len(meta["k"].([]any)) != 2 {
		t.Errorf("jsonb not embedded as JSON: %v", row["meta"])
	}
	if b.Events != 1 || b.Buf[len(b.Buf)-1] != '\n' {
		t.Errorf("events = %d, want 1 newline-terminated event", b.Events)
	}
}

func TestUpdateKeepsKeyOnlyOldRowAndListsUnchangedToast(t *testing.T) {
	e := newTestEncoder()
	b := &Batch{}

	err := e.Update(b, 1, &pglogrepl.UpdateMessage{
		RelationID:   usersRelID,
		OldTupleType: pglogrepl.UpdateMessageTupleTypeKey,
		OldTuple:     tuple(text("7"), null(), null(), null(), null(), null()),
		NewTuple:     tuple(text("7"), text("new"), text("f"), text("1"), null(), toast()),
	})
	if err != nil {
		t.Fatal(err)
	}

	got := decode(t, b)
	old := got["old"].(map[string]any)
	if len(old) != 1 || old["id"] != float64(7) {
		t.Errorf("old = %v, want only the key column", old)
	}
	if _, present := got["new"].(map[string]any)["bio"]; present {
		t.Error("unchanged TOAST column must be absent from new, not null")
	}
	if u := got["unchanged"].([]any); len(u) != 1 || u[0] != "bio" {
		t.Errorf("unchanged = %v, want [bio]", u)
	}
}

func TestSeqIncrementsWithinTransaction(t *testing.T) {
	e := newTestEncoder()
	b := &Batch{}
	del := &pglogrepl.DeleteMessage{
		RelationID:   usersRelID,
		OldTupleType: pglogrepl.DeleteMessageTupleTypeKey,
		OldTuple:     tuple(text("7"), null(), null(), null(), null(), null()),
	}

	for range 2 {
		if err := e.Delete(b, 1, del); err != nil {
			t.Fatal(err)
		}
	}

	dec := json.NewDecoder(bytesReader(b.Buf))
	for want := range 2 {
		var ev map[string]any
		if err := dec.Decode(&ev); err != nil {
			t.Fatal(err)
		}
		if ev["seq"] != float64(want) || ev["op"] != "delete" {
			t.Errorf("event %d: seq=%v op=%v", want, ev["seq"], ev["op"])
		}
	}
}

func TestRecordsCarryValueKeyAndTable(t *testing.T) {
	e := newTestEncoder()
	b := &Batch{}
	if err := e.Insert(b, 1, &pglogrepl.InsertMessage{RelationID: usersRelID, Tuple: tuple(text("7"), text("a"), text("t"), text("1"), null(), null())}); err != nil {
		t.Fatal(err)
	}
	del := &pglogrepl.DeleteMessage{RelationID: usersRelID, OldTupleType: pglogrepl.DeleteMessageTupleTypeKey, OldTuple: tuple(text("9"), null(), null(), null(), null(), null())}
	if err := e.Delete(b, 2, del); err != nil {
		t.Fatal(err)
	}
	if err := e.Truncate(b, 3, &pglogrepl.TruncateMessage{RelationIDs: []uint32{usersRelID}}); err != nil {
		t.Fatal(err)
	}

	wantKeys := []string{`{"id":7}`, `{"id":9}`, `"public.users"`}
	if len(b.Records) != 3 {
		t.Fatalf("got %d records, want 3", len(b.Records))
	}
	var joined []byte
	for i, r := range b.Records {
		if r.Table != "public.users" || string(b.Key(r)) != wantKeys[i] {
			t.Errorf("record %d: table=%q key=%s, want key %s", i, r.Table, b.Key(r), wantKeys[i])
		}
		if v := b.Value(r); !json.Valid(v) || v[len(v)-1] == '\n' {
			t.Errorf("record %d value is not one bare JSON object: %q", i, v)
		}
		joined = append(append(joined, b.Value(r)...), '\n')
	}
	if string(joined) != string(b.Buf) {
		t.Error("record values do not tile the batch buffer")
	}
}

func TestUpdateRebuildsToastedKeyFromOldTuple(t *testing.T) {
	e := newTestEncoder()
	b := &Batch{}
	err := e.Update(b, 1, &pglogrepl.UpdateMessage{
		RelationID:   usersRelID,
		OldTupleType: pglogrepl.UpdateMessageTupleTypeKey,
		OldTuple:     tuple(text("7"), null(), null(), null(), null(), null()),
		NewTuple:     tuple(toast(), text("new"), text("t"), text("1"), null(), null()),
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := string(b.Key(b.Records[0])); got != `{"id":7}` {
		t.Fatalf("key = %s, want the identity from the old tuple", got)
	}
}

func TestKeyIsNeverInvented(t *testing.T) {
	e := newTestEncoder()
	err := e.Update(&Batch{}, 1, &pglogrepl.UpdateMessage{
		RelationID: usersRelID,
		NewTuple:   tuple(toast(), text("new"), text("t"), text("1"), null(), null()),
	})
	if !errors.Is(err, ErrUnencodable) {
		t.Fatalf("err = %v, want ErrUnencodable", err)
	}
}

func TestRecordsSplitSchemaFromName(t *testing.T) {
	e := NewEncoder()
	rel := usersRelation()
	rel.Namespace, rel.RelationName = "a.b", "c"
	e.Relation(rel)
	e.Begin(&pglogrepl.BeginMessage{FinalLSN: 1, Xid: 1})
	b := &Batch{}
	if err := e.Insert(b, 1, &pglogrepl.InsertMessage{RelationID: usersRelID, Tuple: tuple(text("7"), text("a"), text("t"), text("1"), null(), null())}); err != nil {
		t.Fatal(err)
	}
	if r := b.Records[0]; r.Schema != "a.b" || r.Name != "c" || r.Table != "a.b.c" {
		t.Fatalf("record = %+v", r)
	}
}

func TestReplicaIdentityFullHasNoStableKey(t *testing.T) {
	e := NewEncoder()
	rel := usersRelation()
	rel.ReplicaIdentity = replicaIdentityFull
	for _, c := range rel.Columns {
		c.Flags = 1
	}
	e.Relation(rel)
	e.Begin(&pglogrepl.BeginMessage{FinalLSN: 1, Xid: 1})

	b := &Batch{}
	if err := e.Insert(b, 1, &pglogrepl.InsertMessage{RelationID: usersRelID, Tuple: tuple(text("7"), text("a"), text("t"), text("1"), null(), null())}); err != nil {
		t.Fatal(err)
	}
	if got := string(b.Key(b.Records[0])); got != `"public.users"` {
		t.Fatalf("key = %s, want the table name", got)
	}
}

func TestUnknownRelationIsUnencodable(t *testing.T) {
	e := NewEncoder()
	err := e.Insert(&Batch{}, 1, &pglogrepl.InsertMessage{RelationID: 1, Tuple: tuple()})
	if !errors.Is(err, ErrUnencodable) {
		t.Fatalf("err = %v, want ErrUnencodable", err)
	}
}

func BenchmarkInsert(b *testing.B) {
	e := newTestEncoder()
	batch := &Batch{Buf: make([]byte, 0, 4096)}
	msg := &pglogrepl.InsertMessage{
		RelationID: usersRelID,
		Tuple:      tuple(text("7"), text("aymane"), text("t"), text("12.50"), text(`{"k":[1,2]}`), null()),
	}

	b.ReportAllocs()
	for b.Loop() {
		batch.Buf, batch.Keys, batch.Records = batch.Buf[:0], batch.Keys[:0], batch.Records[:0]
		if err := e.Insert(batch, 0x16B3700, msg); err != nil {
			b.Fatal(err)
		}
	}
}
