package sink

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/rs/zerolog"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/aymaneallaoui/walcast/internal/event"
)

const (
	usersRel  = 1
	ordersRel = 2
)

func relationMessage(id uint32, table string) *pglogrepl.RelationMessage {
	return &pglogrepl.RelationMessage{
		RelationID: id, Namespace: "public", RelationName: table,
		Columns: []*pglogrepl.RelationMessageColumn{{Flags: 1, Name: "id", DataType: 20}, {Name: "note", DataType: 25}},
	}
}

func insert(t *testing.T, enc *event.Encoder, b *event.Batch, rel uint32, id int, note string) {
	t.Helper()
	col := func(v string) *pglogrepl.TupleDataColumn {
		return &pglogrepl.TupleDataColumn{DataType: pglogrepl.TupleDataTypeText, Data: []byte(v)}
	}
	msg := &pglogrepl.InsertMessage{RelationID: rel, Tuple: &pglogrepl.TupleData{Columns: []*pglogrepl.TupleDataColumn{col(fmt.Sprint(id)), col(note)}}}
	if err := enc.Insert(b, 1, msg); err != nil {
		t.Fatal(err)
	}
}

func newEncoder() *event.Encoder {
	enc := event.NewEncoder()
	enc.Relation(relationMessage(usersRel, "users"))
	enc.Relation(relationMessage(ordersRel, "orders"))
	enc.Begin(&pglogrepl.BeginMessage{FinalLSN: 1, Xid: 1})
	return enc
}

func newKafka(t *testing.T, clusterOpts []kfake.Opt, clientOpts ...kgo.Opt) (*Kafka, *kfake.Cluster) {
	t.Helper()
	cluster, err := kfake.NewCluster(clusterOpts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cluster.Close)

	snk, err := NewKafka(KafkaConfig{
		Brokers: cluster.ListenAddrs(), TopicPrefix: "walcast.", ClientID: "walcast-test", ClientOptions: clientOpts,
	}, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = snk.Close() })
	return snk, cluster
}

func sendKafka(t *testing.T, ctx context.Context, snk *Kafka, b *event.Batch) error {
	t.Helper()
	result := make(chan error, 2)
	snk.Send(ctx, b, func(err error) { result <- err })
	select {
	case err := <-result:
		select {
		case <-result:
			t.Fatal("done called more than once")
		case <-time.After(50 * time.Millisecond):
		}
		return err
	case <-time.After(15 * time.Second):
		t.Fatal("done never called")
		return nil
	}
}

func consume(t *testing.T, cluster *kfake.Cluster, want int, topics ...string) []*kgo.Record {
	t.Helper()
	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(cluster.ListenAddrs()...),
		kgo.ConsumeTopics(topics...),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var records []*kgo.Record
	for len(records) < want {
		fetches := consumer.PollFetches(ctx)
		if err := fetches.Err0(); err != nil {
			t.Fatalf("consumed %d of %d records: %v", len(records), want, err)
		}
		records = append(records, fetches.Records()...)
	}
	return records
}

func TestKafka_Send(t *testing.T) {
	t.Run("routes each event to its table topic, keyed by row identity, in order per key", func(t *testing.T) {
		snk, cluster := newKafka(t, []kfake.Opt{kfake.SeedTopics(4, "walcast.public.users", "walcast.public.orders")})
		enc, b := newEncoder(), &event.Batch{}
		for i := range 20 {
			insert(t, enc, b, usersRel, i%2, fmt.Sprintf("u%d", i))
		}
		insert(t, enc, b, ordersRel, 7, "order")

		if err := sendKafka(t, context.Background(), snk, b); err != nil {
			t.Fatal(err)
		}

		orders := consume(t, cluster, 1, "walcast.public.orders")
		if string(orders[0].Key) != `{"id":7}` || string(orders[0].Value) != string(b.Value(b.Records[20])) {
			t.Fatalf("orders record key=%s value=%s", orders[0].Key, orders[0].Value)
		}

		partitionOf := map[string]int32{}
		lastSeq := map[string]int{}
		for _, r := range consume(t, cluster, 20, "walcast.public.users") {
			key := string(r.Key)
			if p, seen := partitionOf[key]; seen && p != r.Partition {
				t.Fatalf("key %s landed on partitions %d and %d", key, p, r.Partition)
			}
			partitionOf[key] = r.Partition

			var seq int
			if _, err := fmt.Sscanf(string(r.Value[bytes.Index(r.Value, []byte(`"note":"u`))+len(`"note":"u`):]), "%d", &seq); err != nil {
				t.Fatalf("cannot read sequence from %s: %v", r.Value, err)
			}
			if prev, seen := lastSeq[key]; seen && seq < prev {
				t.Fatalf("key %s saw u%d after u%d", key, seq, prev)
			}
			lastSeq[key] = seq
		}
		if len(partitionOf) != 2 {
			t.Fatalf("got keys %v, want two row identities", partitionOf)
		}
	})

	t.Run("missing topic is reported as retriable, never as a rejection", func(t *testing.T) {
		snk, _ := newKafka(t, nil, kgo.UnknownTopicRetries(0), kgo.MetadataMinAge(10*time.Millisecond))
		enc, b := newEncoder(), &event.Batch{}
		insert(t, enc, b, usersRel, 1, "x")

		err := sendKafka(t, context.Background(), snk, b)
		if err == nil || errors.Is(err, ErrRejected) {
			t.Fatalf("err = %v, want a retriable produce error", err)
		}
	})

	t.Run("cancelled context fails the batch", func(t *testing.T) {
		snk, _ := newKafka(t, []kfake.Opt{kfake.SeedTopics(1, "walcast.public.users")})
		enc, b := newEncoder(), &event.Batch{}
		insert(t, enc, b, usersRel, 1, "x")
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		if err := sendKafka(t, ctx, snk, b); !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want context.Canceled", err)
		}
	})

	t.Run("empty batch is acknowledged without producing", func(t *testing.T) {
		snk, _ := newKafka(t, nil)
		if err := sendKafka(t, context.Background(), snk, &event.Batch{}); err != nil {
			t.Fatal(err)
		}
	})
}

func TestClassifyKafkaError(t *testing.T) {
	if err := classifyKafkaError(nil); err != nil {
		t.Fatalf("nil became %v", err)
	}
	plain := errors.New("connection reset")
	if err := classifyKafkaError(plain); !errors.Is(err, plain) || errors.Is(err, ErrRejected) {
		t.Fatalf("transport error classified as %v", err)
	}
	if err := classifyKafkaError(kerr.MessageTooLarge); !errors.Is(err, ErrRejected) || !errors.Is(err, kerr.MessageTooLarge) {
		t.Fatalf("non-retriable broker error classified as %v", err)
	}
	if err := classifyKafkaError(kerr.NotLeaderForPartition); errors.Is(err, ErrRejected) {
		t.Fatalf("retriable broker error classified as %v", err)
	}
}

func TestNewKafka_rejectsUnknownSASLMechanism(t *testing.T) {
	if _, err := NewKafka(KafkaConfig{Brokers: []string{"127.0.0.1:1"}, SASLMechanism: "gssapi"}, zerolog.Nop()); err == nil {
		t.Fatal("expected an error")
	}
}

func TestTopicName(t *testing.T) {
	tests := []struct {
		name, prefix, schema, table string
		check                       func(t *testing.T, topic string)
	}{
		{"plain name is untouched", "walcast.", "public", "users", func(t *testing.T, topic string) {
			if topic != "walcast.public.users" {
				t.Fatalf("topic = %q", topic)
			}
		}},
		{"quoted identifier is sanitised and hashed", "walcast.", "public", "my table", func(t *testing.T, topic string) {
			if !strings.HasPrefix(topic, "walcast.public.my_table-") || len(topic) != len("walcast.public.my_table-")+8 {
				t.Fatalf("topic = %q", topic)
			}
		}},
		{"overlong name is cut to the limit", strings.Repeat("p", 124), strings.Repeat("a", 63), strings.Repeat("b", 63), func(t *testing.T, topic string) {
			if len(topic) != maxTopicLen {
				t.Fatalf("len = %d, want %d", len(topic), maxTopicLen)
			}
		}},
		{"non ascii bytes become underscores", "", "public", "café", func(t *testing.T, topic string) {
			for i := range len(topic) {
				if !legalTopicByte(topic[i]) {
					t.Fatalf("illegal byte in %q", topic)
				}
			}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) { tt.check(t, topicName(tt.prefix, tt.schema, tt.table)) })
	}

	distinct := [][2][2]string{
		{{"public", "my table"}, {"public", "my_table"}},
		{{"a.b", "c"}, {"a", "b.c"}},
		{{"public", "my table"}, {"public", strings.TrimPrefix(topicName("walcast.", "public", "my table"), "walcast.public.")}},
	}
	for _, pair := range distinct {
		a, b := topicName("walcast.", pair[0][0], pair[0][1]), topicName("walcast.", pair[1][0], pair[1][1])
		if a == b {
			t.Errorf("tables %q and %q collide on topic %q", pair[0], pair[1], a)
		}
	}
	if a, b := topicName("", "public", "my table"), topicName("", "public", "my table"); a != b {
		t.Fatalf("topic name is not stable: %q vs %q", a, b)
	}
}

func TestProduceGroup_settle(t *testing.T) {
	const value = `{"table":"public.docs","op":"insert","lsn":"0/1","commit_lsn":"0/2","seq":3,"txid":9,"ts":"2026-09-20T10:00:00Z","new":{"email":"jane@example.com"}}`
	record := func() *kgo.Record {
		return &kgo.Record{Topic: "walcast.public.docs", Key: []byte(`{"email":"jane@example.com"}`), Value: []byte(value)}
	}

	t.Run("error locates the event without leaking the row", func(t *testing.T) {
		var got error
		g := &produceGroup{remaining: 2, done: func(err error) { got = err }}
		g.settle(record(), kerr.MessageTooLarge)
		g.settle(record(), nil)

		if !errors.Is(got, ErrRejected) || !errors.Is(got, kerr.MessageTooLarge) {
			t.Fatalf("err = %v, want a rejection wrapping MESSAGE_TOO_LARGE", got)
		}
		for _, want := range []string{"walcast.public.docs", `"commit_lsn":"0/2"`, `"seq":3`, fmt.Sprint(len(value)) + " byte"} {
			if !strings.Contains(got.Error(), want) {
				t.Errorf("error %q does not mention %q", got, want)
			}
		}
		if strings.Contains(got.Error(), "jane@example.com") {
			t.Fatalf("error leaks row data: %v", got)
		}
	})

	for name, order := range map[string][2]error{
		"fatal after transient":  {kerr.NotLeaderForPartition, kerr.TopicAuthorizationFailed},
		"fatal before transient": {kerr.TopicAuthorizationFailed, kerr.NotLeaderForPartition},
	} {
		t.Run("fatal error wins, "+name, func(t *testing.T) {
			var got error
			g := &produceGroup{remaining: 2, done: func(err error) { got = err }}
			g.settle(record(), order[0])
			g.settle(record(), order[1])
			if !errors.Is(got, ErrRejected) || !errors.Is(got, kerr.TopicAuthorizationFailed) {
				t.Fatalf("err = %v, want the authorization failure as a rejection", got)
			}
		})
	}
}

func BenchmarkKafka_Send(b *testing.B) {
	b.Run("no linger", func(b *testing.B) { benchmarkKafkaSend(b) })
	b.Run("client default 10ms linger", func(b *testing.B) { benchmarkKafkaSend(b, kgo.ProducerLinger(10*time.Millisecond)) })
}

func benchmarkKafkaSend(b *testing.B, opts ...kgo.Opt) {
	cluster, err := kfake.NewCluster(kfake.SeedTopics(4, "walcast.public.users"))
	if err != nil {
		b.Fatal(err)
	}
	defer cluster.Close()
	snk, err := NewKafka(KafkaConfig{Brokers: cluster.ListenAddrs(), TopicPrefix: "walcast.", ClientID: "bench", ClientOptions: opts}, zerolog.Nop())
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = snk.Close() }()

	const events = 256
	enc, batch := newEncoder(), &event.Batch{}
	for i := range events {
		col := func(v string) *pglogrepl.TupleDataColumn {
			return &pglogrepl.TupleDataColumn{DataType: pglogrepl.TupleDataTypeText, Data: []byte(v)}
		}
		msg := &pglogrepl.InsertMessage{RelationID: usersRel, Tuple: &pglogrepl.TupleData{Columns: []*pglogrepl.TupleDataColumn{col(fmt.Sprint(i)), col("note")}}}
		if err := enc.Insert(batch, 1, msg); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	for b.Loop() {
		settled := make(chan error, 1)
		snk.Send(context.Background(), batch, func(err error) { settled <- err })
		if err := <-settled; err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/events, "ns/event")
}
