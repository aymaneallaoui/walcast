package sink

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"

	"github.com/aymaneallaoui/walcast/internal/event"
)

const (
	SASLPlain       = "plain"
	SASLScramSHA256 = "scram-sha-256"
	SASLScramSHA512 = "scram-sha-512"

	kafkaCloseTimeout = 10 * time.Second
	kafkaPingTimeout  = 5 * time.Second
	maxTopicLen       = 249
	topicHashLen      = 4
)

var (
	generatedTopicRE = regexp.MustCompile(`-[0-9a-f]{8}$`)
	eventHeaderEnd   = []byte(`,"ts":`)
)

type tableID struct{ schema, name string }

type KafkaConfig struct {
	Brokers         []string
	TopicPrefix     string
	ClientID        string
	TLS             bool
	SASLMechanism   string
	SASLUsername    string
	SASLPassword    string
	MaxMessageBytes int32
	EmitTruncate    bool
	ClientOptions   []kgo.Opt
}

// Kafka produces one record per event to a topic per table. The client is idempotent with
// acks=all by default, so retries neither duplicate nor reorder records within a partition.
type Kafka struct {
	client *kgo.Client
	prefix string
	log    zerolog.Logger
	topics map[tableID]string

	emitTruncate bool
}

func NewKafka(cfg KafkaConfig, log zerolog.Logger) (*Kafka, error) {
	// Batches arrive already lingered upstream, so the client's default 10ms linger only adds
	// latency: measured 10.5ms vs 0.2ms per 256 event batch on an in-process broker.
	opts := []kgo.Opt{kgo.SeedBrokers(cfg.Brokers...), kgo.ClientID(cfg.ClientID), kgo.ProducerLinger(0)}
	if cfg.TLS {
		opts = append(opts, kgo.DialTLSConfig(&tls.Config{MinVersion: tls.VersionTLS12}))
	}
	mechanism, err := saslMechanism(cfg)
	if err != nil {
		return nil, err
	}
	if mechanism != nil {
		opts = append(opts, kgo.SASL(mechanism))
	}
	if cfg.MaxMessageBytes > 0 {
		opts = append(opts, kgo.ProducerBatchMaxBytes(cfg.MaxMessageBytes))
	}

	client, err := kgo.NewClient(append(opts, cfg.ClientOptions...)...)
	if err != nil {
		return nil, fmt.Errorf("kafka client: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(context.Background(), kafkaPingTimeout)
	defer cancel()
	if err := client.Ping(pingCtx); err != nil {
		log.Warn().Err(err).Strs("brokers", cfg.Brokers).Msg("kafka brokers unreachable at startup, deliveries will retry")
	}
	return &Kafka{client: client, prefix: cfg.TopicPrefix, log: log, topics: make(map[tableID]string), emitTruncate: cfg.EmitTruncate}, nil
}

func saslMechanism(cfg KafkaConfig) (sasl.Mechanism, error) {
	switch cfg.SASLMechanism {
	case "":
		return nil, nil
	case SASLPlain:
		return plain.Auth{User: cfg.SASLUsername, Pass: cfg.SASLPassword}.AsMechanism(), nil
	case SASLScramSHA256:
		return scram.Auth{User: cfg.SASLUsername, Pass: cfg.SASLPassword}.AsSha256Mechanism(), nil
	case SASLScramSHA512:
		return scram.Auth{User: cfg.SASLUsername, Pass: cfg.SASLPassword}.AsSha512Mechanism(), nil
	default:
		return nil, fmt.Errorf("unknown sasl mechanism %q", cfg.SASLMechanism)
	}
}

// Send returns once every record is buffered; done fires after the last produce callback. The
// method value is bound once: binding it per record cost one allocation per event (measured).
func (k *Kafka) Send(ctx context.Context, b *event.Batch, done func(error)) {
	records := make([]kgo.Record, 0, len(b.Records))
	for i, r := range b.Records {
		// A truncate is keyed by table while rows are keyed by identity, so a consumer can read it
		// after a later insert from another partition and wipe that new row.
		if r.Op == event.OpTruncate && !k.emitTruncate {
			k.log.Warn().Str("table", r.Table).Msg("truncate not sent to kafka, set KAFKA_EMIT_TRUNCATE=true to send it")
			b.Records[i].Skipped = true
			continue
		}
		records = append(records, kgo.Record{Topic: k.topic(r.Schema, r.Name), Key: b.Key(r), Value: b.Value(r)})
	}
	if len(records) == 0 {
		done(nil)
		return
	}

	pending := &produceGroup{remaining: len(records), done: done}
	settle := pending.settle
	for i := range records {
		k.client.Produce(ctx, &records[i], settle)
	}
}

func (k *Kafka) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), kafkaCloseTimeout)
	defer cancel()
	err := k.client.Flush(ctx)
	k.client.Close()
	return err
}

func (k *Kafka) topic(schema, name string) string {
	id := tableID{schema, name}
	topic, ok := k.topics[id]
	if !ok {
		topic = topicName(k.prefix, schema, name)
		if topic != k.prefix+schema+"."+name {
			k.log.Warn().Str("schema", schema).Str("table", name).Str("topic", topic).Msg("table name is not a legal kafka topic, using a sanitised name")
		}
		k.topics[id] = topic
	}
	return topic
}

// topicName keeps prefix.schema.table untouched only when that mapping cannot be ambiguous. Dots
// inside an identifier, illegal bytes, over 249 bytes, or a name shaped like a generated one get
// sanitised plus a hash of the exact (schema, table) pair, so distinct tables never share a topic.
func topicName(prefix, schema, name string) string {
	full := prefix + schema + "." + name
	plain := len(full) <= maxTopicLen && !generatedTopicRE.MatchString(full)
	for _, part := range [2]string{schema, name} {
		plain = plain && part != "" && !bytes.ContainsRune([]byte(part), '.')
	}
	sanitised := []byte(full)
	for i, c := range sanitised {
		if !legalTopicByte(c) {
			sanitised[i] = '_'
			plain = false
		}
	}
	if plain {
		return full
	}

	sum := sha256.Sum256([]byte(prefix + "\x00" + schema + "\x00" + name))
	suffix := "-" + hex.EncodeToString(sum[:topicHashLen])
	return string(sanitised[:min(len(sanitised), maxTopicLen-len(suffix))]) + suffix
}

func legalTopicByte(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '.' || c == '_' || c == '-'
}

type produceGroup struct {
	mu        sync.Mutex
	remaining int
	err       error
	done      func(error)
}

// settle keeps the first fatal error in preference to the first error: a transient failure on
// one record must not hide an authorization failure on another and turn a stop into a replay.
func (g *produceGroup) settle(r *kgo.Record, err error) {
	if err != nil {
		err = classifyKafkaError(fmt.Errorf("produce to %s, %d byte value, event %s: %w", r.Topic, len(r.Value), eventHeader(r.Value), err))
	}

	g.mu.Lock()
	if err != nil && (g.err == nil || errors.Is(err, ErrRejected) && !errors.Is(g.err, ErrRejected)) {
		g.err = err
	}
	g.remaining--
	finished, final := g.remaining == 0, g.err
	g.mu.Unlock()

	if finished {
		g.done(final)
	}
}

// eventHeader returns the event's leading fields (table, op, lsn, commit_lsn, seq, txid), which
// locate it exactly. The row key and row data stay out of errors because they can hold PII.
func eventHeader(value []byte) []byte {
	if i := bytes.Index(value, eventHeaderEnd); i > 0 {
		return append(value[:i:i], '}')
	}
	return []byte("{}")
}

// classifyKafkaError marks broker errors Kafka itself calls non-retriable (record too large,
// authorization, invalid topic) as rejections: replaying the same record fails the same way.
func classifyKafkaError(err error) error {
	if kafkaErr, ok := errors.AsType[*kerr.Error](err); ok && !kafkaErr.Retriable {
		return fmt.Errorf("%w: %w", ErrRejected, err)
	}
	return err
}
