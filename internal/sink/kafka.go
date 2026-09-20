package sink

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
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
	maxLoggedKeyLen   = 64
)

type KafkaConfig struct {
	Brokers         []string
	TopicPrefix     string
	ClientID        string
	TLS             bool
	SASLMechanism   string
	SASLUsername    string
	SASLPassword    string
	MaxMessageBytes int32
	ClientOptions   []kgo.Opt
}

// Kafka produces one record per event to a topic per table. The client is idempotent with
// acks=all by default, so retries neither duplicate nor reorder records within a partition.
type Kafka struct {
	client *kgo.Client
	prefix string
	log    zerolog.Logger
	topics map[string]string
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
	return &Kafka{client: client, prefix: cfg.TopicPrefix, log: log, topics: make(map[string]string)}, nil
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
	if len(b.Records) == 0 {
		done(nil)
		return
	}

	pending := &produceGroup{remaining: len(b.Records), done: done}
	settle := pending.settle
	records := make([]kgo.Record, len(b.Records))
	for i, r := range b.Records {
		records[i] = kgo.Record{Topic: k.topic(r.Table), Key: b.Key(r), Value: b.Value(r)}
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

func (k *Kafka) topic(table string) string {
	topic, ok := k.topics[table]
	if !ok {
		topic = topicName(k.prefix, table)
		if topic != k.prefix+table {
			k.log.Warn().Str("table", table).Str("topic", topic).Msg("table name is not a legal kafka topic, using a sanitised name")
		}
		k.topics[table] = topic
	}
	return topic
}

// topicName keeps legal names untouched. Anything else (quoted identifiers, names over Kafka's
// 249 byte limit) is sanitised and gets a hash suffix so two different tables can never collide.
func topicName(prefix, table string) string {
	name := prefix + table
	legal := len(name) <= maxTopicLen && name != "." && name != ".."
	sanitised := []byte(name)
	for i, c := range sanitised {
		if !legalTopicByte(c) {
			sanitised[i] = '_'
			legal = false
		}
	}
	if legal {
		return name
	}

	sum := sha256.Sum256([]byte(name))
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

func (g *produceGroup) settle(r *kgo.Record, err error) {
	g.mu.Lock()
	if err != nil && g.err == nil {
		key := r.Key[:min(len(r.Key), maxLoggedKeyLen)]
		g.err = fmt.Errorf("produce to %s, key %s, %d byte value: %w", r.Topic, key, len(r.Value), err)
	}
	g.remaining--
	finished, first := g.remaining == 0, g.err
	g.mu.Unlock()

	if finished {
		g.done(classifyKafkaError(first))
	}
}

// classifyKafkaError marks broker errors Kafka itself calls non-retriable (record too large,
// authorization, invalid topic) as rejections: replaying the same record fails the same way.
func classifyKafkaError(err error) error {
	if kafkaErr, ok := errors.AsType[*kerr.Error](err); ok && !kafkaErr.Retriable {
		return fmt.Errorf("%w: %w", ErrRejected, err)
	}
	return err
}
