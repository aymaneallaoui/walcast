package sink

import (
	"context"
	"crypto/tls"
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
)

type KafkaConfig struct {
	Brokers       []string
	TopicPrefix   string
	ClientID      string
	TLS           bool
	SASLMechanism string
	SASLUsername  string
	SASLPassword  string
	ClientOptions []kgo.Opt
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
	opts := []kgo.Opt{kgo.SeedBrokers(cfg.Brokers...), kgo.ClientID(cfg.ClientID)}
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

	client, err := kgo.NewClient(append(opts, cfg.ClientOptions...)...)
	if err != nil {
		return nil, fmt.Errorf("kafka client: %w", err)
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

// Send returns once every record is buffered; done fires after the last produce callback.
// Keys and values alias the batch, which the caller keeps alive until done.
func (k *Kafka) Send(ctx context.Context, b *event.Batch, done func(error)) {
	if len(b.Records) == 0 {
		done(nil)
		return
	}

	pending := &produceGroup{remaining: len(b.Records), done: done}
	records := make([]kgo.Record, len(b.Records))
	for i, r := range b.Records {
		records[i] = kgo.Record{Topic: k.topic(r.Table), Key: b.Key(r), Value: b.Value(r)}
		k.client.Produce(ctx, &records[i], pending.settle)
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
		topic = k.prefix + table
		k.topics[table] = topic
	}
	return topic
}

type produceGroup struct {
	mu        sync.Mutex
	remaining int
	err       error
	done      func(error)
}

func (g *produceGroup) settle(_ *kgo.Record, err error) {
	g.mu.Lock()
	if err != nil && g.err == nil {
		g.err = err
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
	var kafkaErr *kerr.Error
	if errors.As(err, &kafkaErr) && !kafkaErr.Retriable {
		return fmt.Errorf("%w: %w", ErrRejected, err)
	}
	return err
}
