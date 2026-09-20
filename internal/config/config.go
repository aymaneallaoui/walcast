package config

import (
	"errors"
	"fmt"
	"io/fs"
	"net"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/joho/godotenv"
)

const (
	SinkStdout  = "stdout"
	SinkWebhook = "webhook"
	SinkKafka   = "kafka"

	saslPlain       = "plain"
	saslScramSHA256 = "scram-sha-256"
	saslScramSHA512 = "scram-sha-512"
	maxTopicLen     = 249

	maxKafkaMessageBytes = 100 << 20

	minWebhookSecretLen = 16
)

var (
	nameRE        = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)
	topicPrefixRE = regexp.MustCompile(`^[a-zA-Z0-9._-]*$`)
	tableRE       = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}(\.[a-z_][a-z0-9_]{0,62})?$`)
)

type Config struct {
	LogLevel        string        `env:"LOG_LEVEL" envDefault:"info"`
	LogFormat       string        `env:"LOG_FORMAT" envDefault:"json"`
	DatabaseURL     string        `env:"DATABASE_URL,notEmpty"`
	ShutdownTimeout time.Duration `env:"SHUTDOWN_TIMEOUT" envDefault:"10s"`

	SlotName          string   `env:"SLOT_NAME" envDefault:"walcast_slot"`
	PublicationName   string   `env:"PUBLICATION_NAME" envDefault:"walcast_pub"`
	PublicationTables []string `env:"PUBLICATION_TABLES"`

	FeedbackInterval time.Duration `env:"FEEDBACK_INTERVAL" envDefault:"5s"`
	ServerTimeout    time.Duration `env:"SERVER_TIMEOUT" envDefault:"60s"`
	BatchMaxBytes    int           `env:"BATCH_MAX_BYTES" envDefault:"65536"`
	BatchLinger      time.Duration `env:"BATCH_LINGER" envDefault:"5ms"`
	InflightMaxBytes int64         `env:"INFLIGHT_MAX_BYTES" envDefault:"67108864"`

	ReconnectMinDelay time.Duration `env:"RECONNECT_MIN_DELAY" envDefault:"500ms"`
	ReconnectMaxDelay time.Duration `env:"RECONNECT_MAX_DELAY" envDefault:"30s"`

	Sink            string        `env:"SINK" envDefault:"stdout"`
	WebhookURL      string        `env:"WEBHOOK_URL"`
	WebhookSecret   string        `env:"WEBHOOK_SECRET"`
	WebhookTimeout  time.Duration `env:"WEBHOOK_TIMEOUT" envDefault:"10s"`
	WebhookRetryMin time.Duration `env:"WEBHOOK_RETRY_MIN" envDefault:"500ms"`
	WebhookRetryMax time.Duration `env:"WEBHOOK_RETRY_MAX" envDefault:"30s"`

	KafkaBrokers         []string `env:"KAFKA_BROKERS"`
	KafkaTopicPrefix     string   `env:"KAFKA_TOPIC_PREFIX" envDefault:"walcast."`
	KafkaClientID        string   `env:"KAFKA_CLIENT_ID" envDefault:"walcast"`
	KafkaTLS             bool     `env:"KAFKA_TLS"`
	KafkaSASLMechanism   string   `env:"KAFKA_SASL_MECHANISM"`
	KafkaSASLUsername    string   `env:"KAFKA_SASL_USERNAME"`
	KafkaSASLPassword    string   `env:"KAFKA_SASL_PASSWORD"`
	KafkaMaxMessageBytes int32    `env:"KAFKA_MAX_MESSAGE_BYTES" envDefault:"1000012"`
}

func Load() (Config, error) {
	if err := godotenv.Load(); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return Config{}, fmt.Errorf("load .env: %w", err)
	}

	cfg, err := env.ParseAs[Config]()
	if err != nil {
		return Config{}, fmt.Errorf("parse env: %w", err)
	}
	for i, t := range cfg.PublicationTables {
		cfg.PublicationTables[i] = strings.TrimSpace(t)
	}
	for i, b := range cfg.KafkaBrokers {
		cfg.KafkaBrokers[i] = strings.TrimSpace(b)
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// Validate restricts identifiers to a safe charset because replication connections
// cannot use query parameters, so these names are interpolated into SQL.
func (c Config) Validate() error {
	if !nameRE.MatchString(c.SlotName) {
		return fmt.Errorf("invalid SLOT_NAME %q", c.SlotName)
	}
	if !nameRE.MatchString(c.PublicationName) {
		return fmt.Errorf("invalid PUBLICATION_NAME %q", c.PublicationName)
	}
	for _, t := range c.PublicationTables {
		if !tableRE.MatchString(t) {
			return fmt.Errorf("invalid table %q in PUBLICATION_TABLES", t)
		}
	}
	if c.ShutdownTimeout <= 0 {
		return errors.New("SHUTDOWN_TIMEOUT must be positive")
	}
	if c.ServerTimeout <= c.FeedbackInterval {
		return errors.New("SERVER_TIMEOUT must be above FEEDBACK_INTERVAL")
	}
	if c.FeedbackInterval <= 0 || c.BatchLinger <= 0 || c.BatchMaxBytes <= 0 || c.InflightMaxBytes <= 0 {
		return errors.New("FEEDBACK_INTERVAL, BATCH_LINGER, BATCH_MAX_BYTES and INFLIGHT_MAX_BYTES must be positive")
	}
	if c.ReconnectMinDelay <= 0 || c.ReconnectMaxDelay < c.ReconnectMinDelay {
		return errors.New("RECONNECT_MIN_DELAY must be positive and not above RECONNECT_MAX_DELAY")
	}
	return c.validateSink()
}

func (c Config) validateSink() error {
	switch c.Sink {
	case "", SinkStdout:
		return nil
	case SinkWebhook:
		return c.validateWebhook()
	case SinkKafka:
		return c.validateKafka()
	default:
		return fmt.Errorf("invalid SINK %q (want %s, %s or %s)", c.Sink, SinkStdout, SinkWebhook, SinkKafka)
	}
}

func (c Config) validateWebhook() error {
	u, err := url.Parse(c.WebhookURL)
	if err != nil || u.Host == "" {
		return errors.New("WEBHOOK_URL must be an absolute URL")
	}
	if u.Scheme != "https" && (u.Scheme != "http" || !isLoopback(u.Hostname())) {
		return errors.New("WEBHOOK_URL must use https, plain http is allowed for loopback hosts only")
	}
	if u.User != nil {
		return errors.New("WEBHOOK_URL must not carry credentials, requests are authenticated by WEBHOOK_SECRET")
	}
	if len(c.WebhookSecret) < minWebhookSecretLen {
		return fmt.Errorf("WEBHOOK_SECRET must be at least %d characters", minWebhookSecretLen)
	}
	if c.WebhookTimeout <= 0 || c.WebhookRetryMin <= 0 || c.WebhookRetryMax < c.WebhookRetryMin {
		return errors.New("WEBHOOK_TIMEOUT and WEBHOOK_RETRY_MIN must be positive, WEBHOOK_RETRY_MAX not below WEBHOOK_RETRY_MIN")
	}
	return nil
}

func (c Config) validateKafka() error {
	if len(c.KafkaBrokers) == 0 {
		return errors.New("KAFKA_BROKERS must list at least one host:port")
	}
	allLoopback := true
	for _, b := range c.KafkaBrokers {
		host, port, err := net.SplitHostPort(b)
		if err != nil || host == "" || port == "" {
			return fmt.Errorf("invalid broker %q in KAFKA_BROKERS, want host:port", b)
		}
		allLoopback = allLoopback && isLoopback(host)
	}
	if !c.KafkaTLS && !allLoopback {
		return errors.New("KAFKA_TLS must be true unless every broker is a loopback host")
	}

	switch c.KafkaSASLMechanism {
	case "":
	case saslPlain, saslScramSHA256, saslScramSHA512:
		if c.KafkaSASLUsername == "" || c.KafkaSASLPassword == "" {
			return errors.New("KAFKA_SASL_USERNAME and KAFKA_SASL_PASSWORD are required with KAFKA_SASL_MECHANISM")
		}
	default:
		return fmt.Errorf("invalid KAFKA_SASL_MECHANISM %q (want %s, %s or %s)", c.KafkaSASLMechanism, saslPlain, saslScramSHA256, saslScramSHA512)
	}

	if c.KafkaMaxMessageBytes <= 0 || c.KafkaMaxMessageBytes > maxKafkaMessageBytes {
		return fmt.Errorf("KAFKA_MAX_MESSAGE_BYTES must be between 1 and %d", maxKafkaMessageBytes)
	}
	if !topicPrefixRE.MatchString(c.KafkaTopicPrefix) || len(c.KafkaTopicPrefix) > maxTopicLen/2 {
		return fmt.Errorf("invalid KAFKA_TOPIC_PREFIX %q", c.KafkaTopicPrefix)
	}
	return nil
}

func isLoopback(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
