package config

import (
	"strings"
	"testing"
	"time"
)

func validConfig() Config {
	return Config{
		ShutdownTimeout:   time.Second,
		SettleTimeout:     time.Minute,
		SlotName:          "walcast_slot",
		PublicationName:   "walcast_pub",
		StateSchema:       "walcast_state",
		FeedbackInterval:  time.Second,
		ServerTimeout:     time.Minute,
		BatchMaxBytes:     1,
		BatchLinger:       time.Millisecond,
		InflightMaxBytes:  1,
		ReconnectMinDelay: time.Millisecond,
		ReconnectMaxDelay: time.Second,
	}
}

func TestValidateAcceptsDefaultsShape(t *testing.T) {
	cfg := validConfig()
	cfg.PublicationTables = []string{"users", "public.orders"}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidateAcceptsBackfillSelections(t *testing.T) {
	for name, tables := range map[string][]string{
		"off":       nil,
		"all":       {"all"},
		"qualified": {"public.users", "orders"},
	} {
		t.Run(name, func(t *testing.T) {
			cfg := validConfig()
			cfg.BackfillTables, cfg.BackfillChunkRows, cfg.BackfillChunkBytes = tables, 100, 1<<20
			if err := cfg.Validate(); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestValidateRejectsUnsafeIdentifiersAndBadBounds(t *testing.T) {
	cases := map[string]func(*Config){
		"slot with quote":           func(c *Config) { c.SlotName = "x'; DROP TABLE users; --" },
		"uppercase slot":            func(c *Config) { c.SlotName = "Walcast" },
		"publication with space":    func(c *Config) { c.PublicationName = "my pub" },
		"table with injection":      func(c *Config) { c.PublicationTables = []string{"users; DROP TABLE users"} },
		"table with three parts":    func(c *Config) { c.PublicationTables = []string{"a.b.c"} },
		"zero shutdown timeout":     func(c *Config) { c.ShutdownTimeout = 0 },
		"zero feedback interval":    func(c *Config) { c.FeedbackInterval = 0 },
		"server timeout too low":    func(c *Config) { c.ServerTimeout = c.FeedbackInterval },
		"overlong table name":       func(c *Config) { c.PublicationTables = []string{strings.Repeat("a", 64)} },
		"min delay above max":       func(c *Config) { c.ReconnectMinDelay = time.Minute },
		"non positive batch size":   func(c *Config) { c.BatchMaxBytes = 0 },
		"state schema with quote":   func(c *Config) { c.StateSchema = "x'; DROP SCHEMA public; --" },
		"state schema public":       func(c *Config) { c.StateSchema = "public" },
		"state schema pg prefix":    func(c *Config) { c.StateSchema = "pg_walcast" },
		"state schema catalog":      func(c *Config) { c.StateSchema = "information_schema" },
		"negative generation":       func(c *Config) { c.SlotRecreateGeneration = -1 },
		"metrics addr without port": func(c *Config) { c.MetricsAddr = "127.0.0.1" },
		"pprof without a listener":  func(c *Config) { c.PprofEnabled = true },
		"backfill table injection":  func(c *Config) { c.BackfillTables = []string{"users; DROP TABLE users"} },
		"backfill all mixed in": func(c *Config) {
			c.BackfillTables = []string{"all", "public.users"}
			c.BackfillChunkRows = 1
			c.BackfillChunkBytes = 1
		},
		"backfill zero chunk rows": func(c *Config) { c.BackfillTables = []string{"users"}; c.BackfillChunkBytes = 1 },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			cfg := validConfig()
			mutate(&cfg)
			if err := cfg.Validate(); err == nil {
				t.Fatal("expected a validation error")
			}
		})
	}
}

func TestLoadTrimsPublicationTables(t *testing.T) {
	t.Chdir(t.TempDir())
	t.Setenv("DATABASE_URL", "postgres://localhost/db")
	t.Setenv("PUBLICATION_TABLES", "public.a, public.b")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got := cfg.PublicationTables; len(got) != 2 || got[1] != "public.b" {
		t.Fatalf("tables = %q", got)
	}
}

func webhookConfig() Config {
	cfg := validConfig()
	cfg.Sink = SinkWebhook
	cfg.WebhookURL = "https://hooks.example.com/walcast"
	cfg.WebhookSecret = "0123456789abcdef"
	cfg.WebhookTimeout = time.Second
	cfg.WebhookRetryMin = time.Millisecond
	cfg.WebhookRetryMax = time.Second
	return cfg
}

func TestValidate_webhookSink(t *testing.T) {
	accepted := map[string]func(*Config){
		"https":                 func(*Config) {},
		"http to localhost":     func(c *Config) { c.WebhookURL = "http://localhost:8080/hook" },
		"http to 127.0.0.1":     func(c *Config) { c.WebhookURL = "http://127.0.0.1:8080/hook" },
		"http to ipv6 loopback": func(c *Config) { c.WebhookURL = "http://[::1]:8080/hook" },
	}
	for name, mutate := range accepted {
		t.Run("accepts "+name, func(t *testing.T) {
			cfg := webhookConfig()
			mutate(&cfg)
			if err := cfg.Validate(); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}

	rejected := map[string]func(*Config){
		"unknown sink":            func(c *Config) { c.Sink = "pulsar" },
		"missing url":             func(c *Config) { c.WebhookURL = "" },
		"relative url":            func(c *Config) { c.WebhookURL = "/hook" },
		"plain http to a remote":  func(c *Config) { c.WebhookURL = "http://hooks.example.com/walcast" },
		"loopback lookalike host": func(c *Config) { c.WebhookURL = "http://localhost.example.com/hook" },
		"credentials in url":      func(c *Config) { c.WebhookURL = "https://user:pass@hooks.example.com/" },
		"non http scheme":         func(c *Config) { c.WebhookURL = "ftp://hooks.example.com/" },
		"missing secret":          func(c *Config) { c.WebhookSecret = "" },
		"short secret":            func(c *Config) { c.WebhookSecret = "short" },
		"zero timeout":            func(c *Config) { c.WebhookTimeout = 0 },
		"retry max below min":     func(c *Config) { c.WebhookRetryMax = c.WebhookRetryMin / 2 },
	}
	for name, mutate := range rejected {
		t.Run("rejects "+name, func(t *testing.T) {
			cfg := webhookConfig()
			mutate(&cfg)
			if err := cfg.Validate(); err == nil {
				t.Fatal("expected a validation error")
			}
		})
	}
}

func TestValidate_stdoutIgnoresWebhookSettings(t *testing.T) {
	cfg := validConfig()
	cfg.Sink = SinkStdout
	cfg.WebhookURL = "not a url"
	if err := cfg.Validate(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func kafkaConfig() Config {
	cfg := validConfig()
	cfg.Sink = SinkKafka
	cfg.KafkaBrokers = []string{"broker-1.example.com:9093", "broker-2.example.com:9093"}
	cfg.KafkaTopicPrefix = "walcast."
	cfg.KafkaTLS = true
	cfg.KafkaMaxMessageBytes = 1000012
	return cfg
}

func TestValidate_kafkaSink(t *testing.T) {
	accepted := map[string]func(*Config){
		"tls to remote brokers": func(*Config) {},
		"plaintext to loopback": func(c *Config) { c.KafkaTLS = false; c.KafkaBrokers = []string{"localhost:9092", "127.0.0.1:9093"} },
		"scram over tls": func(c *Config) {
			c.KafkaSASLMechanism, c.KafkaSASLUsername, c.KafkaSASLPassword = "scram-sha-512", "walcast", "secret"
		},
		"empty topic prefix": func(c *Config) { c.KafkaTopicPrefix = "" },
	}
	for name, mutate := range accepted {
		t.Run("accepts "+name, func(t *testing.T) {
			cfg := kafkaConfig()
			mutate(&cfg)
			if err := cfg.Validate(); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}

	rejected := map[string]func(*Config){
		"no brokers":            func(c *Config) { c.KafkaBrokers = nil },
		"broker without port":   func(c *Config) { c.KafkaBrokers = []string{"broker.example.com"} },
		"plaintext to a remote": func(c *Config) { c.KafkaTLS = false },
		"plaintext with one remote": func(c *Config) {
			c.KafkaTLS = false
			c.KafkaBrokers = []string{"localhost:9092", "broker.example.com:9092"}
		},
		"unknown sasl mechanism":   func(c *Config) { c.KafkaSASLMechanism = "gssapi" },
		"sasl without credentials": func(c *Config) { c.KafkaSASLMechanism = "plain" },
		"sasl over plaintext loopback": func(c *Config) {
			c.KafkaTLS, c.KafkaBrokers = false, []string{"localhost:9092"}
			c.KafkaSASLMechanism, c.KafkaSASLUsername, c.KafkaSASLPassword = "plain", "u", "p"
		},
		"zero settle timeout":         func(c *Config) { c.SettleTimeout = 0 },
		"topic prefix with bad chars": func(c *Config) { c.KafkaTopicPrefix = "wal cast/" },
		"zero max message bytes":      func(c *Config) { c.KafkaMaxMessageBytes = 0 },
		"absurd max message bytes":    func(c *Config) { c.KafkaMaxMessageBytes = 1 << 30 },
	}
	for name, mutate := range rejected {
		t.Run("rejects "+name, func(t *testing.T) {
			cfg := kafkaConfig()
			mutate(&cfg)
			if err := cfg.Validate(); err == nil {
				t.Fatal("expected a validation error")
			}
		})
	}
}
