package config

import (
	"errors"
	"fmt"
	"io/fs"
	"regexp"
	"strings"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/joho/godotenv"
)

var (
	nameRE  = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)
	tableRE = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}(\.[a-z_][a-z0-9_]{0,62})?$`)
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
	return nil
}
