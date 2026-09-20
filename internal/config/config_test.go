package config

import (
	"strings"
	"testing"
	"time"
)

func validConfig() Config {
	return Config{
		ShutdownTimeout:   time.Second,
		SlotName:          "walcast_slot",
		PublicationName:   "walcast_pub",
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

func TestValidateRejectsUnsafeIdentifiersAndBadBounds(t *testing.T) {
	cases := map[string]func(*Config){
		"slot with quote":         func(c *Config) { c.SlotName = "x'; DROP TABLE users; --" },
		"uppercase slot":          func(c *Config) { c.SlotName = "Walcast" },
		"publication with space":  func(c *Config) { c.PublicationName = "my pub" },
		"table with injection":    func(c *Config) { c.PublicationTables = []string{"users; DROP TABLE users"} },
		"table with three parts":  func(c *Config) { c.PublicationTables = []string{"a.b.c"} },
		"zero shutdown timeout":   func(c *Config) { c.ShutdownTimeout = 0 },
		"zero feedback interval":  func(c *Config) { c.FeedbackInterval = 0 },
		"server timeout too low":  func(c *Config) { c.ServerTimeout = c.FeedbackInterval },
		"overlong table name":     func(c *Config) { c.PublicationTables = []string{strings.Repeat("a", 64)} },
		"min delay above max":     func(c *Config) { c.ReconnectMinDelay = time.Minute },
		"non positive batch size": func(c *Config) { c.BatchMaxBytes = 0 },
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
