package config

import (
	"errors"
	"fmt"
	"io/fs"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/joho/godotenv"
)

type Config struct {
	LogLevel        string        `env:"LOG_LEVEL" envDefault:"info"`
	LogFormat       string        `env:"LOG_FORMAT" envDefault:"json"`
	DatabaseURL     string        `env:"DATABASE_URL,notEmpty"`
	ShutdownTimeout time.Duration `env:"SHUTDOWN_TIMEOUT" envDefault:"10s"`
}

func Load() (Config, error) {
	if err := godotenv.Load(); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return Config{}, fmt.Errorf("load .env: %w", err)
	}

	cfg, err := env.ParseAs[Config]()
	if err != nil {
		return Config{}, fmt.Errorf("parse env: %w", err)
	}
	return cfg, nil
}
