package logger

import (
	"fmt"
	"io"
	"time"

	"github.com/rs/zerolog"
)

const (
	FormatJSON    = "json"
	FormatConsole = "console"
)

func New(w io.Writer, level, format string) (zerolog.Logger, error) {
	lvl, err := zerolog.ParseLevel(level)
	if err != nil {
		return zerolog.Logger{}, fmt.Errorf("parse log level %q: %w", level, err)
	}

	switch format {
	case FormatJSON:
	case FormatConsole:
		w = zerolog.ConsoleWriter{Out: w, TimeFormat: time.RFC3339}
	default:
		return zerolog.Logger{}, fmt.Errorf("unknown log format %q (want %s or %s)", format, FormatJSON, FormatConsole)
	}

	return zerolog.New(w).Level(lvl).With().Timestamp().Logger(), nil
}
