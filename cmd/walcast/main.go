package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/rs/zerolog"

	"github.com/aymaneallaoui/walcast/internal/app"
	"github.com/aymaneallaoui/walcast/internal/config"
	"github.com/aymaneallaoui/walcast/internal/logger"
	"github.com/aymaneallaoui/walcast/internal/metrics"
	"github.com/aymaneallaoui/walcast/internal/sink"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "walcast:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	log, err := logger.New(os.Stderr, cfg.LogLevel, cfg.LogFormat)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	m := metrics.New()
	served := make(chan error, 1)
	if cfg.MetricsAddr == "" {
		close(served)
	} else {
		ln, err := metrics.Listen(cfg.MetricsAddr)
		if err != nil {
			return err
		}
		go func() {
			err := metrics.Serve(ctx, ln, m, cfg.PprofEnabled, log)
			if err != nil {
				log.Error().Err(err).Msg("metrics listener stopped, walcast keeps streaming without it")
			}
			served <- err
		}()
	}

	snk, err := newSink(cfg, log, m)
	if err != nil {
		return err
	}
	err = app.New(cfg, log, snk).WithMetrics(m).Run(ctx)
	stop()
	<-served
	return err
}

func newSink(cfg config.Config, log zerolog.Logger, m *metrics.Metrics) (sink.Sink, error) {
	switch cfg.Sink {
	case config.SinkWebhook:
		return sink.NewWebhook(sink.WebhookConfig{
			URL:      cfg.WebhookURL,
			Secret:   cfg.WebhookSecret,
			Timeout:  cfg.WebhookTimeout,
			RetryMin: cfg.WebhookRetryMin,
			RetryMax: cfg.WebhookRetryMax,
			OnRetry:  m.SinkRetries.Inc,
		}, log), nil
	case config.SinkKafka:
		return sink.NewKafka(sink.KafkaConfig{
			Brokers:         cfg.KafkaBrokers,
			TopicPrefix:     cfg.KafkaTopicPrefix,
			ClientID:        cfg.KafkaClientID,
			TLS:             cfg.KafkaTLS,
			SASLMechanism:   cfg.KafkaSASLMechanism,
			SASLUsername:    cfg.KafkaSASLUsername,
			SASLPassword:    cfg.KafkaSASLPassword,
			MaxMessageBytes: cfg.KafkaMaxMessageBytes,
			EmitTruncate:    cfg.KafkaEmitTruncate,
		}, log)
	default:
		return sink.NewWriter(os.Stdout), nil
	}
}
