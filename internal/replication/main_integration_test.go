//go:build integration

package replication_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

const (
	postgresImage  = "postgres:17-alpine"
	startupTimeout = 2 * time.Minute
)

// TestMain starts a throwaway Postgres unless DATABASE_URL already points at one. Without Docker
// the tests skip locally and fail in CI, so a broken runner can never pass with zero tests run.
func TestMain(m *testing.M) {
	os.Exit(run(m))
}

func run(m *testing.M) int {
	if os.Getenv("DATABASE_URL") != "" {
		return m.Run()
	}

	ctx, cancel := context.WithTimeout(context.Background(), startupTimeout)
	defer cancel()
	container, err := postgres.Run(ctx, postgresImage,
		postgres.WithDatabase("walcast"),
		postgres.WithUsername("walcast"),
		postgres.WithPassword("walcast"),
		postgres.BasicWaitStrategies(),
		testcontainers.WithCmdArgs("-c", "wal_level=logical", "-c", "max_replication_slots=20", "-c", "max_wal_senders=20"),
		// Go 1.27's race detector misreports a race inside pgx's SCRAM handshake (crypto/pbkdf2, both
		// accesses on one goroutine), about 1 run in 30. Trust auth keeps that code out of the tests.
		testcontainers.WithEnv(map[string]string{"POSTGRES_HOST_AUTH_METHOD": "trust"}),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cannot start postgres container: %v\n", err)
		if os.Getenv("CI") != "" {
			return 1
		}
		return m.Run()
	}
	defer func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			fmt.Fprintf(os.Stderr, "terminate postgres container: %v\n", err)
		}
	}()

	url, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		fmt.Fprintf(os.Stderr, "postgres connection string: %v\n", err)
		return 1
	}
	if err := os.Setenv("DATABASE_URL", url); err != nil {
		fmt.Fprintf(os.Stderr, "set DATABASE_URL: %v\n", err)
		return 1
	}
	return m.Run()
}
