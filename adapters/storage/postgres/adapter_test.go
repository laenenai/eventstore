package postgres_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	pgadapter "github.com/laenenai/eventstore/adapters/storage/postgres"
	"github.com/laenenai/eventstore/es"
	"github.com/laenenai/eventstore/estest"
)

// The Postgres adapter is verified against the shared conformance
// suite in estest. A real PG 17 instance runs in a testcontainers
// container shared across all subtests (TestMain sets up; each subtest
// uses a unique tenant id for isolation).
//
// Set EVENTSTORE_SKIP_PG_TESTS=1 to skip when Docker isn't available.

var (
	adapter *pgadapter.Adapter
	pool    *pgxpool.Pool
)

func TestMain(m *testing.M) {
	if os.Getenv("EVENTSTORE_SKIP_PG_TESTS") == "1" {
		fmt.Println("skipping postgres adapter tests (EVENTSTORE_SKIP_PG_TESTS=1)")
		os.Exit(0)
	}

	ctx := context.Background()

	container, err := postgres.Run(ctx,
		"postgres:17-alpine",
		postgres.WithDatabase("eventstore_test"),
		postgres.WithUsername("test"),
		postgres.WithPassword("test"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60*time.Second),
		),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "start postgres container: %v\n", err)
		os.Exit(1)
	}

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		_ = container.Terminate(ctx)
		fmt.Fprintf(os.Stderr, "connection string: %v\n", err)
		os.Exit(1)
	}

	pool, err = pgxpool.New(ctx, dsn)
	if err != nil {
		_ = container.Terminate(ctx)
		fmt.Fprintf(os.Stderr, "pgxpool.New: %v\n", err)
		os.Exit(1)
	}

	adapter = pgadapter.New(pool)
	if err := adapter.Migrate(ctx); err != nil {
		pool.Close()
		_ = container.Terminate(ctx)
		fmt.Fprintf(os.Stderr, "migrate: %v\n", err)
		os.Exit(1)
	}

	code := m.Run()

	pool.Close()
	_ = container.Terminate(ctx)
	os.Exit(code)
}

func TestConformance(t *testing.T) {
	estest.RunStoreConformance(t, func() es.Store { return adapter })
}
