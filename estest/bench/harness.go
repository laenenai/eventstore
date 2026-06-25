package bench

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	pgadapter "github.com/laenenai/eventstore/adapters/storage/postgres"
)

// Harness wires a real PostgreSQL container, the framework's
// adapter, and the two-pool RLS split (`eventstore_app` + admin)
// the spike's scenarios drive load through. One Harness per test
// invocation; `t.Cleanup` tears the container down so concurrent
// runs don't share state.
//
// Why testcontainers and not a long-lived Postgres: the spike's
// scenarios mutate global state (autovacuum settings, lots of
// tenants, large state_cache). Each run wants a clean slate. The
// 10K smoke completes in ~2 minutes; the testcontainer overhead
// (~1s startup + ~1s migrations) is negligible.
//
// Skip env: EVENTSTORE_SKIP_PG_TESTS=1 (matches the postgres
// adapter's test pattern).
type Harness struct {
	Container testcontainers.Container
	DSN       string

	AppPool   *pgxpool.Pool
	AdminPool *pgxpool.Pool

	Adapter *pgadapter.Adapter
}

// Setup boots Postgres, migrates the schema (current main, including
// migration 16 if present on this branch), and returns a configured
// harness. Lifetime is bound to t.Cleanup.
func Setup(t *testing.T) *Harness {
	t.Helper()
	if os.Getenv("EVENTSTORE_SKIP_PG_TESTS") == "1" {
		t.Skip("EVENTSTORE_SKIP_PG_TESTS=1")
	}

	ctx := context.Background()
	container, err := postgres.Run(ctx,
		"postgres:17-alpine",
		postgres.WithDatabase("bench"),
		postgres.WithUsername("test"),
		postgres.WithPassword("test"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60*time.Second),
		),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	t.Cleanup(func() { _ = container.Terminate(ctx) })

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}

	// Bootstrap pool: superuser. Runs migrations + grants role
	// membership; then closed.
	bootstrap, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("bootstrap pool: %v", err)
	}
	if err := pgadapter.New(bootstrap).Migrate(ctx); err != nil {
		bootstrap.Close()
		t.Fatalf("migrate: %v", err)
	}
	for _, stmt := range []string{
		"GRANT eventstore_app TO test",
		"GRANT eventstore_admin TO test",
	} {
		if _, err := bootstrap.Exec(ctx, stmt); err != nil {
			bootstrap.Close()
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	bootstrap.Close()

	app, err := poolAs(ctx, dsn, "eventstore_app")
	if err != nil {
		t.Fatalf("app pool: %v", err)
	}
	t.Cleanup(app.Close)

	admin, err := poolAs(ctx, dsn, "eventstore_admin")
	if err != nil {
		t.Fatalf("admin pool: %v", err)
	}
	t.Cleanup(admin.Close)

	return &Harness{
		Container: container,
		DSN:       dsn,
		AppPool:   app,
		AdminPool: admin,
		Adapter:   pgadapter.New(app, pgadapter.WithAdminPool(admin)),
	}
}

// poolAs returns a pool whose connections SET ROLE on first use,
// matching the postgres adapter test pattern.
func poolAs(ctx context.Context, dsn, role string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	cfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		_, err := conn.Exec(ctx, fmt.Sprintf("SET ROLE %q", role))
		return err
	}
	// Larger pool than default — the load generator parallelises
	// writes and would otherwise queue on the default 4-conn cap.
	cfg.MaxConns = 32
	return pgxpool.NewWithConfig(ctx, cfg)
}
