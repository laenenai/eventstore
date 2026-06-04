package postgres_test

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
	"github.com/laenenai/eventstore/es"
	"github.com/laenenai/eventstore/estest"
)

// The Postgres adapter is verified against the shared conformance
// suite in estest. A real PG 17 instance runs in a testcontainers
// container shared across all subtests (TestMain sets up; each subtest
// uses a unique tenant id for isolation).
//
// Two pgxpool.Pools split the trust boundary at the database layer per
// ADR 0032:
//
//   appPool   — connections run as `eventstore_app` (no BYPASSRLS);
//               tenant binding via SET LOCAL app.tenant_id, issued by
//               the adapter inside each transaction, is required for
//               RLS-protected queries to return rows.
//   adminPool — connections run as `eventstore_admin` (BYPASSRLS);
//               used for cross-tenant code paths (global position
//               cursor, cross-tenant outbox drain, cross-tenant
//               state-cache invalidation, admin tooling).
//
// The testcontainer superuser (`test`) creates both roles via migration
// 00015, then grants role membership to itself so the AfterConnect
// hooks can SET ROLE on each new pooled connection.
//
// Set EVENTSTORE_SKIP_PG_TESTS=1 to skip when Docker isn't available.

var (
	adapter   *pgadapter.Adapter
	appPool   *pgxpool.Pool
	adminPool *pgxpool.Pool
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

	// Bootstrap pool runs as the testcontainer superuser to apply
	// migrations (which create the eventstore_app + eventstore_admin
	// roles) and to grant role membership to the test superuser.
	// Closed once setup is complete.
	bootstrap, err := pgxpool.New(ctx, dsn)
	if err != nil {
		_ = container.Terminate(ctx)
		fmt.Fprintf(os.Stderr, "pgxpool.New (bootstrap): %v\n", err)
		os.Exit(1)
	}
	if err := pgadapter.New(bootstrap).Migrate(ctx); err != nil {
		bootstrap.Close()
		_ = container.Terminate(ctx)
		fmt.Fprintf(os.Stderr, "migrate: %v\n", err)
		os.Exit(1)
	}
	for _, stmt := range []string{
		"GRANT eventstore_app TO test",
		"GRANT eventstore_admin TO test",
	} {
		if _, err := bootstrap.Exec(ctx, stmt); err != nil {
			bootstrap.Close()
			_ = container.Terminate(ctx)
			fmt.Fprintf(os.Stderr, "%s: %v\n", stmt, err)
			os.Exit(1)
		}
	}
	bootstrap.Close()

	appPool, err = newPoolAs(ctx, dsn, "eventstore_app")
	if err != nil {
		_ = container.Terminate(ctx)
		fmt.Fprintf(os.Stderr, "app pool: %v\n", err)
		os.Exit(1)
	}
	adminPool, err = newPoolAs(ctx, dsn, "eventstore_admin")
	if err != nil {
		appPool.Close()
		_ = container.Terminate(ctx)
		fmt.Fprintf(os.Stderr, "admin pool: %v\n", err)
		os.Exit(1)
	}

	adapter = pgadapter.New(appPool, pgadapter.WithAdminPool(adminPool))

	code := m.Run()

	appPool.Close()
	adminPool.Close()
	_ = container.Terminate(ctx)
	os.Exit(code)
}

// newPoolAs builds a pgxpool that issues SET ROLE on every new
// connection so every query inherits the role's privileges (including
// BYPASSRLS for eventstore_admin, no BYPASSRLS for eventstore_app).
// In production the equivalent is connecting with the role's own
// credentials; SET ROLE is the test-fixture shortcut that keeps the
// testcontainer's single-user setup intact.
func newPoolAs(ctx context.Context, dsn, roleName string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}
	cfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		_, err := conn.Exec(ctx, fmt.Sprintf("SET ROLE %q", roleName))
		return err
	}
	return pgxpool.NewWithConfig(ctx, cfg)
}

func TestConformance(t *testing.T) {
	estest.RunStoreConformance(t, func() es.Store { return adapter })
}
