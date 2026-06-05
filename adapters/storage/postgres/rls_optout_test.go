package postgres_test

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/laenenai/eventstore/aggregate"
	"github.com/laenenai/eventstore/es"
	"github.com/laenenai/eventstore/estest"
	pgadapter "github.com/laenenai/eventstore/adapters/storage/postgres"
	counterv1 "github.com/laenenai/eventstore/gen/test/counter/v1"
)

// TestWithoutRLS_LowPrivRoleCanMigrate exercises the WithoutRLS opt-out
// end to end against a role that lacks CREATEROLE — the exact situation
// on managed Postgres (e.g. Neon), where the application role cannot
// create the eventstore_app / eventstore_admin roles.
//
// It first reproduces the failure: a full Migrate (RLS included) run by
// the low-privilege role fails at migration 00015 with "permission
// denied to create role". It then shows WithoutRLS migrating cleanly on
// the same database, leaves no roles and no RLS policy behind, and that
// the store is fully usable on the bare main pool.
//
// Self-contained: spins its own container so it does not disturb the
// shared RLS-enabled fixture in TestMain.
func TestWithoutRLS_LowPrivRoleCanMigrate(t *testing.T) {
	if os.Getenv("EVENTSTORE_SKIP_PG_TESTS") == "1" {
		t.Skip("EVENTSTORE_SKIP_PG_TESTS=1")
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
		t.Fatalf("start postgres container: %v", err)
	}
	t.Cleanup(func() { _ = container.Terminate(ctx) })

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}

	// Superuser pool: create a low-privilege login role that owns just
	// enough of schema public to run the DDL migrations, but cannot
	// create roles.
	super, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("superuser pool: %v", err)
	}
	defer super.Close()
	for _, stmt := range []string{
		"CREATE ROLE lowpriv LOGIN PASSWORD 'lowpriv' NOSUPERUSER NOCREATEROLE NOCREATEDB",
		"GRANT CREATE, USAGE ON SCHEMA public TO lowpriv",
	} {
		if _, err := super.Exec(ctx, stmt); err != nil {
			t.Fatalf("%q: %v", stmt, err)
		}
	}

	// Pool connecting AS lowpriv (its own credentials — not SET ROLE),
	// mirroring how a managed-Postgres app role connects.
	lowCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	lowCfg.ConnConfig.User = "lowpriv"
	lowCfg.ConnConfig.Password = "lowpriv"
	lowPool, err := pgxpool.NewWithConfig(ctx, lowCfg)
	if err != nil {
		t.Fatalf("lowpriv pool: %v", err)
	}
	defer lowPool.Close()

	// Phase A — reproduce the bug: full migration (RLS included) fails
	// at 00015 because lowpriv cannot CREATE ROLE.
	err = pgadapter.New(lowPool).Migrate(ctx)
	if err == nil {
		t.Fatalf("full Migrate as lowpriv unexpectedly succeeded; expected CREATE ROLE permission failure")
	}
	if !strings.Contains(err.Error(), "permission denied to create role") {
		t.Fatalf("full Migrate as lowpriv: got %v; want a CREATE ROLE permission error", err)
	}

	// Phase B — WithoutRLS migrates cleanly on the same database.
	// Migrations 1–14 are already applied (committed before 00015
	// failed); excluding 00015 leaves nothing that needs privilege.
	if err := pgadapter.New(lowPool, pgadapter.WithoutRLS()).Migrate(ctx); err != nil {
		t.Fatalf("WithoutRLS Migrate as lowpriv: %v", err)
	}

	// No eventstore roles were created.
	for _, role := range []string{"eventstore_app", "eventstore_admin"} {
		var exists bool
		if err := super.QueryRow(ctx,
			"SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $1)", role,
		).Scan(&exists); err != nil {
			t.Fatalf("check role %s: %v", role, err)
		}
		if exists {
			t.Errorf("role %q exists after WithoutRLS migrate; expected none", role)
		}
	}

	// No RLS policy / no row security forced on events.
	var rowSecurity bool
	if err := super.QueryRow(ctx,
		"SELECT relrowsecurity FROM pg_class WHERE relname = 'events'",
	).Scan(&rowSecurity); err != nil {
		t.Fatalf("check events relrowsecurity: %v", err)
	}
	if rowSecurity {
		t.Errorf("events has row security enabled after WithoutRLS migrate; expected disabled")
	}
	var policies int
	if err := super.QueryRow(ctx,
		"SELECT count(*) FROM pg_policies WHERE tablename = 'events'",
	).Scan(&policies); err != nil {
		t.Fatalf("count events policies: %v", err)
	}
	if policies != 0 {
		t.Errorf("events has %d policies after WithoutRLS migrate; expected 0", policies)
	}

	// The store is usable on the bare main pool: append + read back,
	// including a cross-tenant ReadAll (which WithoutRLS permits without
	// a separate admin pool, since there are no policies to bypass).
	rt := &aggregate.Runtime[counterState, counterv1.Command, counterv1.Event]{
		Store:   pgadapter.New(lowPool, pgadapter.WithoutRLS()),
		Decider: counterDecider,
		Codec:   counterv1.EventCodec{},
	}
	tenant := "without-rls-tenant"
	tctx := es.WithTenant(ctx, tenant)
	sid := estest.MustStream(t, tenant, "counter", "1")
	if _, err := rt.Handle(tctx, sid, &counterv1.Init{Min: 0, Max: 10, Initial: 0}); err != nil {
		t.Fatalf("append on WithoutRLS store: %v", err)
	}

	rows, err := pgadapter.New(lowPool, pgadapter.WithoutRLS()).ReadAll(ctx, 0, 100)
	if err != nil {
		t.Fatalf("ReadAll on WithoutRLS store: %v", err)
	}
	if len(rows) == 0 {
		t.Errorf("ReadAll returned 0 rows after an append; expected at least 1")
	}
}
