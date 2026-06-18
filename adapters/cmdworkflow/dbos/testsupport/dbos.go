package testsupport

import (
	"context"
	"testing"
	"time"

	dbossdk "github.com/dbos-inc/dbos-transact-golang/dbos"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	pgadapter "github.com/laenenai/eventstore/adapters/storage/postgres"
)

// Env holds a running Postgres testcontainer, the shared pgxpool,
// the eventstore adapter, and a DBOSContext bound to the same
// database. Cleanup is wired via t.Cleanup — container terminates,
// pool closes, DBOS shuts down.
//
// The same pgxpool backs the eventstore (events, state_cache, outbox,
// subscriber_dlq, …) and DBOS's workflow journal (dbos.* schema).
// One database, one transaction story. Matches the headline DBOS
// advantage over external runtime processes.
type Env struct {
	Container testcontainers.Container
	Pool      *pgxpool.Pool
	DSN       string
	DCtx      dbossdk.DBOSContext
	Adapter   *pgadapter.Adapter
}

// Start launches a Postgres testcontainer, builds a pgxpool, and
// creates a DBOSContext that shares the same database. Returns the
// env with everything plumbed.
//
// The caller registers their workflows on env.DCtx BEFORE invoking
// env.DCtx.Launch() — DBOS requires registration-before-launch.
// Start does NOT call Launch; that's the test's responsibility once
// registrations are done.
func Start(t *testing.T) *Env {
	t.Helper()
	ctx := context.Background()

	pgContainer, err := postgres.Run(ctx,
		"postgres:17-alpine",
		postgres.WithDatabase("cmdworkflow_test"),
		postgres.WithUsername("test"),
		postgres.WithPassword("test"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60*time.Second),
		),
	)
	if err != nil {
		t.Fatalf("start Postgres container: %v", err)
	}
	t.Cleanup(func() { _ = pgContainer.Terminate(ctx) })

	dsn, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	t.Cleanup(pool.Close)

	// Eventstore migrations on the shared pool. DBOS's own schema
	// (dbos.*) is migrated by NewDBOSContext below — both run in
	// the same PG, in separate schemas, no collision.
	adapter := pgadapter.New(pool)
	if err := adapter.Migrate(ctx); err != nil {
		t.Fatalf("eventstore migrate: %v", err)
	}

	dctx, err := dbossdk.NewDBOSContext(ctx, dbossdk.Config{
		DatabaseURL:  dsn,
		AppName:      "eventstore-test",
		SystemDBPool: pool,
	})
	if err != nil {
		t.Fatalf("NewDBOSContext: %v", err)
	}
	// Drain DBOS workers before testcontainers terminates Postgres
	// (LIFO cleanup order: this t.Cleanup runs FIRST). 10s is generous
	// — the alternative is "context canceled" / "tx is closed" log
	// noise as queue runner, system_database listener, and reconciler
	// goroutines race the Postgres shutdown. The cost of a longer
	// drain is paid only on test exit; the cost of getting it wrong
	// is misleading log lines that mask real failures.
	t.Cleanup(func() { dctx.Shutdown(10 * time.Second) })

	return &Env{
		Container: pgContainer,
		Pool:      pool,
		DSN:       dsn,
		DCtx:      dctx,
		Adapter:   adapter,
	}
}
