package testsupport

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	dbossdk "github.com/dbos-inc/dbos-transact-golang/dbos"

	sqliteadapter "github.com/laenenai/eventstore/adapters/storage/sqlite"
)

// SqliteEnv mirrors Env but with a SQLite-backed eventstore + DBOS
// system database. Both share one *sql.DB handle pointing at the
// same SQLite file — DBOS lays down its tables alongside the
// framework's event log in one file, one transaction story. This is
// the headline "DBOS shares your DB" property from ADR 0026,
// extended to SQLite via the SqliteSystemDB field that landed in
// dbos-transact-golang v0.16.0.
//
// Spike status (ADR 0033, in progress): this exists to validate
// that the SqliteSystemDB hook actually works end-to-end against
// the framework's adapter. If the spike succeeds, this file
// becomes the canonical fixture for SQLite-backed cmdworkflow tests
// and ADR 0026 § 4's "SQLite + DBOS is not supported" caveat is
// retracted. If it fails, this file goes away and the caveat
// stands.
type SqliteEnv struct {
	DB      *sql.DB
	Path    string
	DCtx    dbossdk.DBOSContext
	Adapter *sqliteadapter.Adapter
}

// StartSQLite spins up a SQLite-backed environment ready for DBOS +
// eventstore tests. Unlike Start (which boots a Postgres
// testcontainer), nothing leaves the test process — no Docker, no
// containers, no network. That's the whole point of the spike: if
// this works, local DBOS demos run on `go test` alone.
//
// Caller registers workflows on env.DCtx BEFORE invoking
// env.DCtx.Launch(); StartSQLite does NOT call Launch. Same shape
// as Start.
//
// Cleanup is wired via t.Cleanup in LIFO order: DBOS shuts down
// first (drains workers), then the DB closes. The file in
// t.TempDir is removed by the test framework after the test ends.
func StartSQLite(t *testing.T) *SqliteEnv {
	t.Helper()
	ctx := context.Background()

	// File-backed (not :memory:) so multiple connections in the
	// pool see the same data. SQLite's :memory: is per-connection.
	path := filepath.Join(t.TempDir(), "spike.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("sql.Open sqlite: %v", err)
	}
	// SQLite + concurrent writers: single connection avoids the
	// "database is locked" surprise that bites multi-connection
	// pools on shared-cache mode. Production adopters tune their
	// own pool; the spike just wants determinism.
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })

	adapter := sqliteadapter.New(db)
	if err := adapter.Migrate(ctx); err != nil {
		t.Fatalf("eventstore migrate: %v", err)
	}

	dctx, err := dbossdk.NewDBOSContext(ctx, dbossdk.Config{
		AppName:        "eventstore-spike",
		SqliteSystemDB: db,
	})
	if err != nil {
		t.Fatalf("NewDBOSContext (SQLite): %v", err)
	}
	t.Cleanup(func() { dctx.Shutdown(10 * time.Second) })

	return &SqliteEnv{
		DB:      db,
		Path:    path,
		DCtx:    dctx,
		Adapter: adapter,
	}
}
