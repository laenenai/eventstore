// Package sqlite is the SQLite/libSQL storage adapter, satisfying the
// es.Store contract. Driver-agnostic — accepts an already-opened
// *sql.DB. See ADR 0019 for the driver selection rationale.
package sqlite

import (
	"database/sql"

	"github.com/laenenai/eventstore/adapters/storage/sqlite/internal/db"
)

// Adapter implements es.Store against any SQLite-compatible *sql.DB.
// Three drivers are recommended depending on deployment (ADR 0019):
//
//   - modernc.org/sqlite — pure-Go local files (dev, tests, single-node)
//   - tursodatabase/libsql-client-go — pure-Go remote Turso/sqld
//   - tursodatabase/go-libsql — CGO embedded-replica Turso
//
// The adapter does no driver detection; the caller registers and opens
// whatever driver they want and hands the *sql.DB in.
type Adapter struct {
	db      *sql.DB
	queries *db.Queries
}

// Option configures an Adapter.
type Option func(*Adapter)

// New constructs an Adapter against an opened *sql.DB.
// The adapter does not assume migrations have been applied; call
// Migrate before first use.
func New(sqlDB *sql.DB, opts ...Option) *Adapter {
	a := &Adapter{
		db:      sqlDB,
		queries: db.New(sqlDB),
	}
	for _, opt := range opts {
		opt(a)
	}
	return a
}

// Close releases adapter-local resources. The caller's *sql.DB is not
// closed — DB lifecycle is the caller's responsibility.
func (a *Adapter) Close() error { return nil }
