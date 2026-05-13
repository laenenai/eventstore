// Package sqlite is the SQLite/libSQL storage adapter, satisfying the
// es.Store contract. Driver-agnostic — accepts an already-opened
// *sql.DB. Consumers register the driver they need:
//
//   - modernc.org/sqlite for local dev / self-hosted single-node
//   - github.com/tursodatabase/libsql-client-go for Turso remote
//   - github.com/tursodatabase/go-libsql for Turso embedded replicas
//
// SQL is SQLite-compatible across all three drivers.
//
// See ADR 0017 (module layout), ADR 0018 (migrations + queries),
// ADR 0019 (SQLite driver strategy).
package sqlite
