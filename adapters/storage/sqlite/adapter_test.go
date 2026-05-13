package sqlite_test

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite" // pure-Go SQLite driver

	sqliteadapter "github.com/laenenai/eventstore/adapters/storage/sqlite"
	"github.com/laenenai/eventstore/es"
	"github.com/laenenai/eventstore/estest"
)

// The SQLite adapter is verified against the shared conformance suite
// in estest. Tests use modernc.org/sqlite (pure Go, no CGO) with a
// per-run temp-file DB; WAL mode and the goose state table behave as
// in production.

func TestConformance(t *testing.T) {
	dir := t.TempDir()
	dsn := fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)&_pragma=foreign_keys(ON)",
		filepath.Join(dir, "test.db"))

	sqlDB, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })

	a := sqliteadapter.New(sqlDB)
	if err := a.Migrate(context.Background()); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	estest.RunStoreConformance(t, func() es.Store { return a })
}
