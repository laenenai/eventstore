package sqlite

import (
	"context"
	"embed"
	"fmt"

	"github.com/pressly/goose/v3"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// Migrate applies framework-owned schema migrations idempotently.
//
// SQLite is single-writer at the file level, so goose's
// advisory-locking concerns from Postgres don't apply here — only one
// writer can be running at a time anyway.
//
// Migrations are embedded in the adapter binary; no external CLI is
// required at runtime (ADR 0018).
func (a *Adapter) Migrate(ctx context.Context) error {
	if err := goose.SetDialect(string(goose.DialectSQLite3)); err != nil {
		return fmt.Errorf("sqlite migrate: set dialect: %w", err)
	}
	goose.SetBaseFS(migrationsFS)

	if err := goose.UpContext(ctx, a.db, "migrations"); err != nil {
		return fmt.Errorf("sqlite migrate: %w", err)
	}
	return nil
}
