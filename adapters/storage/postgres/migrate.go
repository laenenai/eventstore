package postgres

import (
	"context"
	"embed"
	"fmt"

	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// Migrate applies framework-owned schema migrations idempotently.
//
// Goose handles concurrency on multi-instance deploys via its own
// advisory-lock acquisition — only one instance applies migrations at a
// time; others no-op.
//
// Migrations are embedded in the adapter binary; no external CLI is
// required at runtime (ADR 0018).
func (a *Adapter) Migrate(ctx context.Context) error {
	sqlDB := stdlib.OpenDBFromPool(a.pool)
	defer sqlDB.Close()

	if err := goose.SetDialect(string(goose.DialectPostgres)); err != nil {
		return fmt.Errorf("postgres migrate: set dialect: %w", err)
	}
	goose.SetBaseFS(migrationsFS)

	if err := goose.UpContext(ctx, sqlDB, "migrations"); err != nil {
		return fmt.Errorf("postgres migrate: %w", err)
	}
	return nil
}
