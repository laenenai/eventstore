package postgres

import (
	"context"
	"embed"
	"fmt"
	"io/fs"

	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// rlsMigrationFile is the migration excluded by WithoutRLS — role
// creation + RLS policies (ADR 0032). It is the highest version, so
// dropping it leaves no gap in the goose sequence.
const rlsMigrationFile = "00015_rls_tenant_isolation.sql"

// excludeFS wraps an fs.FS and hides a set of files by base name from
// directory listings. goose discovers migrations via ReadDir, so a file
// dropped here is never collected or applied; Open passes through for
// the files that remain.
type excludeFS struct {
	fs.FS
	exclude map[string]bool
}

func (e excludeFS) ReadDir(name string) ([]fs.DirEntry, error) {
	entries, err := fs.ReadDir(e.FS, name)
	if err != nil {
		return nil, err
	}
	kept := entries[:0]
	for _, ent := range entries {
		if e.exclude[ent.Name()] {
			continue
		}
		kept = append(kept, ent)
	}
	return kept, nil
}

// Migrate applies framework-owned schema migrations idempotently.
//
// Goose handles concurrency on multi-instance deploys via its own
// advisory-lock acquisition — only one instance applies migrations at a
// time; others no-op.
//
// Migrations are embedded in the adapter binary; no external CLI is
// required at runtime (ADR 0018).
//
// When the adapter was constructed WithoutRLS, the RLS migration
// (00015) is excluded so no roles are created and no policies installed
// — see WithoutRLS.
func (a *Adapter) Migrate(ctx context.Context) error {
	sqlDB := stdlib.OpenDBFromPool(a.pool)
	defer sqlDB.Close()

	if err := goose.SetDialect(string(goose.DialectPostgres)); err != nil {
		return fmt.Errorf("postgres migrate: set dialect: %w", err)
	}

	var migrations fs.FS = migrationsFS
	if a.skipRLSMigration {
		migrations = excludeFS{FS: migrationsFS, exclude: map[string]bool{rlsMigrationFile: true}}
	}
	goose.SetBaseFS(migrations)

	if err := goose.UpContext(ctx, sqlDB, "migrations"); err != nil {
		return fmt.Errorf("postgres migrate: %w", err)
	}
	return nil
}
