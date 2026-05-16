// Package commands implements esctl subcommands. Each subcommand
// resolves its store via Connect(), runs its read, and renders via
// the shared Renderer.
package commands

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	_ "modernc.org/sqlite"

	pgadapter "github.com/laenenai/eventstore/adapters/storage/postgres"
	sqliteadapter "github.com/laenenai/eventstore/adapters/storage/sqlite"
	"github.com/laenenai/eventstore/es"
)

// Store is the unified surface esctl needs across both adapters.
// Both *postgres.Adapter and *sqlite.Adapter satisfy it.
type Store interface {
	es.Store
	es.StateCacheReader
	es.StateCacheWriter
	es.OutboxAdmin
	es.ProjectionAdmin
}

// Closer is the cleanup side of Connect.
type Closer interface{ Close() error }

// Connect parses dbURL and returns a Store + a Closer. SchemeS:
//
//	postgres://, postgresql://       — Postgres via pgx
//	file:./path.db, sqlite:///path   — SQLite via modernc.org/sqlite
//	./events.db (no scheme)          — SQLite (path heuristic)
//
// The store is NOT migrated — esctl is read-only and assumes the
// schema is current.
func Connect(ctx context.Context, dbURL string) (Store, Closer, error) {
	driver, dsn, err := detectDriver(dbURL)
	if err != nil {
		return nil, nil, err
	}
	switch driver {
	case "postgres":
		pool, err := pgxpool.New(ctx, dsn)
		if err != nil {
			return nil, nil, fmt.Errorf("pgxpool.New: %w", err)
		}
		a := pgadapter.New(pool)
		return a, &pgxCloser{pool: pool}, nil
	case "sqlite":
		d, err := sql.Open("sqlite", dsn)
		if err != nil {
			return nil, nil, fmt.Errorf("sql.Open sqlite: %w", err)
		}
		a := sqliteadapter.New(d)
		return a, d, nil
	default:
		return nil, nil, fmt.Errorf("unsupported driver %q", driver)
	}
}

func detectDriver(dbURL string) (driver, dsn string, err error) {
	// Bare-path heuristic — no scheme, looks like a path.
	if !strings.Contains(dbURL, "://") {
		if dbURL == "" {
			return "", "", fmt.Errorf("empty --db")
		}
		return "sqlite", dbURL, nil
	}
	u, perr := url.Parse(dbURL)
	if perr != nil {
		return "", "", fmt.Errorf("parse --db: %w", perr)
	}
	switch u.Scheme {
	case "postgres", "postgresql":
		return "postgres", dbURL, nil
	case "sqlite":
		// sqlite:///path -> /path; sqlite://./rel -> ./rel
		return "sqlite", strings.TrimPrefix(dbURL, "sqlite://"), nil
	case "file":
		return "sqlite", strings.TrimPrefix(dbURL, "file:"), nil
	default:
		return "", "", fmt.Errorf("unknown DB scheme %q", u.Scheme)
	}
}

type pgxCloser struct{ pool *pgxpool.Pool }

func (c *pgxCloser) Close() error {
	c.pool.Close()
	return nil
}

// storeOverride is set by tests to swap in a fake Store instead of
// going through Connect(). Production code leaves it nil.
var storeOverride Store

// withStore is a helper every subcommand action calls — opens the
// store, runs fn, closes. Errors propagate.
func withStore(ctx context.Context, dbURL string,
	fn func(context.Context, Store) error,
) error {
	if storeOverride != nil {
		return fn(ctx, storeOverride)
	}
	st, closer, err := Connect(ctx, dbURL)
	if err != nil {
		return err
	}
	defer func() { _ = closer.Close() }()
	return fn(ctx, st)
}

