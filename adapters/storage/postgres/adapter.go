// Package postgres is the Postgres storage adapter, satisfying the
// es.Store contract (see ADR 0017). Uses pgx/v5 and sqlc-generated
// queries; migrations run via goose with embedded SQL.
package postgres

import (
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/laenenai/eventstore/adapters/storage/postgres/internal/db"
)

// Adapter implements es.Store against a PostgreSQL database. Operations
// run inside transactions on the provided pgx pool. The caller owns the
// pool's lifecycle; Close is a no-op.
type Adapter struct {
	pool    *pgxpool.Pool
	queries *db.Queries
	lockKey int64

	// drainLocks holds connections for currently-held session-level
	// advisory locks (es.DrainLocker contract). Populated lazily on
	// first TryAcquireDrainLock; survives until Close.
	drainLocks drainLockerState

	// projectionLocks is the analogous state for es.ProjectionLocker.
	// Separate namespace from drainLocks via projectionLockKey.
	projectionLocks drainLockerState
}

// Option configures an Adapter.
type Option func(*Adapter)

// WithLockKey overrides the advisory-lock key used to serialize
// appends store-wide (ADR 0009).
//
// The default key is the right choice for normal deployments — every
// framework instance (across all services and replicas sharing the
// same database) using the default key serializes against every
// other, which is what HA requires.
//
// Override only in two cases:
//
//  1. Multiple independent eventstore deployments share one physical
//     database (rare; usually multi-tenancy handles this within one
//     deployment instead).
//  2. Another system in the same database uses the default key value
//     for unrelated advisory locks (extremely rare given the FNV-1a
//     derivation of the default).
func WithLockKey(key int64) Option {
	return func(a *Adapter) { a.lockKey = key }
}

// New constructs an Adapter against an existing pgxpool.Pool. The
// adapter does not assume migrations have been applied; call Migrate
// before first use.
func New(pool *pgxpool.Pool, opts ...Option) *Adapter {
	a := &Adapter{
		pool:    pool,
		queries: db.New(pool),
		lockKey: defaultLockKey,
	}
	for _, opt := range opts {
		opt(a)
	}
	return a
}

// Close releases adapter-local resources. The caller's pool is not
// closed — pool lifecycle is the caller's responsibility.
func (a *Adapter) Close() error { return nil }

// defaultLockKey is a stable 64-bit integer derived from the framework
// identifier. Computed via FNV-1a so it's reproducible across builds
// and unlikely to collide with application-level advisory locks.
//
// Value: FNV-1a("github.com/laenenai/eventstore:append").
var defaultLockKey int64 = fnv1aLockKey("github.com/laenenai/eventstore:append")

func fnv1aLockKey(s string) int64 {
	const (
		offset uint64 = 14695981039346656037
		prime  uint64 = 1099511628211
	)
	h := offset
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= prime
	}
	return int64(h)
}
