// Package postgres is the Postgres storage adapter, satisfying the
// es.Store contract (see ADR 0017). Uses pgx/v5 and sqlc-generated
// queries; migrations run via goose with embedded SQL.
package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/laenenai/eventstore/adapters/storage/postgres/internal/db"
)

// Adapter implements es.Store against a PostgreSQL database. Operations
// run inside transactions on the provided pgx pool. The caller owns the
// pool's lifecycle; Close is a no-op.
type Adapter struct {
	pool         *pgxpool.Pool
	queries      *db.Queries
	adminPool    *pgxpool.Pool
	adminQueries *db.Queries
	// allowMainPoolForCrossTenant routes cross-tenant calls to the main
	// pool when no admin pool is configured. Set by WithoutRLSEnforcement
	// during the migration ramp described in ADR 0032.
	allowMainPoolForCrossTenant bool
	lockKey                     int64

	// drainLocks holds connections for currently-held session-level
	// advisory locks (es.DrainLocker contract). Populated lazily on
	// first TryAcquireDrainLock; survives until Close.
	drainLocks drainLockerState

	// projectionLocks is the analogous state for es.ProjectionLocker.
	// Separate namespace from drainLocks via projectionLockKey.
	projectionLocks drainLockerState
}

// ErrAdminPoolRequired is returned by cross-tenant code paths when the
// adapter was constructed without an admin pool. Per ADR 0032, the
// global-position cursor, cross-tenant outbox drain, cross-tenant
// state-cache invalidation, and admin tooling must run under the
// eventstore_admin role (BYPASSRLS) — there is no implicit upgrade
// from the tenant-scoped pool.
var ErrAdminPoolRequired = errors.New("postgres: admin pool required for cross-tenant operation (configure WithAdminPool)")

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

// WithAdminPool configures the connection pool used for cross-tenant
// operations (ADR 0032). The pool's connections must run as a role
// with BYPASSRLS — typically `eventstore_admin`. If unset, cross-tenant
// methods fail with ErrAdminPoolRequired unless WithoutRLSEnforcement
// is also set.
//
// The admin pool is a separate identity from the tenant-scoped pool by
// design: reviewers and operators see at a glance that a code path is
// operating outside the per-tenant boundary. There is no implicit
// upgrade.
func WithAdminPool(pool *pgxpool.Pool) Option {
	return func(a *Adapter) {
		a.adminPool = pool
		if pool != nil {
			a.adminQueries = db.New(pool)
		}
	}
}

// WithoutRLSEnforcement allows cross-tenant operations to run on the
// main pool when no admin pool is configured. It exists solely as the
// migration ramp described in ADR 0032 — operators upgrading from a
// single-pool deployment can ship the new binary first, then apply
// migration 00015 and wire up an admin pool in a separate change.
//
// Safe states:
//
//  1. Migration 00015 has not been applied. There are no RLS policies,
//     so cross-tenant queries on the main pool work normally.
//  2. Migration 00015 has been applied and the main pool's role has
//     BYPASSRLS (e.g., a superuser used for development, or a role
//     explicitly granted BYPASSRLS during a transitional period).
//     Cross-tenant queries bypass the policies.
//
// Unsafe state — DO NOT leave the option set:
//
//   - Migration 00015 has been applied and the main pool's role does
//     not have BYPASSRLS. Cross-tenant queries will return zero rows
//     (the RLS policy filters everything out because `app.tenant_id`
//     is unset), silently breaking the outbox publisher and any other
//     cross-tenant consumer. There will be no error to alert you.
//
// Remove this option once WithAdminPool is wired. Production deployments
// should never ship with it set after the role split is complete.
func WithoutRLSEnforcement() Option {
	return func(a *Adapter) { a.allowMainPoolForCrossTenant = true }
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

// withTenantTx runs fn inside a transaction with `app.tenant_id` bound
// to tenantID for the duration of the transaction. The RLS policies
// installed by migration 00015 evaluate against this setting; without
// it every query against a tenant-scoped table errors out (see ADR 0032).
//
// SET LOCAL is scoped to the transaction by Postgres — there is no risk
// of the binding leaking past commit or rollback onto the next checkout
// of the pooled connection.
func (a *Adapter) withTenantTx(ctx context.Context, tenantID string, fn func(*db.Queries) error) error {
	if tenantID == "" {
		return fmt.Errorf("postgres: tenant id required (ADR 0007)")
	}
	return pgx.BeginFunc(ctx, a.pool, func(tx pgx.Tx) error {
		// set_config(..., true) is the parameterized equivalent of
		// SET LOCAL — scoped to the transaction, no SQL-injection
		// surface from the tenant id.
		if _, err := tx.Exec(ctx, "SELECT set_config('app.tenant_id', $1, true)", tenantID); err != nil {
			return fmt.Errorf("bind tenant: %w", err)
		}
		return fn(a.queries.WithTx(tx))
	})
}

// admin returns the *db.Queries bound to the admin pool. When no admin
// pool is configured, the behavior depends on WithoutRLSEnforcement:
// if set, the main pool's queries are returned as a fallback (migration
// ramp per ADR 0032); otherwise ErrAdminPoolRequired surfaces.
func (a *Adapter) admin() (*db.Queries, error) {
	if a.adminQueries != nil {
		return a.adminQueries, nil
	}
	if a.allowMainPoolForCrossTenant {
		return a.queries, nil
	}
	return nil, ErrAdminPoolRequired
}

// runForTenant routes fn to either the tenant-bound app pool (when
// tenantID != "") or the admin pool (when tenantID == ""). The
// empty-tenant case is used by cross-tenant projectors and subscribers
// — projection_checkpoint and state_stream_subscribers both allow the
// '' default for that purpose. Admin tooling that inspects DLQs without
// pinning a tenant takes the same path. Both paths surface a clear
// error if their preconditions aren't met (ErrTenantMissing-like or
// ErrAdminPoolRequired).
func (a *Adapter) runForTenant(ctx context.Context, tenantID string, fn func(*db.Queries) error) error {
	if tenantID == "" {
		q, err := a.admin()
		if err != nil {
			return err
		}
		return fn(q)
	}
	return a.withTenantTx(ctx, tenantID, fn)
}

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
