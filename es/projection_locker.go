package es

import "context"

// ProjectionLocker is the projection-side mirror of DrainLocker.
// Postgres implements it via pg_try_advisory_lock (with a different
// keyspace from DrainLocker so the two don't collide); SQLite is a
// no-op (single-writer file lock already serializes).
//
// projection.Runtime auto-detects this interface from its Store and,
// when Runtime.LockKey is set, attempts acquisition at the start of
// Run/RunOnce. Losers exit cleanly with (0, nil). See ADR 0020
// decision 3f and cookbook recipe 06.
type ProjectionLocker interface {
	TryAcquireProjectionLock(ctx context.Context, key string) (bool, error)
	ReleaseProjectionLock(ctx context.Context, key string) error
}
