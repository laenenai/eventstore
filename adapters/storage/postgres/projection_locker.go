package postgres

import (
	"context"
	"fmt"
	"hash/fnv"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/laenenai/eventstore/es"
)

// TryAcquireProjectionLock implements es.ProjectionLocker. Mirrors
// TryAcquireDrainLock but uses a distinct keyspace (the FNV-1a prefix
// below) so a projection and a drain with the same user-supplied key
// don't collide.
func (a *Adapter) TryAcquireProjectionLock(ctx context.Context, key string) (bool, error) {
	a.projectionLocks.mu.Lock()
	defer a.projectionLocks.mu.Unlock()

	if a.projectionLocks.conns == nil {
		a.projectionLocks.conns = map[string]*pgxpool.Conn{}
	}
	if _, held := a.projectionLocks.conns[key]; held {
		return false, nil
	}

	conn, err := a.pool.Acquire(ctx)
	if err != nil {
		return false, fmt.Errorf("acquire pool conn for projection lock %q: %w", key, err)
	}

	var acquired bool
	if err := conn.QueryRow(ctx,
		"SELECT pg_try_advisory_lock($1)",
		projectionLockKey(key),
	).Scan(&acquired); err != nil {
		conn.Release()
		return false, fmt.Errorf("pg_try_advisory_lock(%q): %w", key, err)
	}
	if !acquired {
		conn.Release()
		return false, nil
	}

	a.projectionLocks.conns[key] = conn
	return true, nil
}

// ReleaseProjectionLock implements es.ProjectionLocker.
func (a *Adapter) ReleaseProjectionLock(ctx context.Context, key string) error {
	a.projectionLocks.mu.Lock()
	defer a.projectionLocks.mu.Unlock()

	conn, held := a.projectionLocks.conns[key]
	if !held {
		return nil
	}
	delete(a.projectionLocks.conns, key)
	defer conn.Release()

	if _, err := conn.Exec(ctx,
		"SELECT pg_advisory_unlock($1)",
		projectionLockKey(key),
	); err != nil {
		return fmt.Errorf("pg_advisory_unlock(%q): %w", key, err)
	}
	return nil
}

// projectionLockKey derives a stable int64 from the user-supplied key.
// Distinct prefix from drainLockKey to keep the two lock namespaces
// from clashing if a user happens to pass the same key to both.
func projectionLockKey(key string) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte("eventstore.projection.runner:"))
	_, _ = h.Write([]byte(key))
	return int64(h.Sum64())
}

var _ es.ProjectionLocker = (*Adapter)(nil)
