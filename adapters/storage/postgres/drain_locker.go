package postgres

import (
	"context"
	"fmt"
	"hash/fnv"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
)

// drainLocker holds dedicated pool connections for currently-held
// session-level advisory locks. Lock keys map to (int64, connection).
type drainLockerState struct {
	mu    sync.Mutex
	conns map[string]*pgxpool.Conn
}

// drainLocks is the Adapter's lock-state container. Lives on the
// Adapter so each adapter instance has its own state.
var _ = drainLockerState{} // satisfy the linter; embedded in Adapter

// TryAcquireDrainLock implements es.DrainLocker via Postgres'
// pg_try_advisory_lock at session scope. The lock is held by a
// connection acquired from the pool; the same key cannot be
// acquired twice by the same adapter instance.
func (a *Adapter) TryAcquireDrainLock(ctx context.Context, key string) (bool, error) {
	a.drainLocks.mu.Lock()
	defer a.drainLocks.mu.Unlock()

	if a.drainLocks.conns == nil {
		a.drainLocks.conns = map[string]*pgxpool.Conn{}
	}
	if _, held := a.drainLocks.conns[key]; held {
		// Already held by this adapter — treat as "another holder
		// has it" since there's only one concurrent drainer per key.
		return false, nil
	}

	conn, err := a.pool.Acquire(ctx)
	if err != nil {
		return false, fmt.Errorf("acquire pool conn for drain lock %q: %w", key, err)
	}

	var acquired bool
	if err := conn.QueryRow(ctx,
		"SELECT pg_try_advisory_lock($1)",
		drainLockKey(key),
	).Scan(&acquired); err != nil {
		conn.Release()
		return false, fmt.Errorf("pg_try_advisory_lock(%q): %w", key, err)
	}
	if !acquired {
		conn.Release()
		return false, nil
	}

	a.drainLocks.conns[key] = conn
	return true, nil
}

// ReleaseDrainLock releases the named session-level advisory lock
// and returns the underlying connection to the pool. Idempotent.
func (a *Adapter) ReleaseDrainLock(ctx context.Context, key string) error {
	a.drainLocks.mu.Lock()
	defer a.drainLocks.mu.Unlock()

	conn, held := a.drainLocks.conns[key]
	if !held {
		return nil
	}
	delete(a.drainLocks.conns, key)
	defer conn.Release()

	if _, err := conn.Exec(ctx,
		"SELECT pg_advisory_unlock($1)",
		drainLockKey(key),
	); err != nil {
		return fmt.Errorf("pg_advisory_unlock(%q): %w", key, err)
	}
	return nil
}

// drainLockKey converts the user-supplied string key into the int64
// that Postgres advisory-lock functions expect. FNV-1a 64-bit gives a
// stable mapping across processes.
func drainLockKey(key string) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte("eventstore.outbox.drain:"))
	_, _ = h.Write([]byte(key))
	return int64(h.Sum64())
}
