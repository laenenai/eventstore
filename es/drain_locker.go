package es

import "context"

// DrainLocker is implemented by storage adapters that support
// process-coordination locks for concurrent drain safety.
//
// Postgres implements it via pg_try_advisory_lock at session scope —
// the framework holds a dedicated connection from the pool while the
// lock is acquired. SQLite does NOT implement it: SQLite is single-
// writer at the file level, so concurrent drainers naturally serialize
// on database write access without needing extra coordination.
//
// outbox.Drain auto-detects this interface; consumers set
// Drain.LockKey to a stable string (typically the drain's purpose,
// e.g., "outbox-drain", or "outbox-drain:<tenant>" for tenant-scoped
// drains) and the Drain calls TryAcquireDrainLock at the start of
// Run, skips the run if another instance holds the lock, and releases
// at the end.
//
// See cookbook recipe 06 for the full deployment story.
type DrainLocker interface {
	// TryAcquireDrainLock attempts a non-blocking acquisition.
	// Returns:
	//   (true,  nil) — lock acquired; caller must Release later.
	//   (false, nil) — another holder; caller should skip this run.
	//   (false, err) — infrastructure failure (DB error, network).
	//
	// The same key may be used safely from multiple instances; only
	// one wins per acquire round. Re-acquiring while already held by
	// this same adapter instance returns (false, nil).
	TryAcquireDrainLock(ctx context.Context, key string) (bool, error)

	// ReleaseDrainLock releases a lock previously acquired with
	// TryAcquireDrainLock. No-op if the key is not currently held.
	// Idempotent.
	ReleaseDrainLock(ctx context.Context, key string) error
}
