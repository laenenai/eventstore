// Package outbox hosts the outbox-drain helper.
//
// See ADR 0014 for the outbox semantics and ADR 0012 for how the
// drain fits into the Profile B (scale-to-zero) delivery model:
// the writer commits events + outbox rows atomically; a scheduled
// drain wakes the DB on a cadence, pulls pending rows, hands each
// to the configured EventPublisher, marks rows published.
package outbox

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/laenenai/eventstore/es"
	"github.com/laenenai/eventstore/publisher"
)

// Drain is the outbox drain runtime. Holds a reference to an
// OutboxStore (a storage adapter implementing the outbox queries) and
// a Publisher (the configured EventPublisher).
type Drain struct {
	// Store provides the outbox queries. Implemented by storage
	// adapters (adapters/storage/sqlite, adapters/storage/postgres).
	Store es.OutboxStore

	// Publisher hands events to subscribers. Pluggable per ADR 0012:
	// inproc for tests; restate / nats / sns / pubsub / cfqueues for
	// production.
	Publisher publisher.Publisher

	// Tenant scopes the drain to a single tenant. Empty string means
	// cross-tenant (a shared scheduled drain across all tenants in
	// the database).
	Tenant string

	// BatchSize is the number of rows pulled per Run iteration.
	// Default 100. Larger batches amortize wake-up overhead;
	// smaller batches reduce blast radius on a stuck publisher.
	BatchSize int

	// CleanupRetention sets how long published rows are kept before
	// the drain's cleanup pass deletes them. Default 7 days.
	// Set to 0 to disable cleanup.
	CleanupRetention time.Duration

	// LockKey, if non-empty, enables concurrent-drain safety via the
	// store's es.DrainLocker interface. When set:
	//   - Run / RunOnce call TryAcquireDrainLock(LockKey) at start.
	//   - If acquired, the drain proceeds and releases at end.
	//   - If another instance holds the lock, Run / RunOnce return
	//     (0, 0, nil) immediately ("skipped this turn").
	//   - If the Store does not implement DrainLocker, LockKey is
	//     ignored (SQLite is naturally single-writer; Postgres
	//     implements it via pg_try_advisory_lock).
	//
	// Recommended values:
	//   "outbox-drain"           — global, single concurrent drainer
	//   "outbox-drain:"+Tenant   — per-tenant concurrent drainers
	//
	// See cookbook recipe 06 for the deployment patterns.
	LockKey string

	// Shard / TotalShards enable sharded draining. When TotalShards
	// > 0, this drain processes only rows where
	// (global_position % TotalShards) == Shard. Multiple drain
	// replicas can run concurrently on disjoint subsets without any
	// coordination — each replica is configured with its own Shard
	// value. Defaults (0, 0) mean "no sharding".
	//
	// Sharding is independent of LockKey. They can be combined
	// (LockKey = "outbox-drain:shard-N") if you want exactly-one
	// drainer per shard.
	Shard       int
	TotalShards int
}

const (
	defaultBatchSize        = 100
	defaultCleanupRetention = 7 * 24 * time.Hour
)

// Run pulls all currently-pending rows in batches, publishes each via
// the configured Publisher, marks published rows, and runs the
// cleanup pass. Returns when the pending set is empty (caught up) or
// ctx is cancelled.
//
// Run is the entrypoint for a scheduled drain job. Typical deployment
// invokes Run from a cron, Lambda, Cloudflare Worker scheduled
// trigger, or similar.
//
// If LockKey is set and the Store implements es.DrainLocker, Run
// acquires the lock first. If another instance holds it, Run returns
// (0, 0, nil) immediately.
//
// Returns the count of rows published and the count cleaned up.
func (d *Drain) Run(ctx context.Context) (published int, cleaned int, err error) {
	if err := d.validate(); err != nil {
		return 0, 0, err
	}

	locker, locked, err := d.acquireLock(ctx)
	if err != nil {
		return 0, 0, err
	}
	if d.LockKey != "" && locker != nil && !locked {
		return 0, 0, nil
	}
	if locked {
		defer func() { _ = locker.ReleaseDrainLock(ctx, d.LockKey) }()
	}

	// Drain pending rows in batches until the pending set is empty.
	for {
		if err := ctx.Err(); err != nil {
			return published, cleaned, nil
		}
		n, err := d.runBatch(ctx)
		if err != nil {
			return published, cleaned, err
		}
		if n == 0 {
			break
		}
		published += n
	}

	// Cleanup pass.
	if d.CleanupRetention > 0 {
		cutoff := time.Now().UTC().Add(-d.cleanupRetention())
		if c, err := d.Store.CleanupPublishedOutbox(ctx, d.Tenant, cutoff); err != nil {
			return published, cleaned, fmt.Errorf("outbox cleanup: %w", err)
		} else if c > 0 {
			cleaned = c
		}
	}

	return published, cleaned, nil
}

// RunOnce processes one batch of pending rows. Returns the count
// successfully published. A zero return means the pending set is
// empty OR every row in the batch failed.
//
// Useful for callers that want fine-grained control over the drain
// cadence (e.g., wrap with custom backoff between batches, integrate
// with rate-limited downstream publishers). Run is the standard
// scheduled-drain entrypoint that calls RunOnce in a loop until
// drained.
//
// If LockKey is set, RunOnce acquires/releases the lock the same way
// Run does. A drainer that wants the lock to span multiple RunOnce
// calls should acquire it manually via the store's DrainLocker.
func (d *Drain) RunOnce(ctx context.Context) (int, error) {
	if err := d.validate(); err != nil {
		return 0, err
	}
	locker, locked, err := d.acquireLock(ctx)
	if err != nil {
		return 0, err
	}
	if d.LockKey != "" && locker != nil && !locked {
		return 0, nil
	}
	if locked {
		defer func() { _ = locker.ReleaseDrainLock(ctx, d.LockKey) }()
	}
	return d.runBatch(ctx)
}

// runBatch is the unlocked single-batch primitive used by both Run
// and RunOnce. Callers are responsible for any locking.
func (d *Drain) runBatch(ctx context.Context) (int, error) {
	rows, err := d.Store.PendingOutbox(ctx, d.Tenant, d.batchSize())
	if err != nil {
		return 0, fmt.Errorf("outbox: read pending: %w", err)
	}
	if len(rows) == 0 {
		return 0, nil
	}

	var published int
	for _, row := range rows {
		// Client-side shard filter — keeps the framework agnostic to
		// store-side sharding support. Postgres adapters could push
		// this down to the WHERE clause in a future commit; for now
		// the simple filter works on any adapter.
		if d.TotalShards > 0 {
			if int(row.Envelope.GlobalPosition%uint64(d.TotalShards)) != d.Shard {
				continue
			}
		}

		if err := d.Publisher.Publish(ctx, row.Envelope); err != nil {
			// Mark as failed; the next drain run will retry.
			if markErr := d.Store.MarkOutboxFailed(ctx, row.Envelope.TenantID,
				row.Envelope.GlobalPosition, err.Error()); markErr != nil {
				return published, fmt.Errorf("outbox: mark failed: %w", markErr)
			}
			continue
		}
		if err := d.Store.MarkOutboxPublished(ctx, row.Envelope.TenantID,
			row.Envelope.GlobalPosition); err != nil {
			return published, fmt.Errorf("outbox: mark published: %w", err)
		}
		published++
	}
	return published, nil
}

// acquireLock returns (locker, locked, err). locker is non-nil iff
// the Store implements DrainLocker AND LockKey is set; locked is
// whether we successfully acquired.
func (d *Drain) acquireLock(ctx context.Context) (es.DrainLocker, bool, error) {
	if d.LockKey == "" {
		return nil, false, nil
	}
	locker, ok := d.Store.(es.DrainLocker)
	if !ok {
		// Store doesn't support locking — SQLite is naturally serial.
		return nil, false, nil
	}
	acquired, err := locker.TryAcquireDrainLock(ctx, d.LockKey)
	if err != nil {
		return locker, false, fmt.Errorf("acquire drain lock %q: %w", d.LockKey, err)
	}
	return locker, acquired, nil
}

func (d *Drain) validate() error {
	if d.Store == nil {
		return errors.New("outbox: Store is required")
	}
	if d.Publisher == nil {
		return errors.New("outbox: Publisher is required")
	}
	if d.TotalShards < 0 || d.Shard < 0 || (d.TotalShards > 0 && d.Shard >= d.TotalShards) {
		return fmt.Errorf("outbox: invalid sharding: Shard=%d TotalShards=%d", d.Shard, d.TotalShards)
	}
	return nil
}

func (d *Drain) batchSize() int {
	if d.BatchSize <= 0 {
		return defaultBatchSize
	}
	return d.BatchSize
}

func (d *Drain) cleanupRetention() time.Duration {
	if d.CleanupRetention <= 0 {
		return defaultCleanupRetention
	}
	return d.CleanupRetention
}
