// Package outbox hosts the outbox-drain helper.
//
// See ADR 0014 for the outbox semantics, ADR 0012 for the delivery
// model, and cookbook recipe 06 for the deployment patterns and
// DLQ operator story.
package outbox

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"time"

	"github.com/laenenai/eventstore/es"
	"github.com/laenenai/eventstore/publisher"
)

// Drain is the outbox drain runtime.
//
// Delivery contract (with default config):
//
//   - At-least-once delivery to the publisher.
//   - Per-stream order preserved: if event with version N in stream X
//     fails to publish, no event with version > N in stream X is
//     delivered until either N succeeds or N enters DLQ.
//   - Cross-stream interleaving is allowed — stream Y's events don't
//     wait for stream X's failures.
//   - When a row's attempts reach MaxAttempts (DLQ threshold), the
//     entire stream is quarantined: no further events from that
//     stream are delivered until operator action (replay or abandon
//     via the OutboxAdmin interface). This is the default; set
//     AutoResumeAfterDLQ=true to opt into "skip DLQ'd events and
//     continue the stream with a gap."
type Drain struct {
	// Store provides the outbox queries.
	Store es.OutboxStore

	// Publisher hands events to subscribers.
	Publisher publisher.Publisher

	// Tenant scopes the drain to a single tenant. Empty string means
	// cross-tenant.
	Tenant string

	// BatchSize is the number of rows pulled per batch. Default 100.
	BatchSize int

	// CleanupRetention sets how long published rows are kept before
	// the drain's cleanup pass deletes them. Default 7 days.
	// Set to 0 to disable cleanup.
	CleanupRetention time.Duration

	// LockKey enables concurrent-drain safety via es.DrainLocker.
	// See cookbook recipe 06.
	LockKey string

	// Shard / TotalShards enable sharded draining. Sharding is by
	// FNV-1a hash of (tenant_id, stream_id), so all events of a given
	// stream are always handled by the same shard — strict per-stream
	// ordering is preserved. See cookbook recipe 06.
	Shard       int
	TotalShards int

	// MaxAttempts caps retry count. After this many failed publish
	// attempts, the row enters DLQ state and the drain stops
	// retrying. Default 0 = unbounded retries.
	MaxAttempts int32

	// BackoffBase + BackoffMax control retry timing. After a failed
	// publish, next_attempt_at is set to:
	//
	//   now + min(BackoffMax, BackoffBase * 2^(attempts-1))
	//
	// Default (0, 0) = retry every drain cycle.
	// Recommended: BackoffBase = 1s, BackoffMax = 5m.
	BackoffBase time.Duration
	BackoffMax  time.Duration

	// AutoResumeAfterDLQ controls behavior when a stream has a row in
	// DLQ state.
	//
	// Default (false) — quarantine mode: the stream stays paused
	// until operator action releases the DLQ'd row. Subscribers
	// never see a gap; the system fails loud rather than silently
	// dropping events.
	//
	// True — skip mode: DLQ'd rows are skipped (the drain's
	// PendingOutbox query already excludes them via attempts <
	// MaxAttempts). Subsequent events in the same stream proceed,
	// leaving a gap that the subscriber's gap-fill must recover.
	AutoResumeAfterDLQ bool

	// OnDLQ is an optional callback fired when the drain observes
	// a row crossing into DLQ state. Use to wire alerts, metrics,
	// or audit events. Called from the drain goroutine; should not
	// block.
	OnDLQ func(row es.OutboxRow)
}

const (
	defaultBatchSize        = 100
	defaultCleanupRetention = 7 * 24 * time.Hour
)

// Run drains pending rows in batches until empty, then runs the
// cleanup pass. See cookbook recipe 06 for the deployment patterns.
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

	// halted = streams paused for this Run due to a transient failure
	// or quarantine. Persists across batches so per-stream order is
	// preserved across the entire drain run.
	halted := map[string]bool{}
	quarantined := map[string]bool{}
	if !d.AutoResumeAfterDLQ && d.MaxAttempts > 0 {
		qs, qerr := d.Store.QuarantinedStreams(ctx, d.Tenant, d.MaxAttempts)
		if qerr != nil {
			return published, cleaned, fmt.Errorf("outbox: read quarantined: %w", qerr)
		}
		for _, s := range qs {
			k := quarantineKey(s.TenantID, s.StreamID)
			quarantined[k] = true
			halted[k] = true
		}
	}

	for {
		if err := ctx.Err(); err != nil {
			return published, cleaned, nil
		}
		n, anyProcessed, err := d.runBatch(ctx, halted, quarantined)
		if err != nil {
			return published, cleaned, err
		}
		published += n
		if !anyProcessed {
			break
		}
	}

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

// RunOnce processes one batch of pending rows.
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
	halted := map[string]bool{}
	quarantined := map[string]bool{}
	if !d.AutoResumeAfterDLQ && d.MaxAttempts > 0 {
		qs, qerr := d.Store.QuarantinedStreams(ctx, d.Tenant, d.MaxAttempts)
		if qerr != nil {
			return 0, fmt.Errorf("outbox: read quarantined: %w", qerr)
		}
		for _, s := range qs {
			k := quarantineKey(s.TenantID, s.StreamID)
			quarantined[k] = true
			halted[k] = true
		}
	}
	n, _, err := d.runBatch(ctx, halted, quarantined)
	return n, err
}

// runBatch reads one page of pending rows and processes each in order.
// halted carries Run-scoped per-stream halt state (preserves ordering
// across batches). quarantined carries the DLQ'd-streams set (only
// populated when AutoResumeAfterDLQ=false). Both maps are mutated as
// failures occur. anyProcessed is true if any row was published or
// marked-failed — the caller uses it to decide whether more progress
// is possible.
func (d *Drain) runBatch(ctx context.Context, halted, quarantined map[string]bool) (int, bool, error) {
	rows, err := d.Store.PendingOutbox(ctx, d.Tenant, d.batchSize(), d.MaxAttempts)
	if err != nil {
		return 0, false, fmt.Errorf("outbox: read pending: %w", err)
	}
	if len(rows) == 0 {
		return 0, false, nil
	}

	var (
		published    int
		anyProcessed bool
	)
	for _, row := range rows {
		key := quarantineKey(row.Envelope.TenantID, row.Envelope.StreamID.Canonical())
		if d.TotalShards > 0 {
			if int(streamShard(key, d.TotalShards)) != d.Shard {
				continue
			}
		}
		if quarantined[key] || halted[key] {
			continue
		}

		if err := d.Publisher.Publish(ctx, row.Envelope); err != nil {
			nextAttempts := row.Attempts + 1
			nextAt := d.computeNextAttemptAt(nextAttempts)
			if markErr := d.Store.MarkOutboxFailed(ctx, row.Envelope.TenantID,
				row.Envelope.GlobalPosition, err.Error(), nextAt); markErr != nil {
				return published, anyProcessed, fmt.Errorf("outbox: mark failed: %w", markErr)
			}
			anyProcessed = true

			crossedDLQ := d.MaxAttempts > 0 && nextAttempts >= d.MaxAttempts
			if crossedDLQ {
				if d.OnDLQ != nil {
					row.Attempts = nextAttempts
					d.OnDLQ(row)
				}
				if !d.AutoResumeAfterDLQ {
					halted[key] = true
					quarantined[key] = true
				}
				// AutoResumeAfterDLQ=true: don't halt. The DLQ'd row
				// is excluded from future PendingOutbox queries
				// (attempts >= MaxAttempts), so subsequent rows of
				// this stream proceed in the same run.
			} else {
				// Transient failure: halt this stream for the rest
				// of this Run. With backoff>0 the row's next_attempt_at
				// is also in the future, so future Runs honor the delay.
				halted[key] = true
			}
			continue
		}
		if err := d.Store.MarkOutboxPublished(ctx, row.Envelope.TenantID,
			row.Envelope.GlobalPosition); err != nil {
			return published, anyProcessed, fmt.Errorf("outbox: mark published: %w", err)
		}
		published++
		anyProcessed = true
	}
	return published, anyProcessed, nil
}

// quarantineKey is the map key for per-stream quarantine state and
// also the input to streamShard — same key, same shard.
func quarantineKey(tenantID, streamID string) string {
	return tenantID + "|" + streamID
}

// streamShard returns which shard a stream belongs to. We shard by
// stream (FNV-1a hash of the canonical key) rather than by
// global_position so that all events of a given stream go to one
// drain — strict per-stream ordering would otherwise be impossible
// to preserve across shards.
func streamShard(streamKey string, totalShards int) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(streamKey))
	return h.Sum32() % uint32(totalShards)
}

// computeNextAttemptAt returns the next-retry-eligible time for a
// row that just failed. attemptsAfter is the value of attempts AFTER
// this failure is recorded.
func (d *Drain) computeNextAttemptAt(attemptsAfter int32) time.Time {
	if d.BackoffBase <= 0 {
		return time.Time{} // zero time = eligible immediately
	}
	// Exponential: base * 2^(attempts-1)
	delay := d.BackoffBase
	for i := int32(1); i < attemptsAfter; i++ {
		next := delay * 2
		if d.BackoffMax > 0 && next > d.BackoffMax {
			delay = d.BackoffMax
			break
		}
		delay = next
	}
	if d.BackoffMax > 0 && delay > d.BackoffMax {
		delay = d.BackoffMax
	}
	return time.Now().UTC().Add(delay)
}

func (d *Drain) acquireLock(ctx context.Context) (es.DrainLocker, bool, error) {
	if d.LockKey == "" {
		return nil, false, nil
	}
	locker, ok := d.Store.(es.DrainLocker)
	if !ok {
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
