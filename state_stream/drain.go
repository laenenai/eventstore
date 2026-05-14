package state_stream

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"time"

	"github.com/laenenai/eventstore/es"
)

// Drain reads state_cache rows where the named subscriber is behind,
// delivers each as a StateEnvelope, and advances the subscriber's
// position. Mirrors outbox.Drain's deployment surface so operators
// learn one pattern (cookbook recipe 06). See ADR 0024.
type Drain struct {
	// SubscriberName identifies the position rows this drain
	// advances. Stable across deploys. Distinct subscribers using
	// the same Store may run different Drains independently.
	SubscriberName string

	// Store provides the state_stream query surface plus the
	// StateCacheReader (used as the source of truth for the state
	// bytes). Both shipped adapters implement both contracts.
	Store es.Store

	// Publisher delivers each StateEnvelope. A returned error keeps
	// the subscriber's position at its previous value; the next
	// drain cycle re-attempts with the latest state at that point
	// (coalescing-on-retry — ADR 0024 § 2).
	Publisher es.StatePublisher

	// Tenant scopes the drain to a single tenant. Empty string runs
	// cross-tenant; recommended for admin-scope subscribers, less
	// common for app-side subscribers.
	Tenant string

	// BatchSize bounds the streams pulled per Run iteration.
	// Default 100.
	BatchSize int

	// LockKey enables single-runner safety across replicas. When set
	// and the Store implements es.ProjectionLocker, RunOnce performs
	// a non-blocking acquisition at the start of each cycle and
	// exits cleanly with no deliveries if another instance holds it.
	// Same advisory-lock primitive as the outbox + projection
	// runtimes — distinct keyspace ("eventstore.state_stream.drain")
	// so it doesn't collide.
	LockKey string

	// Shard / TotalShards partition delivery work across runners.
	// FNV-1a hash of (tenant_id|stream_id) % TotalShards — same
	// stream-sticky scheme as outbox.Drain. Streams always go to
	// the same shard so any per-stream invariant (e.g., position
	// monotonicity) holds.
	//
	// Default (0, 0) = no sharding.
	Shard       int
	TotalShards int

	// OnDeliveryError is invoked when Publisher returns an error
	// for a particular stream. Useful for metrics / logging /
	// alerting. The drain itself does not block on failures — it
	// counts them and continues with the rest of the batch.
	OnDeliveryError func(env es.StateEnvelope, err error)
}

const defaultBatchSize = 100

// Run pulls batches until either pending=0 or ctx is cancelled.
// Returns the count of successful deliveries (across all batches in
// this Run) and any top-level error.
//
// Per-delivery failures don't propagate as errors — they're reported
// via OnDeliveryError and reflected in the Failed count from
// RunOnce. Coalescing-on-retry means the next Run delivers the
// latest state for each previously-failed stream.
func (d *Drain) Run(ctx context.Context) (int, error) {
	if err := d.validate(); err != nil {
		return 0, err
	}

	locker, acquired, err := d.acquireLock(ctx)
	if err != nil {
		return 0, err
	}
	if d.LockKey != "" && locker != nil && !acquired {
		return 0, nil // another runner holds the lock — clean exit
	}
	if acquired {
		defer func() { _ = locker.ReleaseProjectionLock(ctx, d.lockKey()) }()
	}

	var total int
	for {
		if err := ctx.Err(); err != nil {
			return total, nil
		}
		r, err := d.runBatch(ctx)
		if err != nil {
			return total, err
		}
		total += r.Delivered
		if r.Delivered+r.Failed == 0 {
			// Either no pending streams or every stream in the
			// batch was outside our shard. Either way we're done
			// for this Run.
			break
		}
	}
	return total, nil
}

// RunOnce processes one batch and returns. Useful for tests and for
// scheduled-cron deployments where the host drives the loop.
func (d *Drain) RunOnce(ctx context.Context) (RunResult, error) {
	if err := d.validate(); err != nil {
		return RunResult{}, err
	}
	locker, acquired, err := d.acquireLock(ctx)
	if err != nil {
		return RunResult{}, err
	}
	if d.LockKey != "" && locker != nil && !acquired {
		return RunResult{}, nil
	}
	if acquired {
		defer func() { _ = locker.ReleaseProjectionLock(ctx, d.lockKey()) }()
	}
	return d.runBatch(ctx)
}

// RunResult summarizes one batch.
type RunResult struct {
	Delivered int
	Failed    int
}

func (d *Drain) runBatch(ctx context.Context) (RunResult, error) {
	streamStore, ok := d.Store.(es.StateStreamStore)
	if !ok {
		return RunResult{}, errors.New("state_stream: Store does not implement es.StateStreamStore")
	}

	envs, err := streamStore.ListStreamsBehind(ctx, d.SubscriberName, d.Tenant, d.batchSize())
	if err != nil {
		return RunResult{}, fmt.Errorf("state_stream: list streams behind: %w", err)
	}
	if len(envs) == 0 {
		return RunResult{}, nil
	}

	var result RunResult
	for _, env := range envs {
		if !d.inShard(env) {
			continue
		}
		if err := d.Publisher.PublishState(ctx, env); err != nil {
			result.Failed++
			if d.OnDeliveryError != nil {
				d.OnDeliveryError(env, err)
			}
			continue
		}
		if err := streamStore.AdvanceStateStreamPosition(ctx,
			d.SubscriberName, env.TenantID, env.StreamID, env.Version); err != nil {
			// Position advance failed AFTER successful delivery.
			// The next drain cycle will re-deliver this stream's
			// state — receiver idempotency on Version absorbs it.
			result.Failed++
			if d.OnDeliveryError != nil {
				d.OnDeliveryError(env, fmt.Errorf("advance position: %w", err))
			}
			continue
		}
		result.Delivered++
	}
	return result, nil
}

func (d *Drain) validate() error {
	if d.SubscriberName == "" {
		return errors.New("state_stream: SubscriberName is required")
	}
	if d.Store == nil {
		return errors.New("state_stream: Store is required")
	}
	if d.Publisher == nil {
		return errors.New("state_stream: Publisher is required")
	}
	if d.TotalShards < 0 || d.Shard < 0 || (d.TotalShards > 0 && d.Shard >= d.TotalShards) {
		return fmt.Errorf("state_stream: invalid sharding Shard=%d TotalShards=%d",
			d.Shard, d.TotalShards)
	}
	return nil
}

func (d *Drain) batchSize() int {
	if d.BatchSize <= 0 {
		return defaultBatchSize
	}
	return d.BatchSize
}

func (d *Drain) lockKey() string {
	return "state_stream.drain:" + d.SubscriberName + ":" + d.Tenant
}

func (d *Drain) acquireLock(ctx context.Context) (es.ProjectionLocker, bool, error) {
	if d.LockKey == "" {
		return nil, false, nil
	}
	locker, ok := d.Store.(es.ProjectionLocker)
	if !ok {
		return nil, false, nil
	}
	acquired, err := locker.TryAcquireProjectionLock(ctx, d.lockKey())
	if err != nil {
		return locker, false, fmt.Errorf("state_stream: acquire lock %q: %w", d.lockKey(), err)
	}
	return locker, acquired, nil
}

// inShard reports whether env's stream is in this runner's slice.
// Stream-sticky: same scheme as outbox.Drain (FNV-1a over
// tenant|stream_id, modulo TotalShards). All deliveries for one
// stream always land on the same shard.
func (d *Drain) inShard(env es.StateEnvelope) bool {
	if d.TotalShards <= 0 {
		return true
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(env.TenantID))
	_, _ = h.Write([]byte{'|'})
	_, _ = h.Write([]byte(env.StreamID))
	return int(h.Sum32()%uint32(d.TotalShards)) == d.Shard
}

// Status is a small helper for ops code: query subscriber lag via
// the Store's StateStreamAdmin (if implemented). Returns the same
// shape as es.StateStreamSubscriberStatus.
func (d *Drain) Status(ctx context.Context) (es.StateStreamSubscriberStatus, error) {
	admin, ok := d.Store.(es.StateStreamAdmin)
	if !ok {
		return es.StateStreamSubscriberStatus{}, errors.New("state_stream: Store does not implement es.StateStreamAdmin")
	}
	return admin.StateStreamStatus(ctx, d.SubscriberName, d.Tenant)
}

// time is only used here to silence unused-import linting when
// tests/builds elide the time-dependent paths above.
var _ = time.Time{}
