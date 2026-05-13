// Package projection hosts the projector runtime: subscribe to events
// (via polling or a publisher), invoke a handler per event, advance
// a checkpoint. See ADR 0012 for the delivery model.
//
// This iteration provides catch-up (polling) projectors only. The
// publisher-driven live path arrives with the EventPublisher commit.
package projection

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"time"

	"github.com/laenenai/eventstore/es"
)

// Runtime drives one projector against an es.Store.
//
// Usage:
//
//	rt := &projection.Runtime{
//	    Name:       "user-by-email",
//	    Store:      store,
//	    Checkpoint: projection.NewMemoryCheckpoint(),
//	    Handler:    userByEmail.Handle,
//	}
//	if err := rt.Run(ctx); err != nil { ... }
//
// Run loops until ctx is cancelled. RunOnce processes a single batch
// and returns — useful for tests and for scheduled-cron projection
// execution where the host (rather than the runtime) drives the loop.
type Runtime struct {
	// Name identifies this projector for checkpoint persistence.
	// Must be stable across deploys.
	Name string

	// Store is the event source.
	Store es.Store

	// Checkpoint persists the projector's cursor.
	Checkpoint Checkpoint

	// Handler is invoked once per event.
	Handler Handler

	// Tenant scopes the projector to a single tenant. Empty string
	// means cross-tenant (admin-scope projections like billing or
	// compliance). Most projectors should be tenant-scoped.
	Tenant string

	// BatchSize is the number of events read per poll. Default 100.
	BatchSize int

	// IdleSleep is the duration to sleep between polls when no new
	// events are available. Default 1 second. Set higher for
	// scale-to-zero deployments where you'd rather use the scheduled
	// drain path; set lower for low-latency interactive projections.
	IdleSleep time.Duration

	// LockKey enables single-runner safety across replicas (ADR 0020
	// decision 3f). When set and the Store implements
	// es.ProjectionLocker, RunOnce attempts a non-blocking acquisition
	// at start and exits cleanly with (0, nil) if another instance
	// holds the lock. SQLite does not implement the interface — file-
	// level write locking already serializes; the same code works on
	// both adapters.
	//
	// Recommended key: a stable string per projection purpose, e.g.,
	// "user-by-email" or "user-by-email:" + tenantID for per-tenant
	// shards.
	LockKey string

	// Shard / TotalShards enable horizontal partitioning of one
	// projection across N runners. Sharding is by FNV-1a hash of
	// (tenant_id | stream_id) % TotalShards — stream-sticky, so all
	// events of a given stream always go to the same shard. Mirrors
	// the outbox drain's design.
	//
	// Use when single-runner throughput is the bottleneck. Each shard
	// runs with its own (Name, LockKey) — typically the projection
	// name with a shard suffix. Within a shard, fail-stop semantics,
	// per-stream ordering, and checkpoint advance work identically to
	// a non-sharded runner.
	//
	// Default (0, 0) = no sharding. Shard must be in [0, TotalShards).
	Shard       int
	TotalShards int

	// DLQOnFailure switches the failure semantic from fail-stop
	// (ADR 0020 decision 3d) to DLQ-skip: when the handler returns an
	// error, the event is captured into projection_dlq, the cursor
	// advances past it, and processing continues with the next event.
	// The default behavior (fail-stop with last-success advance) is
	// safer for code-bug failures; DLQ-skip is the right opt-in for
	// projections where one bad event mustn't halt the whole stream
	// (e.g., analytics rollups, search indexers that tolerate gaps).
	//
	// Requires the Store to implement es.ProjectionDLQWriter (both
	// shipped adapters do).
	DLQOnFailure bool
}

const (
	defaultBatchSize = 100
	defaultIdleSleep = time.Second
)

// Run drives the projector until ctx is cancelled. Returns nil on
// clean shutdown via context cancellation; returns an error on
// persistent failure (store error, handler error, checkpoint error).
func (r *Runtime) Run(ctx context.Context) error {
	if err := r.validate(); err != nil {
		return err
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}
		processed, err := r.RunOnce(ctx)
		if err != nil {
			return err
		}
		if processed == 0 {
			// No new events; sleep and try again.
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(r.idleSleep()):
			}
		}
	}
}

// RunOnce processes a single batch of events and returns the count
// processed. Useful for unit tests and for scheduled execution where
// the caller controls the loop. Returns nil error on empty batch.
func (r *Runtime) RunOnce(ctx context.Context) (int, error) {
	if err := r.validate(); err != nil {
		return 0, err
	}

	locker, acquired, err := r.acquireLock(ctx)
	if err != nil {
		return 0, err
	}
	if r.LockKey != "" && locker != nil && !acquired {
		return 0, nil
	}
	if acquired {
		defer func() { _ = locker.ReleaseProjectionLock(ctx, r.LockKey) }()
	}

	cursor, err := r.Checkpoint.Load(ctx, r.Name, r.Tenant)
	if err != nil {
		return 0, fmt.Errorf("projection %s: load checkpoint: %w", r.Name, err)
	}

	var events []es.Envelope
	if r.Tenant != "" {
		events, err = r.Store.ReadAllForTenant(ctx, r.Tenant, cursor, r.batchSize())
	} else {
		events, err = r.Store.ReadAll(ctx, cursor, r.batchSize())
	}
	if err != nil {
		return 0, fmt.Errorf("projection %s: read events: %w", r.Name, err)
	}
	if len(events) == 0 {
		return 0, nil
	}

	// Per ADR 0020 decision 3d: fail-stop with last-success
	// checkpoint advance. On handler error mid-batch, persist the
	// cursor up to the last successfully-handled event, then return
	// the error. The next RunOnce resumes at the failing event.
	//
	// When sharding is enabled, events that don't hash to this
	// shard are skipped — but the cursor still advances past them so
	// the runner doesn't re-read them every batch. Skipped events
	// count toward "last" but not "successes".
	var (
		last       uint64
		successes  int
		handlerErr error
	)
	for _, env := range events {
		if !r.inShard(env) {
			last = env.GlobalPosition
			continue
		}
		if err := r.Handler(ctx, env); err != nil {
			if r.DLQOnFailure {
				dlq, ok := r.Store.(es.ProjectionDLQWriter)
				if !ok {
					handlerErr = fmt.Errorf("projection %s: DLQOnFailure set but Store does not implement ProjectionDLQWriter", r.Name)
					break
				}
				if derr := dlq.InsertProjectionDLQ(ctx, r.Name, env.TenantID,
					env.GlobalPosition, env.EventID, env.TypeURL, err.Error()); derr != nil {
					handlerErr = fmt.Errorf("projection %s: dlq capture: %w", r.Name, derr)
					break
				}
				// Cursor advances past the failed event; continue.
				last = env.GlobalPosition
				continue
			}
			handlerErr = fmt.Errorf("projection %s: handle event %s: %w",
				r.Name, env.EventID, err)
			break
		}
		last = env.GlobalPosition
		successes++
	}

	if last > 0 {
		if err := r.Checkpoint.Save(ctx, r.Name, r.Tenant, last); err != nil {
			return successes, fmt.Errorf("projection %s: save checkpoint: %w", r.Name, err)
		}
	}
	return successes, handlerErr
}

// acquireLock probes whether the Store implements es.ProjectionLocker
// and, when LockKey is set, attempts a non-blocking acquisition. The
// returned locker is non-nil only when LockKey is set and the store
// implements the interface; callers must release exactly when
// acquired is true.
func (r *Runtime) acquireLock(ctx context.Context) (es.ProjectionLocker, bool, error) {
	if r.LockKey == "" {
		return nil, false, nil
	}
	locker, ok := r.Store.(es.ProjectionLocker)
	if !ok {
		return nil, false, nil
	}
	acquired, err := locker.TryAcquireProjectionLock(ctx, r.LockKey)
	if err != nil {
		return locker, false, fmt.Errorf("projection %s: acquire lock %q: %w", r.Name, r.LockKey, err)
	}
	return locker, acquired, nil
}

func (r *Runtime) validate() error {
	if r.Name == "" {
		return errors.New("projection: Name is required")
	}
	if r.Store == nil {
		return errors.New("projection: Store is required")
	}
	if r.Checkpoint == nil {
		return errors.New("projection: Checkpoint is required")
	}
	if r.Handler == nil {
		return errors.New("projection: Handler is required")
	}
	if r.TotalShards < 0 || r.Shard < 0 || (r.TotalShards > 0 && r.Shard >= r.TotalShards) {
		return fmt.Errorf("projection: invalid sharding Shard=%d TotalShards=%d",
			r.Shard, r.TotalShards)
	}
	return nil
}

// inShard reports whether env's stream falls within this runner's
// shard slice. Stream-sticky hashing — all events of one stream go to
// the same shard — same design as outbox.Drain.
func (r *Runtime) inShard(env es.Envelope) bool {
	if r.TotalShards <= 0 {
		return true
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(env.TenantID))
	_, _ = h.Write([]byte{'|'})
	_, _ = h.Write([]byte(env.StreamID.Canonical()))
	return int(h.Sum32()%uint32(r.TotalShards)) == r.Shard
}

func (r *Runtime) batchSize() int {
	if r.BatchSize <= 0 {
		return defaultBatchSize
	}
	return r.BatchSize
}

func (r *Runtime) idleSleep() time.Duration {
	if r.IdleSleep <= 0 {
		return defaultIdleSleep
	}
	return r.IdleSleep
}
