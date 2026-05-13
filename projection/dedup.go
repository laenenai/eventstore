package projection

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/laenenai/eventstore/es"
)

// DedupStore is the storage surface for WithDedup. Adapters implement
// it against the framework-managed processed_events table.
//
// Used by WithDedup; not part of the projection.Runtime contract.
type DedupStore interface {
	HasProcessedEvent(ctx context.Context, projectionName, tenantID string, eventID uuid.UUID) (bool, error)
	MarkProcessedEvent(ctx context.Context, projectionName, tenantID string, eventID uuid.UUID) error
	CleanupProcessedEvents(ctx context.Context, projectionName string, olderThan time.Time) (int64, error)
}

// WithDedup wraps an inner Handler with a per-event-id dedup gate.
// For each event the wrapper:
//
//  1. Looks up (projection_name, tenant_id, event_id) in the dedup
//     store. If present, returns nil (already processed).
//  2. Otherwise invokes the inner handler.
//  3. On inner success, marks the event as processed.
//
// projectionName is the dedup namespace. It typically matches the
// projection.Runtime.Name. Different projections use different names
// so their dedup ledgers stay disjoint.
//
// IMPORTANT consistency note (ADR 0020 decision 3h): the mark step
// runs AFTER the inner handler returns success but before any
// downstream commit by the framework. A crash between handler-success
// and mark-written re-processes the event on the next run. WithDedup
// reduces duplicate side effects in the common path; it is NOT
// exactly-once. For strict EOS, push idempotency into the external
// system being written to (idempotency keys on payments, dedup IDs on
// queues).
func WithDedup(inner Handler, store DedupStore, projectionName string) Handler {
	return func(ctx context.Context, env es.Envelope) error {
		seen, err := store.HasProcessedEvent(ctx, projectionName, env.TenantID, env.EventID)
		if err != nil {
			return fmt.Errorf("dedup: lookup: %w", err)
		}
		if seen {
			return nil
		}
		if err := inner(ctx, env); err != nil {
			return err
		}
		if err := store.MarkProcessedEvent(ctx, projectionName, env.TenantID, env.EventID); err != nil {
			return fmt.Errorf("dedup: mark: %w", err)
		}
		return nil
	}
}
