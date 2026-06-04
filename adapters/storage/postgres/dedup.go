package postgres

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/laenenai/eventstore/adapters/storage/postgres/internal/db"
	"github.com/laenenai/eventstore/projection"
)

// HasProcessedEvent implements projection.DedupStore.
func (a *Adapter) HasProcessedEvent(ctx context.Context, name, tenantID string, eventID uuid.UUID) (bool, error) {
	var seen bool
	err := a.runForTenant(ctx, tenantID, func(q *db.Queries) error {
		var inner error
		seen, inner = q.HasProcessedEvent(ctx, db.HasProcessedEventParams{
			ProjectionName: name,
			TenantID:       tenantID,
			EventID:        eventID,
		})
		return inner
	})
	return seen, err
}

// MarkProcessedEvent implements projection.DedupStore.
func (a *Adapter) MarkProcessedEvent(ctx context.Context, name, tenantID string, eventID uuid.UUID) error {
	return a.runForTenant(ctx, tenantID, func(q *db.Queries) error {
		return q.MarkProcessedEvent(ctx, db.MarkProcessedEventParams{
			ProjectionName: name,
			TenantID:       tenantID,
			EventID:        eventID,
		})
	})
}

// CleanupProcessedEvents implements projection.DedupStore. Cross-tenant
// by signature (no tenant arg) — sweeps a projection's processed rows
// across every tenant after the retention window, so it runs on the
// admin pool per ADR 0032.
func (a *Adapter) CleanupProcessedEvents(ctx context.Context, name string, olderThan time.Time) (int64, error) {
	q, err := a.admin()
	if err != nil {
		return 0, err
	}
	return q.CleanupProcessedEvents(ctx, db.CleanupProcessedEventsParams{
		ProjectionName: name,
		OlderThan:      olderThan.UTC(),
	})
}

var _ projection.DedupStore = (*Adapter)(nil)
