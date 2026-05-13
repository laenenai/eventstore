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
	return a.queries.HasProcessedEvent(ctx, db.HasProcessedEventParams{
		ProjectionName: name,
		TenantID:       tenantID,
		EventID:        eventID,
	})
}

// MarkProcessedEvent implements projection.DedupStore.
func (a *Adapter) MarkProcessedEvent(ctx context.Context, name, tenantID string, eventID uuid.UUID) error {
	return a.queries.MarkProcessedEvent(ctx, db.MarkProcessedEventParams{
		ProjectionName: name,
		TenantID:       tenantID,
		EventID:        eventID,
	})
}

// CleanupProcessedEvents implements projection.DedupStore.
func (a *Adapter) CleanupProcessedEvents(ctx context.Context, name string, olderThan time.Time) (int64, error) {
	return a.queries.CleanupProcessedEvents(ctx, db.CleanupProcessedEventsParams{
		ProjectionName: name,
		OlderThan:      olderThan.UTC(),
	})
}

var _ projection.DedupStore = (*Adapter)(nil)
