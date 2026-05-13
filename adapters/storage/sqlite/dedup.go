package sqlite

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/laenenai/eventstore/adapters/storage/sqlite/internal/db"
	"github.com/laenenai/eventstore/projection"
)

// HasProcessedEvent implements projection.DedupStore.
func (a *Adapter) HasProcessedEvent(ctx context.Context, name, tenantID string, eventID uuid.UUID) (bool, error) {
	v, err := a.queries.HasProcessedEvent(ctx, db.HasProcessedEventParams{
		ProjectionName: name,
		TenantID:       tenantID,
		EventID:        eventID,
	})
	if err != nil {
		return false, err
	}
	// SQLite returns 0/1 as int64 for EXISTS.
	return v != 0, nil
}

// MarkProcessedEvent implements projection.DedupStore.
func (a *Adapter) MarkProcessedEvent(ctx context.Context, name, tenantID string, eventID uuid.UUID) error {
	return a.queries.MarkProcessedEvent(ctx, db.MarkProcessedEventParams{
		ProjectionName: name,
		TenantID:       tenantID,
		EventID:        eventID,
		ProcessedAt:    formatTime(time.Now().UTC()),
	})
}

// CleanupProcessedEvents implements projection.DedupStore.
func (a *Adapter) CleanupProcessedEvents(ctx context.Context, name string, olderThan time.Time) (int64, error) {
	return a.queries.CleanupProcessedEvents(ctx, db.CleanupProcessedEventsParams{
		ProjectionName: name,
		ProcessedAt:    formatTime(olderThan),
	})
}

var _ projection.DedupStore = (*Adapter)(nil)
