package sqlite

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/laenenai/eventstore/adapters/storage/sqlite/internal/db"
	"github.com/laenenai/eventstore/es"
)

// InsertProjectionDLQ implements es.ProjectionDLQWriter.
func (a *Adapter) InsertProjectionDLQ(ctx context.Context, name, tenantID string,
	globalPosition uint64, eventID uuid.UUID, typeURL, lastError string,
) error {
	truncated := lastError
	if len(truncated) > 2048 {
		truncated = truncated[:2048]
	}
	return a.queries.InsertProjectionDLQ(ctx, db.InsertProjectionDLQParams{
		ProjectionName: name,
		TenantID:       tenantID,
		GlobalPosition: int64(globalPosition),
		EventID:        eventID,
		TypeUrl:        typeURL,
		LastError:      truncated,
		EnqueuedAt:     formatTime(time.Now().UTC()),
	})
}

// ListProjectionDLQ implements es.ProjectionDLQAdmin.
func (a *Adapter) ListProjectionDLQ(ctx context.Context, name, tenantID string,
	afterPosition uint64, limit int,
) ([]es.ProjectionDLQRow, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := a.queries.ListProjectionDLQ(ctx, db.ListProjectionDLQParams{
		ProjectionName: name,
		TenantID:       tenantID,
		AfterPosition:  int64(afterPosition),
		MaxRows:        int64(limit),
	})
	if err != nil {
		return nil, err
	}
	out := make([]es.ProjectionDLQRow, 0, len(rows))
	for _, r := range rows {
		enqueued, err := parseTime(r.EnqueuedAt)
		if err != nil {
			return nil, err
		}
		out = append(out, es.ProjectionDLQRow{
			ProjectionName: r.ProjectionName,
			TenantID:       r.TenantID,
			GlobalPosition: uint64(r.GlobalPosition),
			EventID:        r.EventID,
			TypeURL:        r.TypeUrl,
			LastError:      r.LastError,
			EnqueuedAt:     enqueued,
		})
	}
	return out, nil
}

// CountProjectionDLQ implements es.ProjectionDLQAdmin.
func (a *Adapter) CountProjectionDLQ(ctx context.Context, name, tenantID string) (int64, error) {
	return a.queries.CountProjectionDLQ(ctx, db.CountProjectionDLQParams{
		ProjectionName: name,
		TenantID:       tenantID,
	})
}

// AbandonProjectionDLQ implements es.ProjectionDLQAdmin.
func (a *Adapter) AbandonProjectionDLQ(ctx context.Context, name, tenantID string, globalPosition uint64) error {
	return a.queries.DeleteProjectionDLQ(ctx, db.DeleteProjectionDLQParams{
		ProjectionName: name,
		TenantID:       tenantID,
		GlobalPosition: int64(globalPosition),
	})
}

// AbandonAllProjectionDLQ implements es.ProjectionDLQAdmin.
func (a *Adapter) AbandonAllProjectionDLQ(ctx context.Context, name, tenantID string) (int64, error) {
	return a.queries.AbandonAllProjectionDLQ(ctx, db.AbandonAllProjectionDLQParams{
		ProjectionName: name,
		TenantID:       tenantID,
	})
}

var (
	_ es.ProjectionDLQWriter = (*Adapter)(nil)
	_ es.ProjectionDLQAdmin  = (*Adapter)(nil)
)
