package postgres

import (
	"context"

	"github.com/google/uuid"

	"github.com/laenenai/eventstore/adapters/storage/postgres/internal/db"
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
	return a.runForTenant(ctx, tenantID, func(q *db.Queries) error {
		return q.InsertProjectionDLQ(ctx, db.InsertProjectionDLQParams{
			ProjectionName: name,
			TenantID:       tenantID,
			GlobalPosition: int64(globalPosition),
			EventID:        eventID,
			TypeUrl:        typeURL,
			LastError:      truncated,
		})
	})
}

// ListProjectionDLQ implements es.ProjectionDLQAdmin.
func (a *Adapter) ListProjectionDLQ(ctx context.Context, name, tenantID string,
	afterPosition uint64, limit int,
) ([]es.ProjectionDLQRow, error) {
	if limit <= 0 {
		limit = 100
	}
	var rows []db.ProjectionDlq
	err := a.runForTenant(ctx, tenantID, func(q *db.Queries) error {
		var inner error
		rows, inner = q.ListProjectionDLQ(ctx, db.ListProjectionDLQParams{
			ProjectionName: name,
			TenantID:       tenantID,
			AfterPosition:  int64(afterPosition),
			MaxRows:        int32(limit),
		})
		return inner
	})
	if err != nil {
		return nil, err
	}
	out := make([]es.ProjectionDLQRow, 0, len(rows))
	for _, r := range rows {
		out = append(out, es.ProjectionDLQRow{
			ProjectionName: r.ProjectionName,
			TenantID:       r.TenantID,
			GlobalPosition: uint64(r.GlobalPosition),
			EventID:        r.EventID,
			TypeURL:        r.TypeUrl,
			LastError:      r.LastError,
			EnqueuedAt:     r.EnqueuedAt,
		})
	}
	return out, nil
}

// CountProjectionDLQ implements es.ProjectionDLQAdmin.
func (a *Adapter) CountProjectionDLQ(ctx context.Context, name, tenantID string) (int64, error) {
	var n int64
	err := a.runForTenant(ctx, tenantID, func(q *db.Queries) error {
		var inner error
		n, inner = q.CountProjectionDLQ(ctx, db.CountProjectionDLQParams{
			ProjectionName: name,
			TenantID:       tenantID,
		})
		return inner
	})
	return n, err
}

// AbandonProjectionDLQ implements es.ProjectionDLQAdmin.
func (a *Adapter) AbandonProjectionDLQ(ctx context.Context, name, tenantID string, globalPosition uint64) error {
	return a.runForTenant(ctx, tenantID, func(q *db.Queries) error {
		return q.DeleteProjectionDLQ(ctx, db.DeleteProjectionDLQParams{
			ProjectionName: name,
			TenantID:       tenantID,
			GlobalPosition: int64(globalPosition),
		})
	})
}

// AbandonAllProjectionDLQ implements es.ProjectionDLQAdmin.
func (a *Adapter) AbandonAllProjectionDLQ(ctx context.Context, name, tenantID string) (int64, error) {
	var n int64
	err := a.runForTenant(ctx, tenantID, func(q *db.Queries) error {
		var inner error
		n, inner = q.AbandonAllProjectionDLQ(ctx, db.AbandonAllProjectionDLQParams{
			ProjectionName: name,
			TenantID:       tenantID,
		})
		return inner
	})
	return n, err
}

var (
	_ es.ProjectionDLQWriter = (*Adapter)(nil)
	_ es.ProjectionDLQAdmin  = (*Adapter)(nil)
)
