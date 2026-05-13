package postgres

import (
	"context"

	"github.com/laenenai/eventstore/adapters/storage/postgres/internal/db"
	"github.com/laenenai/eventstore/es"
)

// CountPending implements es.OutboxAdmin.
func (a *Adapter) CountPending(ctx context.Context, tenantID string) (int64, error) {
	return a.queries.CountPending(ctx, tenantID)
}

// CountFailing implements es.OutboxAdmin.
func (a *Adapter) CountFailing(ctx context.Context, tenantID string, maxAttempts int32) (int64, error) {
	return a.queries.CountFailing(ctx, db.CountFailingParams{
		TenantID:    tenantID,
		MaxAttempts: safeMaxAttempts(maxAttempts),
	})
}

// CountDLQ implements es.OutboxAdmin.
func (a *Adapter) CountDLQ(ctx context.Context, tenantID string, maxAttempts int32) (int64, error) {
	return a.queries.CountDLQ(ctx, db.CountDLQParams{
		TenantID:    tenantID,
		MaxAttempts: safeMaxAttempts(maxAttempts),
	})
}

// ListDLQ implements es.OutboxAdmin.
func (a *Adapter) ListDLQ(ctx context.Context, tenantID string, maxAttempts int32, afterPosition uint64, limit int) ([]es.DLQRow, error) {
	rows, err := a.queries.ListDLQ(ctx, db.ListDLQParams{
		TenantID:      tenantID,
		MaxAttempts:   safeMaxAttempts(maxAttempts),
		AfterPosition: int64(afterPosition),
		MaxRows:       int32(limit),
	})
	if err != nil {
		return nil, err
	}
	out := make([]es.DLQRow, 0, len(rows))
	for _, r := range rows {
		dr := es.DLQRow{
			TenantID:       r.TenantID,
			GlobalPosition: uint64(r.GlobalPosition),
			EventID:        r.EventID,
			StreamID:       r.StreamID,
			TypeURL:        r.TypeUrl,
			CorrelationID:  r.CorrelationID,
			CausationID:    r.CausationID,
			CommandID:      r.CommandID,
			ActorPrincipal: r.ActorPrincipal,
			EnqueuedAt:     r.EnqueuedAt,
			Attempts:       r.Attempts,
		}
		if r.LastError != nil {
			dr.LastError = *r.LastError
		}
		if r.NextAttemptAt != nil {
			dr.NextAttemptAt = r.NextAttemptAt
		}
		out = append(out, dr)
	}
	return out, nil
}

// ReplayDLQ implements es.OutboxAdmin.
func (a *Adapter) ReplayDLQ(ctx context.Context, tenantID string, globalPosition uint64) error {
	return a.queries.ReplayDLQ(ctx, db.ReplayDLQParams{
		TenantID:       tenantID,
		GlobalPosition: int64(globalPosition),
	})
}

// AbandonDLQ implements es.OutboxAdmin.
func (a *Adapter) AbandonDLQ(ctx context.Context, tenantID string, globalPosition uint64) error {
	return a.queries.AbandonDLQ(ctx, db.AbandonDLQParams{
		TenantID:       tenantID,
		GlobalPosition: int64(globalPosition),
	})
}

// ReplayAllDLQ implements es.OutboxAdmin.
func (a *Adapter) ReplayAllDLQ(ctx context.Context, tenantID string, maxAttempts int32) (int64, error) {
	return a.queries.ReplayAllDLQ(ctx, db.ReplayAllDLQParams{
		TenantID:    tenantID,
		MaxAttempts: safeMaxAttempts(maxAttempts),
	})
}
