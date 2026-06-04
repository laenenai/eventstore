package postgres

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"

	"github.com/laenenai/eventstore/adapters/storage/postgres/internal/db"
	"github.com/laenenai/eventstore/es"
)

// Status implements es.ProjectionAdmin.
func (a *Adapter) Status(ctx context.Context, name, tenantID string) (es.ProjectionStatus, error) {
	var row db.ProjectionCheckpoint
	err := a.runForTenant(ctx, tenantID, func(q *db.Queries) error {
		var inner error
		row, inner = q.GetProjectionStatus(ctx, db.GetProjectionStatusParams{
			Name:     name,
			TenantID: tenantID,
		})
		return inner
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return es.ProjectionStatus{}, es.ErrStateNotFound
		}
		return es.ProjectionStatus{}, err
	}
	return es.ProjectionStatus{
		Name:      row.Name,
		TenantID:  row.TenantID,
		Cursor:    uint64(row.Cursor),
		UpdatedAt: row.UpdatedAt,
	}, nil
}

// Reset implements es.ProjectionAdmin.
func (a *Adapter) Reset(ctx context.Context, name, tenantID string) error {
	return a.runForTenant(ctx, tenantID, func(q *db.Queries) error {
		return q.ResetProjectionCheckpoint(ctx, db.ResetProjectionCheckpointParams{
			Name:     name,
			TenantID: tenantID,
		})
	})
}

// ResetTo implements es.ProjectionAdmin.
func (a *Adapter) ResetTo(ctx context.Context, name, tenantID string, position uint64) error {
	return a.runForTenant(ctx, tenantID, func(q *db.Queries) error {
		return q.SetProjectionCheckpoint(ctx, db.SetProjectionCheckpointParams{
			Name:     name,
			TenantID: tenantID,
			Cursor:   int64(position),
		})
	})
}

// List implements es.ProjectionAdmin. Cross-tenant — lists every
// checkpoint across every tenant for monitoring tooling. Runs on the
// admin pool per ADR 0032.
func (a *Adapter) List(ctx context.Context) ([]es.ProjectionStatus, error) {
	q, err := a.admin()
	if err != nil {
		return nil, err
	}
	rows, err := q.ListProjectionCheckpoints(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]es.ProjectionStatus, 0, len(rows))
	for _, r := range rows {
		out = append(out, es.ProjectionStatus{
			Name:      r.Name,
			TenantID:  r.TenantID,
			Cursor:    uint64(r.Cursor),
			UpdatedAt: r.UpdatedAt,
		})
	}
	return out, nil
}

var _ es.ProjectionAdmin = (*Adapter)(nil)
