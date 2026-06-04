package postgres

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"

	"github.com/laenenai/eventstore/adapters/storage/postgres/internal/db"
	"github.com/laenenai/eventstore/projection"
)

// Load implements projection.Checkpoint. Returns 0 when the projector
// has never run for this (name, tenant_id) pair. Empty tenantID is the
// cross-tenant projector case — it routes through the admin pool per
// ADR 0032 since the row's tenant_id is '' and would not match any
// tenant-scoped RLS predicate.
func (a *Adapter) Load(ctx context.Context, name, tenantID string) (uint64, error) {
	var cursor int64
	err := a.runForTenant(ctx, tenantID, func(q *db.Queries) error {
		var inner error
		cursor, inner = q.LoadProjectionCheckpoint(ctx, db.LoadProjectionCheckpointParams{
			Name:     name,
			TenantID: tenantID,
		})
		return inner
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, nil
		}
		return 0, err
	}
	return uint64(cursor), nil
}

// Save implements projection.Checkpoint.
func (a *Adapter) Save(ctx context.Context, name, tenantID string, position uint64) error {
	return a.runForTenant(ctx, tenantID, func(q *db.Queries) error {
		return q.SaveProjectionCheckpoint(ctx, db.SaveProjectionCheckpointParams{
			Name:     name,
			TenantID: tenantID,
			Cursor:   int64(position),
		})
	})
}

// Compile-time check that Adapter satisfies projection.Checkpoint.
var _ projection.Checkpoint = (*Adapter)(nil)
