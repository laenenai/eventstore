package postgres

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"

	"github.com/laenenai/eventstore/adapters/storage/postgres/internal/db"
	"github.com/laenenai/eventstore/projection"
)

// Load implements projection.Checkpoint. Returns 0 when the projector
// has never run for this (name, tenant_id) pair.
func (a *Adapter) Load(ctx context.Context, name, tenantID string) (uint64, error) {
	cursor, err := a.queries.LoadProjectionCheckpoint(ctx, db.LoadProjectionCheckpointParams{
		Name:     name,
		TenantID: tenantID,
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
	return a.queries.SaveProjectionCheckpoint(ctx, db.SaveProjectionCheckpointParams{
		Name:     name,
		TenantID: tenantID,
		Cursor:   int64(position),
	})
}

// Compile-time check that Adapter satisfies projection.Checkpoint.
var _ projection.Checkpoint = (*Adapter)(nil)
