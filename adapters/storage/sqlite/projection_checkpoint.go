package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/laenenai/eventstore/adapters/storage/sqlite/internal/db"
	"github.com/laenenai/eventstore/projection"
)

// Load implements projection.Checkpoint.
func (a *Adapter) Load(ctx context.Context, name, tenantID string) (uint64, error) {
	cursor, err := a.queries.LoadProjectionCheckpoint(ctx, db.LoadProjectionCheckpointParams{
		Name:     name,
		TenantID: tenantID,
	})
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, nil
		}
		return 0, err
	}
	return uint64(cursor), nil
}

// Save implements projection.Checkpoint.
func (a *Adapter) Save(ctx context.Context, name, tenantID string, position uint64) error {
	return a.queries.SaveProjectionCheckpoint(ctx, db.SaveProjectionCheckpointParams{
		Name:      name,
		TenantID:  tenantID,
		Cursor:    int64(position),
		UpdatedAt: formatTime(time.Now().UTC()),
	})
}

var _ projection.Checkpoint = (*Adapter)(nil)
