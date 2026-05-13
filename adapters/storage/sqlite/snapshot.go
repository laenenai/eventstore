package sqlite

import (
	"context"
	"database/sql"
	"errors"

	"github.com/laenenai/eventstore/adapters/storage/sqlite/internal/db"
	"github.com/laenenai/eventstore/es"
)

// LoadSnapshot implements es.SnapshotStore.
func (a *Adapter) LoadSnapshot(ctx context.Context, tenantID, streamID string) (es.Snapshot, error) {
	row, err := a.queries.GetSnapshot(ctx, db.GetSnapshotParams{
		TenantID: tenantID,
		StreamID: streamID,
	})
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return es.Snapshot{}, es.ErrSnapshotNotFound
		}
		return es.Snapshot{}, err
	}
	created, err := parseTime(row.CreatedAt)
	if err != nil {
		return es.Snapshot{}, err
	}
	return es.Snapshot{
		TenantID:           row.TenantID,
		StreamID:           row.StreamID,
		Version:            uint64(row.Version),
		StateSchemaVersion: uint32(row.StateSchemaVersion),
		State:              row.State,
		CreatedAt:          created,
	}, nil
}

// SaveSnapshot implements es.SnapshotStore.
func (a *Adapter) SaveSnapshot(ctx context.Context, snap es.Snapshot) error {
	return a.queries.UpsertSnapshot(ctx, db.UpsertSnapshotParams{
		TenantID:           snap.TenantID,
		StreamID:           snap.StreamID,
		Version:            int64(snap.Version),
		StateSchemaVersion: int64(snap.StateSchemaVersion),
		State:              snap.State,
	})
}

// DeleteSnapshot implements es.SnapshotStore.
func (a *Adapter) DeleteSnapshot(ctx context.Context, tenantID, streamID string) error {
	return a.queries.DeleteSnapshot(ctx, db.DeleteSnapshotParams{
		TenantID: tenantID,
		StreamID: streamID,
	})
}

var _ es.SnapshotStore = (*Adapter)(nil)
