package postgres

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"

	"github.com/laenenai/eventstore/adapters/storage/postgres/internal/db"
	"github.com/laenenai/eventstore/es"
)

// GetState implements es.StateCacheReader.
func (a *Adapter) GetState(ctx context.Context, tenantID, streamID string) (es.StateCacheRow, error) {
	row, err := a.queries.GetState(ctx, db.GetStateParams{
		TenantID: tenantID,
		StreamID: streamID,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return es.StateCacheRow{}, es.ErrStateNotFound
		}
		return es.StateCacheRow{}, err
	}
	return es.StateCacheRow{
		TenantID:           tenantID,
		StreamID:           streamID,
		TypeURL:            row.TypeUrl,
		State:              row.State,
		Version:            uint64(row.Version),
		Terminal:           row.Terminal,
		StateSchemaVersion: uint32(row.StateSchemaVersion),
		UpdatedAt:          row.UpdatedAt,
	}, nil
}

// ListStates implements es.StateCacheReader.
func (a *Adapter) ListStates(ctx context.Context, tenantID, typeURL, afterStreamID string, limit int) ([]es.StateCacheRow, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := a.queries.ListStates(ctx, db.ListStatesParams{
		TenantID:      tenantID,
		TypeUrl:       typeURL,
		AfterStreamID: afterStreamID,
		MaxRows:       int32(limit),
	})
	if err != nil {
		return nil, err
	}
	out := make([]es.StateCacheRow, 0, len(rows))
	for _, r := range rows {
		out = append(out, es.StateCacheRow{
			TenantID:           r.TenantID,
			StreamID:           r.StreamID,
			TypeURL:            r.TypeUrl,
			State:              r.State,
			Version:            uint64(r.Version),
			Terminal:           r.Terminal,
			StateSchemaVersion: uint32(r.StateSchemaVersion),
			UpdatedAt:          r.UpdatedAt,
		})
	}
	return out, nil
}

// WipeStateCacheForType implements es.StateCacheWriter.
func (a *Adapter) WipeStateCacheForType(ctx context.Context, tenantID, typeURL string) (int64, error) {
	if tenantID == "" {
		return a.queries.DeleteStateCacheForTypeAllTenants(ctx, typeURL)
	}
	return a.queries.DeleteStateCacheForType(ctx, db.DeleteStateCacheForTypeParams{
		TenantID: tenantID,
		TypeUrl:  typeURL,
	})
}

// UpsertCachedState implements es.StateCacheUpserter. Used by
// aggregate.RebuildStateCache; the normal write path uses Append.
func (a *Adapter) UpsertCachedState(ctx context.Context, row es.StateCacheRow) error {
	schema := row.StateSchemaVersion
	if schema == 0 {
		schema = 1
	}
	return a.queries.UpsertStateCache(ctx, db.UpsertStateCacheParams{
		TenantID:           row.TenantID,
		StreamID:           row.StreamID,
		TypeUrl:            row.TypeURL,
		State:              row.State,
		Version:            int64(row.Version),
		Terminal:           row.Terminal,
		StateSchemaVersion: int32(schema),
	})
}

// Compile-time checks.
var (
	_ es.StateCacheReader   = (*Adapter)(nil)
	_ es.StateCacheWriter   = (*Adapter)(nil)
	_ es.StateCacheUpserter = (*Adapter)(nil)
)
