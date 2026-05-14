package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/laenenai/eventstore/adapters/storage/sqlite/internal/db"
	"github.com/laenenai/eventstore/es"
)

// GetState implements es.StateCacheReader.
func (a *Adapter) GetState(ctx context.Context, tenantID, streamID string) (es.StateCacheRow, error) {
	row, err := a.queries.GetState(ctx, db.GetStateParams{
		TenantID: tenantID,
		StreamID: streamID,
	})
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return es.StateCacheRow{}, es.ErrStateNotFound
		}
		return es.StateCacheRow{}, err
	}
	updated, err := parseTime(row.UpdatedAt)
	if err != nil {
		return es.StateCacheRow{}, err
	}
	return es.StateCacheRow{
		TenantID:           tenantID,
		StreamID:           streamID,
		TypeURL:            row.TypeUrl,
		State:              stateBytes(row.State),
		Version:            uint64(row.Version),
		Terminal:           row.Terminal != 0,
		StateSchemaVersion: uint32(row.StateSchemaVersion),
		UpdatedAt:          updated,
	}, nil
}

// stateBytes coerces the sqlc-typed interface{} (json(state) returns
// TEXT but the column is BLOB JSONB — the driver may scan either
// string or []byte depending on the version) into a stable []byte.
func stateBytes(v interface{}) []byte {
	switch s := v.(type) {
	case []byte:
		return s
	case string:
		return []byte(s)
	case nil:
		return nil
	}
	return nil
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
		MaxRows:       int64(limit),
	})
	if err != nil {
		return nil, err
	}
	out := make([]es.StateCacheRow, 0, len(rows))
	for _, r := range rows {
		updated, err := parseTime(r.UpdatedAt)
		if err != nil {
			return nil, err
		}
		out = append(out, es.StateCacheRow{
			TenantID:           r.TenantID,
			StreamID:           r.StreamID,
			TypeURL:            r.TypeUrl,
			State:              stateBytes(r.State),
			Version:            uint64(r.Version),
			Terminal:           r.Terminal != 0,
			StateSchemaVersion: uint32(r.StateSchemaVersion),
			UpdatedAt:          updated,
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
// aggregate.RebuildStateCache.
func (a *Adapter) UpsertCachedState(ctx context.Context, row es.StateCacheRow) error {
	terminal := int64(0)
	if row.Terminal {
		terminal = 1
	}
	schema := row.StateSchemaVersion
	if schema == 0 {
		schema = 1
	}
	now := formatTime(time.Now().UTC())
	return a.queries.UpsertStateCache(ctx, db.UpsertStateCacheParams{
		TenantID:           row.TenantID,
		StreamID:           row.StreamID,
		TypeUrl:            row.TypeURL,
		State:              string(row.State),
		Version:            int64(row.Version),
		Terminal:           terminal,
		StateSchemaVersion: int64(schema),
		UpdatedAt:          now,
	})
}

// Compile-time checks.
var (
	_ es.StateCacheReader   = (*Adapter)(nil)
	_ es.StateCacheWriter   = (*Adapter)(nil)
	_ es.StateCacheUpserter = (*Adapter)(nil)
)
