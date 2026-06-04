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
	var row db.GetStateRow
	err := a.withTenantTx(ctx, tenantID, func(q *db.Queries) error {
		var inner error
		row, inner = q.GetState(ctx, db.GetStateParams{
			TenantID: tenantID,
			StreamID: streamID,
		})
		return inner
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
	var rows []db.ListStatesRow
	err := a.withTenantTx(ctx, tenantID, func(q *db.Queries) error {
		var inner error
		rows, inner = q.ListStates(ctx, db.ListStatesParams{
			TenantID:      tenantID,
			TypeUrl:       typeURL,
			AfterStreamID: afterStreamID,
			MaxRows:       int32(limit),
		})
		return inner
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

// WipeStateCacheForType implements es.StateCacheWriter. Empty tenantID
// invalidates the cache across every tenant — used for schema-version
// rollouts (ADR 0023) — and runs on the admin pool (ADR 0032).
func (a *Adapter) WipeStateCacheForType(ctx context.Context, tenantID, typeURL string) (int64, error) {
	if tenantID == "" {
		q, err := a.admin()
		if err != nil {
			return 0, err
		}
		return q.DeleteStateCacheForTypeAllTenants(ctx, typeURL)
	}
	var n int64
	err := a.withTenantTx(ctx, tenantID, func(q *db.Queries) error {
		var inner error
		n, inner = q.DeleteStateCacheForType(ctx, db.DeleteStateCacheForTypeParams{
			TenantID: tenantID,
			TypeUrl:  typeURL,
		})
		return inner
	})
	return n, err
}

// UpsertCachedState implements es.StateCacheUpserter. Used by
// aggregate.RebuildStateCache; the normal write path uses Append.
func (a *Adapter) UpsertCachedState(ctx context.Context, row es.StateCacheRow) error {
	schema := row.StateSchemaVersion
	if schema == 0 {
		schema = 1
	}
	return a.withTenantTx(ctx, row.TenantID, func(q *db.Queries) error {
		return q.UpsertStateCache(ctx, db.UpsertStateCacheParams{
			TenantID:           row.TenantID,
			StreamID:           row.StreamID,
			TypeUrl:            row.TypeURL,
			State:              row.State,
			Version:            int64(row.Version),
			Terminal:           row.Terminal,
			StateSchemaVersion: int32(schema),
		})
	})
}

// Compile-time checks.
var (
	_ es.StateCacheReader   = (*Adapter)(nil)
	_ es.StateCacheWriter   = (*Adapter)(nil)
	_ es.StateCacheUpserter = (*Adapter)(nil)
)
