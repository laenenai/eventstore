package postgres

import (
	"context"

	"github.com/laenenai/eventstore/adapters/storage/postgres/internal/db"
	"github.com/laenenai/eventstore/es"
)

// ListStreamsBehind implements es.StateStreamStore. Empty tenantID is
// the cross-tenant subscriber case (default tenant_id column = '');
// routes through the admin pool per ADR 0032.
func (a *Adapter) ListStreamsBehind(ctx context.Context, subscriberName, tenantID string, limit int) ([]es.StateEnvelope, error) {
	if limit <= 0 {
		limit = 100
	}
	var rows []db.ListStreamsBehindRow
	err := a.runForTenant(ctx, tenantID, func(q *db.Queries) error {
		var inner error
		rows, inner = q.ListStreamsBehind(ctx, db.ListStreamsBehindParams{
			Name:     subscriberName,
			TenantID: tenantID,
			MaxRows:  int32(limit),
		})
		return inner
	})
	if err != nil {
		return nil, err
	}
	out := make([]es.StateEnvelope, 0, len(rows))
	for _, r := range rows {
		out = append(out, es.StateEnvelope{
			TenantID:           r.TenantID,
			StreamID:           r.StreamID,
			TypeURL:            r.TypeUrl,
			Version:            uint64(r.Version),
			StateSchemaVersion: uint32(r.StateSchemaVersion),
			State:              r.State,
			UpdatedAt:          r.UpdatedAt,
		})
	}
	return out, nil
}

// AdvanceStateStreamPosition implements es.StateStreamStore.
func (a *Adapter) AdvanceStateStreamPosition(ctx context.Context, subscriberName, tenantID, streamID string, version uint64) error {
	return a.runForTenant(ctx, tenantID, func(q *db.Queries) error {
		return q.UpsertStateStreamPosition(ctx, db.UpsertStateStreamPositionParams{
			Name:     subscriberName,
			TenantID: tenantID,
			StreamID: streamID,
			Version:  int64(version),
		})
	})
}

// ---- StateStreamAdmin ---------------------------------------------------

// StateStreamStatus implements es.StateStreamAdmin.
func (a *Adapter) StateStreamStatus(ctx context.Context, subscriberName, tenantID string) (es.StateStreamSubscriberStatus, error) {
	var row db.CountStateStreamLagRow
	err := a.runForTenant(ctx, tenantID, func(q *db.Queries) error {
		var inner error
		row, inner = q.CountStateStreamLag(ctx, db.CountStateStreamLagParams{
			Name:     subscriberName,
			TenantID: tenantID,
		})
		return inner
	})
	if err != nil {
		return es.StateStreamSubscriberStatus{}, err
	}
	return es.StateStreamSubscriberStatus{
		Name:           subscriberName,
		TenantID:       tenantID,
		StreamsBehind:  row.StreamsBehind,
		MaxLagVersions: uint64(row.MaxLagVersions),
	}, nil
}

// ResetStateStreamSubscriber implements es.StateStreamAdmin.
func (a *Adapter) ResetStateStreamSubscriber(ctx context.Context, subscriberName, tenantID string) (int64, error) {
	var n int64
	err := a.runForTenant(ctx, tenantID, func(q *db.Queries) error {
		var inner error
		n, inner = q.ResetStateStreamSubscriber(ctx, db.ResetStateStreamSubscriberParams{
			Name:     subscriberName,
			TenantID: tenantID,
		})
		return inner
	})
	return n, err
}

// ResetStateStreamSubscriberForStream implements es.StateStreamAdmin.
func (a *Adapter) ResetStateStreamSubscriberForStream(ctx context.Context, subscriberName, tenantID, streamID string) error {
	return a.runForTenant(ctx, tenantID, func(q *db.Queries) error {
		return q.ResetStateStreamSubscriberForStream(ctx, db.ResetStateStreamSubscriberForStreamParams{
			Name:     subscriberName,
			TenantID: tenantID,
			StreamID: streamID,
		})
	})
}

// ListStateStreamSubscribers implements es.StateStreamAdmin. Lists every
// subscriber name across every tenant — runs on the admin pool per
// ADR 0032.
func (a *Adapter) ListStateStreamSubscribers(ctx context.Context) ([]string, error) {
	q, err := a.admin()
	if err != nil {
		return nil, err
	}
	return q.ListStateStreamSubscribers(ctx)
}

var (
	_ es.StateStreamStore = (*Adapter)(nil)
	_ es.StateStreamAdmin = (*Adapter)(nil)
)
