package postgres

import (
	"context"

	"github.com/laenenai/eventstore/adapters/storage/postgres/internal/db"
	"github.com/laenenai/eventstore/es"
)

// ListStreamsBehind implements es.StateStreamStore.
func (a *Adapter) ListStreamsBehind(ctx context.Context, subscriberName, tenantID string, limit int) ([]es.StateEnvelope, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := a.queries.ListStreamsBehind(ctx, db.ListStreamsBehindParams{
		Name:     subscriberName,
		TenantID: tenantID,
		MaxRows:  int32(limit),
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
	return a.queries.UpsertStateStreamPosition(ctx, db.UpsertStateStreamPositionParams{
		Name:     subscriberName,
		TenantID: tenantID,
		StreamID: streamID,
		Version:  int64(version),
	})
}

// ---- StateStreamAdmin ---------------------------------------------------

// StateStreamStatus implements es.StateStreamAdmin.
func (a *Adapter) StateStreamStatus(ctx context.Context, subscriberName, tenantID string) (es.StateStreamSubscriberStatus, error) {
	row, err := a.queries.CountStateStreamLag(ctx, db.CountStateStreamLagParams{
		Name:     subscriberName,
		TenantID: tenantID,
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
	return a.queries.ResetStateStreamSubscriber(ctx, db.ResetStateStreamSubscriberParams{
		Name:     subscriberName,
		TenantID: tenantID,
	})
}

// ResetStateStreamSubscriberForStream implements es.StateStreamAdmin.
func (a *Adapter) ResetStateStreamSubscriberForStream(ctx context.Context, subscriberName, tenantID, streamID string) error {
	return a.queries.ResetStateStreamSubscriberForStream(ctx, db.ResetStateStreamSubscriberForStreamParams{
		Name:     subscriberName,
		TenantID: tenantID,
		StreamID: streamID,
	})
}

// ListStateStreamSubscribers implements es.StateStreamAdmin.
func (a *Adapter) ListStateStreamSubscribers(ctx context.Context) ([]string, error) {
	return a.queries.ListStateStreamSubscribers(ctx)
}

var (
	_ es.StateStreamStore = (*Adapter)(nil)
	_ es.StateStreamAdmin = (*Adapter)(nil)
)
