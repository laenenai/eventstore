package sqlite

import (
	"context"
	"time"

	"github.com/laenenai/eventstore/adapters/storage/sqlite/internal/db"
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
		MaxRows:  int64(limit),
	})
	if err != nil {
		return nil, err
	}
	out := make([]es.StateEnvelope, 0, len(rows))
	for _, r := range rows {
		updated, err := parseTime(r.UpdatedAt)
		if err != nil {
			return nil, err
		}
		out = append(out, es.StateEnvelope{
			TenantID:           r.TenantID,
			StreamID:           r.StreamID,
			TypeURL:            r.TypeUrl,
			Version:            uint64(r.Version),
			StateSchemaVersion: uint32(r.StateSchemaVersion),
			State:              stateBytes(r.State),
			UpdatedAt:          updated,
		})
	}
	return out, nil
}

// AdvanceStateStreamPosition implements es.StateStreamStore.
func (a *Adapter) AdvanceStateStreamPosition(ctx context.Context, subscriberName, tenantID, streamID string, version uint64) error {
	return a.queries.UpsertStateStreamPosition(ctx, db.UpsertStateStreamPositionParams{
		Name:      subscriberName,
		TenantID:  tenantID,
		StreamID:  streamID,
		Version:   int64(version),
		UpdatedAt: formatTime(time.Now().UTC()),
	})
}

// ---- StateStreamAdmin ---------------------------------------------------

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
		MaxLagVersions: uint64Of(row.MaxLagVersions),
	}, nil
}

// uint64Of coerces sqlc's interface{} (which SQLite's COALESCE
// returns as untyped) into a uint64. In practice the driver scans
// into int64; defensive against other numeric types.
func uint64Of(v interface{}) uint64 {
	switch x := v.(type) {
	case int64:
		if x < 0 {
			return 0
		}
		return uint64(x)
	case uint64:
		return x
	case int:
		if x < 0 {
			return 0
		}
		return uint64(x)
	case float64:
		if x < 0 {
			return 0
		}
		return uint64(x)
	}
	return 0
}

func (a *Adapter) ResetStateStreamSubscriber(ctx context.Context, subscriberName, tenantID string) (int64, error) {
	return a.queries.ResetStateStreamSubscriber(ctx, db.ResetStateStreamSubscriberParams{
		Name:     subscriberName,
		TenantID: tenantID,
	})
}

func (a *Adapter) ResetStateStreamSubscriberForStream(ctx context.Context, subscriberName, tenantID, streamID string) error {
	return a.queries.ResetStateStreamSubscriberForStream(ctx, db.ResetStateStreamSubscriberForStreamParams{
		Name:     subscriberName,
		TenantID: tenantID,
		StreamID: streamID,
	})
}

func (a *Adapter) ListStateStreamSubscribers(ctx context.Context) ([]string, error) {
	return a.queries.ListStateStreamSubscribers(ctx)
}

var (
	_ es.StateStreamStore = (*Adapter)(nil)
	_ es.StateStreamAdmin = (*Adapter)(nil)
)
