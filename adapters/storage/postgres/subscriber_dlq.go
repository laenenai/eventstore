package postgres

import (
	"context"

	"github.com/laenenai/eventstore/adapters/storage/postgres/internal/db"
	"github.com/laenenai/eventstore/cmdworkflow"
)

// SubscriberDLQ interfaces (ADR 0025, batched per ADR 0029). See
// sqlite/subscriber_dlq.go for the SQLite mirror; the contract is
// identical. Postgres uses native text[] columns so the slice
// translation is just a passthrough.

// InsertSubscriberDLQ implements cmdworkflow.SubscriberDLQWriter.
func (a *Adapter) InsertSubscriberDLQ(ctx context.Context, row cmdworkflow.SubscriberDLQRow) error {
	if len(row.EventIDs) == 0 {
		return nil
	}
	return a.runForTenant(ctx, row.TenantID, func(q *db.Queries) error {
		return q.InsertSubscriberDLQ(ctx, db.InsertSubscriberDLQParams{
			SubscriberName: row.SubscriberName,
			TenantID:       row.TenantID,
			StreamID:       row.StreamID,
			FirstEventID:   row.EventIDs[0],
			EventIds:       row.EventIDs,
			TypeUrls:       row.TypeURLs,
			LastError:      row.LastError,
			Attempts:       int32(row.Attempts),
			EnqueuedAt:     row.EnqueuedAt.UTC(),
		})
	})
}

// ListSubscriberDLQ implements cmdworkflow.SubscriberDLQAdmin.
func (a *Adapter) ListSubscriberDLQ(ctx context.Context, subscriberName, tenant string, limit int) ([]cmdworkflow.SubscriberDLQRow, error) {
	if limit <= 0 {
		limit = 100
	}
	var rows []db.SubscriberDlq
	err := a.runForTenant(ctx, tenant, func(q *db.Queries) error {
		var inner error
		rows, inner = q.ListSubscriberDLQ(ctx, db.ListSubscriberDLQParams{
			SubscriberName: subscriberName,
			TenantID:       tenant,
			MaxRows:        int32(limit),
		})
		return inner
	})
	if err != nil {
		return nil, err
	}
	out := make([]cmdworkflow.SubscriberDLQRow, len(rows))
	for i, r := range rows {
		out[i] = cmdworkflow.SubscriberDLQRow{
			SubscriberName: r.SubscriberName,
			TenantID:       r.TenantID,
			StreamID:       r.StreamID,
			EventIDs:       r.EventIds,
			TypeURLs:       r.TypeUrls,
			LastError:      r.LastError,
			Attempts:       int(r.Attempts),
			EnqueuedAt:     r.EnqueuedAt,
		}
	}
	return out, nil
}

// ClearSubscriberDLQ implements cmdworkflow.SubscriberDLQAdmin.
func (a *Adapter) ClearSubscriberDLQ(ctx context.Context, subscriberName, tenant string) (int, error) {
	var n int64
	err := a.runForTenant(ctx, tenant, func(q *db.Queries) error {
		var inner error
		n, inner = q.ClearSubscriberDLQ(ctx, db.ClearSubscriberDLQParams{
			SubscriberName: subscriberName,
			TenantID:       tenant,
		})
		return inner
	})
	return int(n), err
}

// DeleteSubscriberDLQRow implements cmdworkflow.SubscriberDLQAdmin.
// Keyed by the first event id in the batch.
func (a *Adapter) DeleteSubscriberDLQRow(ctx context.Context, subscriberName, tenant, firstEventID string) error {
	return a.runForTenant(ctx, tenant, func(q *db.Queries) error {
		return q.DeleteSubscriberDLQRow(ctx, db.DeleteSubscriberDLQRowParams{
			SubscriberName: subscriberName,
			TenantID:       tenant,
			FirstEventID:   firstEventID,
		})
	})
}
