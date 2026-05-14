package postgres

import (
	"context"

	"github.com/laenenai/eventstore/adapters/storage/postgres/internal/db"
	"github.com/laenenai/eventstore/cmdworkflow"
)

// SubscriberDLQ interfaces (ADR 0025). See sqlite/subscriber_dlq.go
// for the SQLite mirror; the contract is identical.

// InsertSubscriberDLQ implements cmdworkflow.SubscriberDLQWriter.
func (a *Adapter) InsertSubscriberDLQ(ctx context.Context, row cmdworkflow.SubscriberDLQRow) error {
	return a.queries.InsertSubscriberDLQ(ctx, db.InsertSubscriberDLQParams{
		SubscriberName: row.SubscriberName,
		TenantID:       row.TenantID,
		EventID:        row.EventID,
		StreamID:       row.StreamID,
		TypeUrl:        row.TypeURL,
		LastError:      row.LastError,
		Attempts:       int32(row.Attempts),
		EnqueuedAt:     row.EnqueuedAt.UTC(),
	})
}

// ListSubscriberDLQ implements cmdworkflow.SubscriberDLQAdmin.
func (a *Adapter) ListSubscriberDLQ(ctx context.Context, subscriberName, tenant string, limit int) ([]cmdworkflow.SubscriberDLQRow, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := a.queries.ListSubscriberDLQ(ctx, db.ListSubscriberDLQParams{
		SubscriberName: subscriberName,
		TenantID:       tenant,
		MaxRows:        int32(limit),
	})
	if err != nil {
		return nil, err
	}
	out := make([]cmdworkflow.SubscriberDLQRow, len(rows))
	for i, r := range rows {
		out[i] = cmdworkflow.SubscriberDLQRow{
			SubscriberName: r.SubscriberName,
			TenantID:       r.TenantID,
			EventID:        r.EventID,
			StreamID:       r.StreamID,
			TypeURL:        r.TypeUrl,
			LastError:      r.LastError,
			Attempts:       int(r.Attempts),
			EnqueuedAt:     r.EnqueuedAt,
		}
	}
	return out, nil
}

// ClearSubscriberDLQ implements cmdworkflow.SubscriberDLQAdmin.
func (a *Adapter) ClearSubscriberDLQ(ctx context.Context, subscriberName, tenant string) (int, error) {
	n, err := a.queries.ClearSubscriberDLQ(ctx, db.ClearSubscriberDLQParams{
		SubscriberName: subscriberName,
		TenantID:       tenant,
	})
	return int(n), err
}

// DeleteSubscriberDLQRow implements cmdworkflow.SubscriberDLQAdmin.
func (a *Adapter) DeleteSubscriberDLQRow(ctx context.Context, subscriberName, tenant, eventID string) error {
	return a.queries.DeleteSubscriberDLQRow(ctx, db.DeleteSubscriberDLQRowParams{
		SubscriberName: subscriberName,
		TenantID:       tenant,
		EventID:        eventID,
	})
}
