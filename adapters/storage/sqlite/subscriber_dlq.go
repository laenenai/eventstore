package sqlite

import (
	"context"
	"encoding/json"
	"time"

	"github.com/laenenai/eventstore/adapters/storage/sqlite/internal/db"
	"github.com/laenenai/eventstore/cmdworkflow"
)

// SubscriberDLQ interfaces (ADR 0025, batched per ADR 0029).
// InsertSubscriberDLQ writes one quarantined command-batch; the admin
// methods are operator surfaces for dashboards / runbooks.
//
// SQLite lacks native arrays, so event_ids and type_urls are stored
// as JSON-encoded TEXT columns. Encode/decode happens here at the
// adapter boundary; the cmdworkflow.SubscriberDLQRow exposes them as
// []string to callers regardless of underlying storage.

// InsertSubscriberDLQ implements cmdworkflow.SubscriberDLQWriter.
func (a *Adapter) InsertSubscriberDLQ(ctx context.Context, row cmdworkflow.SubscriberDLQRow) error {
	if len(row.EventIDs) == 0 {
		// Defensive — onExhausted always passes a non-empty batch, but
		// guard against a hand-rolled writer with a zero-length list.
		return nil
	}
	eventIDsJSON, err := json.Marshal(row.EventIDs)
	if err != nil {
		return err
	}
	typeURLsJSON, err := json.Marshal(row.TypeURLs)
	if err != nil {
		return err
	}
	return a.queries.InsertSubscriberDLQ(ctx, db.InsertSubscriberDLQParams{
		SubscriberName: row.SubscriberName,
		TenantID:       row.TenantID,
		StreamID:       row.StreamID,
		FirstEventID:   row.EventIDs[0],
		EventIds:       string(eventIDsJSON),
		TypeUrls:       string(typeURLsJSON),
		LastError:      row.LastError,
		Attempts:       int64(row.Attempts),
		EnqueuedAt:     row.EnqueuedAt.UTC().Format(time.RFC3339Nano),
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
		MaxRows:        int64(limit),
	})
	if err != nil {
		return nil, err
	}
	out := make([]cmdworkflow.SubscriberDLQRow, len(rows))
	for i, r := range rows {
		var eventIDs, typeURLs []string
		_ = json.Unmarshal([]byte(r.EventIds), &eventIDs)
		_ = json.Unmarshal([]byte(r.TypeUrls), &typeURLs)
		enq, _ := time.Parse(time.RFC3339Nano, r.EnqueuedAt)
		out[i] = cmdworkflow.SubscriberDLQRow{
			SubscriberName: r.SubscriberName,
			TenantID:       r.TenantID,
			StreamID:       r.StreamID,
			EventIDs:       eventIDs,
			TypeURLs:       typeURLs,
			LastError:      r.LastError,
			Attempts:       int(r.Attempts),
			EnqueuedAt:     enq,
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
// Keyed by the first event id in the batch.
func (a *Adapter) DeleteSubscriberDLQRow(ctx context.Context, subscriberName, tenant, firstEventID string) error {
	return a.queries.DeleteSubscriberDLQRow(ctx, db.DeleteSubscriberDLQRowParams{
		SubscriberName: subscriberName,
		TenantID:       tenant,
		FirstEventID:   firstEventID,
	})
}
