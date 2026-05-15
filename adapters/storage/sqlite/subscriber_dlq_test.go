package sqlite_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	sqliteadapter "github.com/laenenai/eventstore/adapters/storage/sqlite"
	"github.com/laenenai/eventstore/cmdworkflow"
)

// TestSubscriberDLQ_InsertListClear exercises the operator surface
// end-to-end against the SQLite adapter under the batched DLQ shape
// (ADR 0029): one row per (subscriber, failed command-batch).
func TestSubscriberDLQ_InsertListClear(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	a := sqliteadapter.New(db)
	if err := a.Migrate(context.Background()); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	tenant := "t-dlq"
	now := time.Now().UTC().Truncate(time.Second)

	// Insert three batches for one subscriber. Each "batch" has two
	// event ids to exercise the array shape.
	batches := [][]string{
		{"ev-1a", "ev-1b"},
		{"ev-2a"},
		{"ev-3a", "ev-3b", "ev-3c"},
	}
	for i, ids := range batches {
		typeURLs := make([]string, len(ids))
		for j := range ids {
			typeURLs[j] = "myapp.invoice.v1.Created"
		}
		if err := a.InsertSubscriberDLQ(context.Background(), cmdworkflow.SubscriberDLQRow{
			SubscriberName: "search-index",
			TenantID:       tenant,
			StreamID:       "invoice:i-" + ids[0],
			EventIDs:       ids,
			TypeURLs:       typeURLs,
			LastError:      "transient API failure",
			Attempts:       3,
			EnqueuedAt:     now.Add(time.Duration(i) * time.Second),
		}); err != nil {
			t.Fatalf("Insert %s: %v", ids[0], err)
		}
	}

	// List returns all three, ordered by enqueued_at.
	rows, err := a.ListSubscriberDLQ(context.Background(), "search-index", tenant, 10)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(rows) != 3 {
		t.Errorf("list: got %d want 3", len(rows))
	}
	if rows[0].EventIDs[0] != "ev-1a" || rows[2].EventIDs[0] != "ev-3a" {
		t.Errorf("ordering: %v", rows)
	}
	// Batch fields round-trip.
	if len(rows[2].EventIDs) != 3 || rows[2].EventIDs[2] != "ev-3c" {
		t.Errorf("batch round-trip: %+v", rows[2])
	}
	if len(rows[2].TypeURLs) != 3 {
		t.Errorf("type_urls round-trip: %+v", rows[2])
	}

	// Delete one specific row by first event id.
	if err := a.DeleteSubscriberDLQRow(context.Background(), "search-index", tenant, "ev-2a"); err != nil {
		t.Fatalf("DeleteRow: %v", err)
	}
	rows, _ = a.ListSubscriberDLQ(context.Background(), "search-index", tenant, 10)
	if len(rows) != 2 {
		t.Errorf("after delete: got %d want 2", len(rows))
	}

	// Clear wipes the rest.
	cleared, err := a.ClearSubscriberDLQ(context.Background(), "search-index", tenant)
	if err != nil {
		t.Fatalf("Clear: %v", err)
	}
	if cleared != 2 {
		t.Errorf("cleared: got %d want 2", cleared)
	}
}

// TestSubscriberDLQ_AdminInterfaceSatisfied compile-time checks that
// the adapter satisfies the commandbus operator interfaces.
func TestSubscriberDLQ_AdminInterfaceSatisfied(t *testing.T) {
	var _ cmdworkflow.SubscriberDLQWriter = (*sqliteadapter.Adapter)(nil)
	var _ cmdworkflow.SubscriberDLQAdmin = (*sqliteadapter.Adapter)(nil)
}
