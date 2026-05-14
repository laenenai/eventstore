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
// end-to-end against the SQLite adapter. Confirms the adapter wires
// cmdworkflow.SubscriberDLQWriter + Admin interfaces correctly.
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

	// Insert three rows for one subscriber.
	for i, ev := range []string{"ev-1", "ev-2", "ev-3"} {
		if err := a.InsertSubscriberDLQ(context.Background(), cmdworkflow.SubscriberDLQRow{
			SubscriberName: "search-index",
			TenantID:       tenant,
			EventID:        ev,
			StreamID:       "invoice:i-" + ev,
			TypeURL:        "myapp.invoice.v1.Created",
			LastError:      "transient API failure",
			Attempts:       3,
			EnqueuedAt:     now.Add(time.Duration(i) * time.Second),
		}); err != nil {
			t.Fatalf("Insert %s: %v", ev, err)
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
	if rows[0].EventID != "ev-1" || rows[2].EventID != "ev-3" {
		t.Errorf("ordering: %v", rows)
	}

	// Delete one specific row.
	if err := a.DeleteSubscriberDLQRow(context.Background(), "search-index", tenant, "ev-2"); err != nil {
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
