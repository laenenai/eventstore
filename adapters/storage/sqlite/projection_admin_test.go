package sqlite_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/laenenai/eventstore/es"
	"github.com/laenenai/eventstore/projection"
)

// TestProjectionAdmin_ResetAndStatus verifies the operator surface for
// rebuilding a projection: run, capture checkpoint, reset, re-run.
func TestProjectionAdmin_ResetAndStatus(t *testing.T) {
	store, agg := newStoreAndCounter(t)
	seedEvents(t, agg, []string{"t-admin"}, 4)

	cp := store.(projection.Checkpoint)
	admin := store.(es.ProjectionAdmin)

	var count atomic.Int64
	rt := &projection.Runtime{
		Name:       "test-admin",
		Tenant:     "t-admin",
		Store:      store,
		Checkpoint: cp,
		Handler: func(ctx context.Context, env es.Envelope) error {
			count.Add(1)
			return nil
		},
	}

	if _, err := rt.RunOnce(context.Background()); err != nil {
		t.Fatalf("first run: %v", err)
	}
	if count.Load() != 4 {
		t.Fatalf("first run count: got %d want 4", count.Load())
	}

	status, err := admin.Status(context.Background(), "test-admin", "t-admin")
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if status.Cursor == 0 {
		t.Errorf("cursor: got 0, expected non-zero after successful run")
	}

	// Reset and re-run: handler must see all 4 events again.
	count.Store(0)
	if err := admin.Reset(context.Background(), "test-admin", "t-admin"); err != nil {
		t.Fatalf("Reset: %v", err)
	}
	if _, err := rt.RunOnce(context.Background()); err != nil {
		t.Fatalf("post-reset run: %v", err)
	}
	if count.Load() != 4 {
		t.Errorf("post-reset count: got %d want 4", count.Load())
	}
}

// TestProjectionAdmin_ResetTo verifies partial replay from a specific
// global_position.
func TestProjectionAdmin_ResetTo(t *testing.T) {
	store, agg := newStoreAndCounter(t)
	seedEvents(t, agg, []string{"t-partial"}, 5)

	cp := store.(projection.Checkpoint)
	admin := store.(es.ProjectionAdmin)

	var count atomic.Int64
	rt := &projection.Runtime{
		Name:       "test-partial",
		Tenant:     "t-partial",
		Store:      store,
		Checkpoint: cp,
		Handler: func(ctx context.Context, env es.Envelope) error {
			count.Add(1)
			return nil
		},
	}
	if _, err := rt.RunOnce(context.Background()); err != nil {
		t.Fatalf("first run: %v", err)
	}

	status, _ := admin.Status(context.Background(), "test-partial", "t-partial")
	endCursor := status.Cursor

	// Rewind to position 2 — first 2 events stay applied; events 3-5
	// will be re-processed.
	if err := admin.ResetTo(context.Background(), "test-partial", "t-partial", 2); err != nil {
		t.Fatalf("ResetTo: %v", err)
	}
	count.Store(0)
	if _, err := rt.RunOnce(context.Background()); err != nil {
		t.Fatalf("partial replay: %v", err)
	}
	// 3 events should be reprocessed (positions 3, 4, 5).
	if count.Load() != 3 {
		t.Errorf("partial replay count: got %d want 3", count.Load())
	}

	// Status reflects the cursor advancing back to end.
	status, _ = admin.Status(context.Background(), "test-partial", "t-partial")
	if status.Cursor != endCursor {
		t.Errorf("cursor after replay: got %d want %d", status.Cursor, endCursor)
	}
}

// TestProjectionAdmin_List enumerates known projectors.
func TestProjectionAdmin_List(t *testing.T) {
	store, agg := newStoreAndCounter(t)
	seedEvents(t, agg, []string{"t-listA", "t-listB"}, 2)

	cp := store.(projection.Checkpoint)
	admin := store.(es.ProjectionAdmin)

	for _, tenant := range []string{"t-listA", "t-listB"} {
		rt := &projection.Runtime{
			Name: "test-list", Tenant: tenant, Store: store, Checkpoint: cp,
			Handler: func(ctx context.Context, env es.Envelope) error { return nil },
		}
		if _, err := rt.RunOnce(context.Background()); err != nil {
			t.Fatalf("run %s: %v", tenant, err)
		}
	}

	all, err := admin.List(context.Background())
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(all) < 2 {
		t.Errorf("List size: got %d want >= 2", len(all))
	}
	seen := map[string]bool{}
	for _, s := range all {
		seen[s.TenantID] = true
	}
	if !seen["t-listA"] || !seen["t-listB"] {
		t.Errorf("expected both tenants in List output, got %+v", all)
	}
}

// TestProjectionAdmin_StatusNotFound verifies the ErrStateNotFound
// signal for an unknown projector.
func TestProjectionAdmin_StatusNotFound(t *testing.T) {
	store, _ := newStoreAndCounter(t)
	admin := store.(es.ProjectionAdmin)

	_, err := admin.Status(context.Background(), "never-run", "tenant-x")
	if !errors.Is(err, es.ErrStateNotFound) {
		t.Errorf("Status on unknown: got err=%v want ErrStateNotFound", err)
	}
}
