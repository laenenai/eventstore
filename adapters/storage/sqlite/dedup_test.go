package sqlite_test

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/laenenai/eventstore/es"
	"github.com/laenenai/eventstore/projection"
)

// TestWithDedup_SkipsAlreadyProcessed verifies that re-running the
// same projection with the dedup wrapper invokes the inner handler
// once per event_id even when events are re-streamed (cursor rewind).
func TestWithDedup_SkipsAlreadyProcessed(t *testing.T) {
	store, agg := newStoreAndCounter(t)
	seedEvents(t, agg, []string{"t-dedup"}, 3)

	dedup := store.(projection.DedupStore)
	cp := store.(projection.Checkpoint)
	admin := store.(es.ProjectionAdmin)

	var inner atomic.Int64
	rt := &projection.Runtime{
		Name:       "test-dedup",
		Tenant:     "t-dedup",
		Store:      store,
		Checkpoint: cp,
		Handler: projection.WithDedup(
			func(ctx context.Context, env es.Envelope) error {
				inner.Add(1)
				return nil
			},
			dedup,
			"test-dedup",
		),
	}

	if _, err := rt.RunOnce(context.Background()); err != nil {
		t.Fatalf("first run: %v", err)
	}
	if inner.Load() != 3 {
		t.Fatalf("first run inner: got %d want 3", inner.Load())
	}

	// Rewind cursor — every event will be re-streamed. The dedup
	// wrapper should suppress the inner handler.
	if err := admin.Reset(context.Background(), "test-dedup", "t-dedup"); err != nil {
		t.Fatalf("Reset: %v", err)
	}
	if _, err := rt.RunOnce(context.Background()); err != nil {
		t.Fatalf("re-run: %v", err)
	}
	if inner.Load() != 3 {
		t.Errorf("inner after rewind: got %d want 3 (dedup should suppress)", inner.Load())
	}
}
