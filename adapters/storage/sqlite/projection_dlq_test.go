package sqlite_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/laenenai/eventstore/es"
	"github.com/laenenai/eventstore/projection"
)

// TestProjection_DLQOnFailure_CapturesAndContinues verifies that with
// DLQOnFailure set, a handler error captures the event to projection_dlq
// and the cursor advances past — subsequent events still process.
func TestProjection_DLQOnFailure_CapturesAndContinues(t *testing.T) {
	store, agg := newStoreAndCounter(t)
	seedEvents(t, agg, []string{"t-dlq"}, 5)

	var seen atomic.Int64
	failAt := int64(3)

	rt := &projection.Runtime{
		Name:         "test-dlq",
		Tenant:       "t-dlq",
		Store:        store,
		Checkpoint:   store.(projection.Checkpoint),
		DLQOnFailure: true,
		Handler: func(ctx context.Context, env es.Envelope) error {
			n := seen.Add(1)
			if n == failAt {
				return errors.New("simulated handler bug")
			}
			return nil
		},
	}

	processed, err := rt.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	// 4 successes (1, 2, 4, 5); event 3 went to DLQ.
	if processed != 4 {
		t.Errorf("processed: got %d want 4 (event 3 to DLQ, rest succeeded)", processed)
	}

	dlqAdmin := store.(es.ProjectionDLQAdmin)
	count, err := dlqAdmin.CountProjectionDLQ(context.Background(), "test-dlq", "t-dlq")
	if err != nil {
		t.Fatalf("CountProjectionDLQ: %v", err)
	}
	if count != 1 {
		t.Errorf("DLQ count: got %d want 1", count)
	}

	rows, err := dlqAdmin.ListProjectionDLQ(context.Background(), "test-dlq", "t-dlq", 0, 10)
	if err != nil {
		t.Fatalf("ListProjectionDLQ: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("ListProjectionDLQ rows: got %d want 1", len(rows))
	}
	if rows[0].LastError == "" {
		t.Errorf("DLQ row missing last_error")
	}
}

// TestProjection_DLQOnFailure_Abandon verifies the operator action
// removes the DLQ marker.
func TestProjection_DLQOnFailure_Abandon(t *testing.T) {
	store, agg := newStoreAndCounter(t)
	seedEvents(t, agg, []string{"t-dlq-abandon"}, 3)

	failed := false
	rt := &projection.Runtime{
		Name:         "test-dlq-abandon",
		Tenant:       "t-dlq-abandon",
		Store:        store,
		Checkpoint:   store.(projection.Checkpoint),
		DLQOnFailure: true,
		Handler: func(ctx context.Context, env es.Envelope) error {
			if !failed {
				failed = true
				return errors.New("once")
			}
			return nil
		},
	}
	if _, err := rt.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}

	dlqAdmin := store.(es.ProjectionDLQAdmin)
	rows, _ := dlqAdmin.ListProjectionDLQ(context.Background(), "test-dlq-abandon", "t-dlq-abandon", 0, 10)
	if len(rows) != 1 {
		t.Fatalf("expected 1 DLQ row, got %d", len(rows))
	}

	if err := dlqAdmin.AbandonProjectionDLQ(context.Background(),
		"test-dlq-abandon", "t-dlq-abandon", rows[0].GlobalPosition); err != nil {
		t.Fatalf("AbandonProjectionDLQ: %v", err)
	}
	count, _ := dlqAdmin.CountProjectionDLQ(context.Background(), "test-dlq-abandon", "t-dlq-abandon")
	if count != 0 {
		t.Errorf("count after abandon: got %d want 0", count)
	}
}

// TestProjection_DLQOnFailure_Disabled verifies that without the flag,
// the old fail-stop semantics still apply (Q3B last-success advance).
func TestProjection_DLQOnFailure_Disabled(t *testing.T) {
	store, agg := newStoreAndCounter(t)
	seedEvents(t, agg, []string{"t-noDLQ"}, 5)

	var seen atomic.Int64
	rt := &projection.Runtime{
		Name:       "test-no-dlq",
		Tenant:     "t-noDLQ",
		Store:      store,
		Checkpoint: store.(projection.Checkpoint),
		// DLQOnFailure deliberately unset.
		Handler: func(ctx context.Context, env es.Envelope) error {
			if seen.Add(1) == 3 {
				return errors.New("fail")
			}
			return nil
		},
	}
	_, err := rt.RunOnce(context.Background())
	if err == nil {
		t.Fatalf("expected error, got nil")
	}

	dlqAdmin := store.(es.ProjectionDLQAdmin)
	count, _ := dlqAdmin.CountProjectionDLQ(context.Background(), "test-no-dlq", "t-noDLQ")
	if count != 0 {
		t.Errorf("DLQ count: got %d want 0 (DLQOnFailure not set)", count)
	}
}
