package sqlite_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	sqliteadapter "github.com/laenenai/eventstore/adapters/storage/sqlite"
	"github.com/laenenai/eventstore/aggregate"
	"github.com/laenenai/eventstore/es"
	"github.com/laenenai/eventstore/estest"
	counterv1 "github.com/laenenai/eventstore/gen/test/counter/v1"
	"github.com/laenenai/eventstore/projection"
)

// Tests for the projection.Runtime against the SQLite adapter. Uses
// the Counter domain (same fixture as aggregate_test.go) to produce a
// stream of events the projector can consume.

func newStoreAndCounter(t *testing.T) (es.Store, *aggregate.Runtime[counterState, counterv1.Command, counterv1.Event]) {
	t.Helper()
	// Use a temp file rather than :memory: so concurrent goroutines
	// (aggregate writer + projection runtime reader) share the same
	// database. With database/sql's connection pooling, each :memory:
	// connection is a separate private DB — concurrent tests would
	// see one another's writes only by accident.
	dsn := fmt.Sprintf("file:%s/test.db?_pragma=journal_mode(WAL)&_pragma=foreign_keys(ON)",
		t.TempDir())
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	a := sqliteadapter.New(db)
	if err := a.Migrate(context.Background()); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	rt := &aggregate.Runtime[counterState, counterv1.Command, counterv1.Event]{
		Store:   a,
		Decider: counterDecider,
		Codec:   counterv1.EventCodec{},
	}
	return a, rt
}

// seedEvents appends N events across the given tenants. Returns the
// number of events created.
func seedEvents(t *testing.T, rt *aggregate.Runtime[counterState, counterv1.Command, counterv1.Event], tenants []string, perTenant int) int {
	t.Helper()
	for _, tenant := range tenants {
		ctx := es.WithTenant(context.Background(), tenant)
		sid := estest.MustStream(t, tenant, "counter", "1")
		if _, err := rt.Handle(ctx, sid, &counterv1.Init{Min: 0, Max: 1000, Initial: 0}); err != nil {
			t.Fatalf("init %s: %v", tenant, err)
		}
		for i := 1; i < perTenant; i++ {
			if _, err := rt.Handle(ctx, sid, &counterv1.Increment{By: 1}); err != nil {
				t.Fatalf("inc %s: %v", tenant, err)
			}
		}
	}
	return len(tenants) * perTenant
}

// TestProjection_RunOnce_ProcessesAllEvents verifies the basic flow:
// append events, run-once, handler sees every event, checkpoint
// advances to the last global_position.
func TestProjection_RunOnce_ProcessesAllEvents(t *testing.T) {
	store, agg := newStoreAndCounter(t)
	want := seedEvents(t, agg, []string{"t-proj-a", "t-proj-b"}, 3)

	var count atomic.Int64
	rt := &projection.Runtime{
		Name:       "test-counter-1",
		Store:      store,
		Checkpoint: projection.NewMemoryCheckpoint(),
		Handler: func(ctx context.Context, env es.Envelope) error {
			count.Add(1)
			return nil
		},
	}

	processed, err := rt.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if processed != want {
		t.Errorf("processed: got %d want %d", processed, want)
	}
	if int(count.Load()) != want {
		t.Errorf("handler invocations: got %d want %d", count.Load(), want)
	}
}

// TestProjection_RunOnce_HonorsCheckpoint verifies the cursor is
// persisted and reused: a second RunOnce after no new events returns 0.
func TestProjection_RunOnce_HonorsCheckpoint(t *testing.T) {
	store, agg := newStoreAndCounter(t)
	seedEvents(t, agg, []string{"t-cp"}, 5)

	cp := projection.NewMemoryCheckpoint()
	rt := &projection.Runtime{
		Name: "test-counter-cp", Store: store, Checkpoint: cp,
		Handler: func(ctx context.Context, env es.Envelope) error { return nil },
	}

	first, err := rt.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("first RunOnce: %v", err)
	}
	if first != 5 {
		t.Errorf("first processed: got %d want 5", first)
	}

	// Second RunOnce: no new events.
	second, err := rt.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("second RunOnce: %v", err)
	}
	if second != 0 {
		t.Errorf("second processed: got %d want 0 (cursor should have advanced)", second)
	}

	// Append more events, RunOnce again: only new events processed.
	ctx := es.WithTenant(context.Background(), "t-cp")
	sid := estest.MustStream(t, "t-cp", "counter", "1")
	if _, err := agg.Handle(ctx, sid, &counterv1.Increment{By: 1}); err != nil {
		t.Fatalf("extra inc: %v", err)
	}
	third, err := rt.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("third RunOnce: %v", err)
	}
	if third != 1 {
		t.Errorf("third processed: got %d want 1", third)
	}
}

// TestProjection_RunOnce_TenantScoped verifies that setting Tenant
// filters events to a single tenant.
func TestProjection_RunOnce_TenantScoped(t *testing.T) {
	store, agg := newStoreAndCounter(t)
	seedEvents(t, agg, []string{"t-scope-a", "t-scope-b"}, 4)
	// 4 + 4 = 8 events total, 4 per tenant.

	var aCount atomic.Int64
	rtA := &projection.Runtime{
		Name: "test-tenant-a", Store: store, Tenant: "t-scope-a",
		Checkpoint: projection.NewMemoryCheckpoint(),
		Handler: func(ctx context.Context, env es.Envelope) error {
			if env.TenantID != "t-scope-a" {
				return errors.New("wrong tenant leaked through")
			}
			aCount.Add(1)
			return nil
		},
	}
	processed, err := rtA.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if processed != 4 {
		t.Errorf("processed for tenant-a: got %d want 4", processed)
	}
	if aCount.Load() != 4 {
		t.Errorf("handler invocations: got %d want 4", aCount.Load())
	}
}

// TestProjection_RunOnce_HandlerErrorStops verifies that a handler
// error halts the projection without advancing the checkpoint.
func TestProjection_RunOnce_HandlerErrorStops(t *testing.T) {
	store, agg := newStoreAndCounter(t)
	seedEvents(t, agg, []string{"t-err"}, 5)

	var seen atomic.Int64
	failAt := int64(3)
	cp := projection.NewMemoryCheckpoint()

	rt := &projection.Runtime{
		Name: "test-err", Store: store, Checkpoint: cp,
		Handler: func(ctx context.Context, env es.Envelope) error {
			n := seen.Add(1)
			if n == failAt {
				return errors.New("simulated handler failure")
			}
			return nil
		},
	}
	_, err := rt.RunOnce(context.Background())
	if err == nil {
		t.Fatalf("expected error, got nil")
	}

	// Checkpoint should NOT have advanced past the failure.
	pos, _ := cp.Load(context.Background(), "test-err")
	if pos != 0 {
		t.Errorf("checkpoint advanced despite error: got %d want 0", pos)
	}

	// A successful re-run should re-process from the start.
	seen.Store(0)
	rt.Handler = func(ctx context.Context, env es.Envelope) error {
		seen.Add(1)
		return nil
	}
	processed, err := rt.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("re-run: %v", err)
	}
	if processed != 5 {
		t.Errorf("re-run processed: got %d want 5", processed)
	}
}

// TestProjection_Run_StopsOnContextCancel verifies the continuous loop
// exits cleanly when the context is cancelled.
func TestProjection_Run_StopsOnContextCancel(t *testing.T) {
	store, agg := newStoreAndCounter(t)
	seedEvents(t, agg, []string{"t-run"}, 3)

	var count atomic.Int64
	rt := &projection.Runtime{
		Name: "test-run", Store: store,
		Checkpoint: projection.NewMemoryCheckpoint(),
		IdleSleep:  10 * time.Millisecond,
		Handler: func(ctx context.Context, env es.Envelope) error {
			count.Add(1)
			return nil
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- rt.Run(ctx) }()

	// Give it time to drain the seeded events.
	time.Sleep(100 * time.Millisecond)

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Run returned: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatalf("Run did not stop after cancel")
	}

	if count.Load() != 3 {
		t.Errorf("processed: got %d want 3", count.Load())
	}
}

// TestProjection_Run_PicksUpNewEvents verifies the continuous loop
// keeps processing events appended after it started.
func TestProjection_Run_PicksUpNewEvents(t *testing.T) {
	store, agg := newStoreAndCounter(t)

	var count atomic.Int64
	rt := &projection.Runtime{
		Name: "test-live", Store: store,
		Checkpoint: projection.NewMemoryCheckpoint(),
		IdleSleep:  10 * time.Millisecond,
		Handler: func(ctx context.Context, env es.Envelope) error {
			count.Add(1)
			return nil
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- rt.Run(ctx) }()

	// Append events while the projector is running.
	hctx := es.WithTenant(context.Background(), "t-live")
	sid := estest.MustStream(t, "t-live", "counter", "1")
	if _, err := agg.Handle(hctx, sid, &counterv1.Init{Min: 0, Max: 100, Initial: 0}); err != nil {
		t.Fatalf("init: %v", err)
	}
	for i := 0; i < 5; i++ {
		if _, err := agg.Handle(hctx, sid, &counterv1.Increment{By: 1}); err != nil {
			t.Fatalf("inc: %v", err)
		}
	}

	// Wait for the projector to catch up.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if count.Load() == 6 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if count.Load() != 6 {
		t.Errorf("processed: got %d want 6 (init + 5 incs)", count.Load())
	}

	cancel()
	<-done
}

// TestProjection_RunOnce_BatchSize verifies the BatchSize limits how
// many events are processed per call.
func TestProjection_RunOnce_BatchSize(t *testing.T) {
	store, agg := newStoreAndCounter(t)
	seedEvents(t, agg, []string{"t-batch"}, 10)

	rt := &projection.Runtime{
		Name: "test-batch", Store: store,
		Checkpoint: projection.NewMemoryCheckpoint(),
		BatchSize:  3,
		Handler:    func(ctx context.Context, env es.Envelope) error { return nil },
	}

	processed, err := rt.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if processed != 3 {
		t.Errorf("processed: got %d want 3 (batch size)", processed)
	}

	// Subsequent calls drain the rest, three at a time.
	total := processed
	for i := 0; i < 5; i++ {
		n, err := rt.RunOnce(context.Background())
		if err != nil {
			t.Fatalf("RunOnce: %v", err)
		}
		total += n
		if n == 0 {
			break
		}
	}
	if total != 10 {
		t.Errorf("total processed: got %d want 10", total)
	}
}

// TestProjection_RunOnce_Validation verifies missing required fields
// produce a useful error.
func TestProjection_RunOnce_Validation(t *testing.T) {
	store, _ := newStoreAndCounter(t)
	cases := []struct {
		name    string
		rt      *projection.Runtime
		wantSub string
	}{
		{"missing name", &projection.Runtime{Store: store, Checkpoint: projection.NewMemoryCheckpoint(), Handler: func(context.Context, es.Envelope) error { return nil }}, "Name"},
		{"missing store", &projection.Runtime{Name: "x", Checkpoint: projection.NewMemoryCheckpoint(), Handler: func(context.Context, es.Envelope) error { return nil }}, "Store"},
		{"missing checkpoint", &projection.Runtime{Name: "x", Store: store, Handler: func(context.Context, es.Envelope) error { return nil }}, "Checkpoint"},
		{"missing handler", &projection.Runtime{Name: "x", Store: store, Checkpoint: projection.NewMemoryCheckpoint()}, "Handler"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := tc.rt.RunOnce(context.Background())
			if err == nil || !strings.Contains(err.Error(), tc.wantSub) {
				t.Errorf("expected error mentioning %q, got %v", tc.wantSub, err)
			}
		})
	}
}
