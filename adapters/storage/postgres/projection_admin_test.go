package postgres_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/laenenai/eventstore/es"
	"github.com/laenenai/eventstore/projection"
)

// TestProjectionAdmin_PG_ResetAndStatus mirrors the SQLite test
// against the real Postgres adapter.
func TestProjectionAdmin_PG_ResetAndStatus(t *testing.T) {
	agg := newCounterRuntime(t)
	tenant := tnt(t, "admin")
	seedEvents(t, agg, []string{tenant}, 4)

	var count atomic.Int64
	rt := &projection.Runtime{
		Name:       "pg-admin",
		Tenant:     tenant,
		Store:      adapter,
		Checkpoint: adapter,
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

	status, err := adapter.Status(context.Background(), "pg-admin", tenant)
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if status.Cursor == 0 {
		t.Errorf("cursor stayed at 0")
	}

	count.Store(0)
	if err := adapter.Reset(context.Background(), "pg-admin", tenant); err != nil {
		t.Fatalf("Reset: %v", err)
	}
	if _, err := rt.RunOnce(context.Background()); err != nil {
		t.Fatalf("post-reset run: %v", err)
	}
	if count.Load() != 4 {
		t.Errorf("post-reset count: got %d want 4", count.Load())
	}
}

func TestProjectionAdmin_PG_StatusNotFound(t *testing.T) {
	_, err := adapter.Status(context.Background(), tnt(t, "never"), "x")
	if !errors.Is(err, es.ErrStateNotFound) {
		t.Errorf("got err=%v want ErrStateNotFound", err)
	}
}

// TestProjection_PG_LockerExclusive verifies pg_try_advisory_lock
// prevents two concurrent runners with the same LockKey from
// processing events simultaneously. Mirror of the drain locker test.
func TestProjection_PG_LockerExclusive(t *testing.T) {
	agg := newCounterRuntime(t)
	tenant := tnt(t, "lock")
	seedEvents(t, agg, []string{tenant}, 3)

	// Runner A holds the lock via a handler that blocks until released.
	holdRelease := make(chan struct{})
	holdEntered := make(chan struct{})
	var once sync.Once

	rtA := &projection.Runtime{
		Name:    "pg-lock",
		Tenant:  tenant,
		LockKey: "pg-lock-key",
		Store:   adapter, Checkpoint: adapter,
		Handler: func(ctx context.Context, env es.Envelope) error {
			once.Do(func() { close(holdEntered) })
			<-holdRelease
			return nil
		},
	}
	go func() { _, _ = rtA.RunOnce(context.Background()) }()
	<-holdEntered

	// Runner B with the same lock key should see the lock held and
	// return (0, nil) cleanly without invoking its handler.
	var bCalls atomic.Int64
	rtB := &projection.Runtime{
		Name:    "pg-lock",
		Tenant:  tenant,
		LockKey: "pg-lock-key",
		Store:   adapter, Checkpoint: adapter,
		Handler: func(ctx context.Context, env es.Envelope) error {
			bCalls.Add(1)
			return nil
		},
	}
	n, err := rtB.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("runner B: %v", err)
	}
	if n != 0 || bCalls.Load() != 0 {
		t.Errorf("runner B should have skipped: n=%d calls=%d", n, bCalls.Load())
	}

	close(holdRelease)
}
