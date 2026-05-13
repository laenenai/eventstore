package sqlite_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/laenenai/eventstore/es"
	"github.com/laenenai/eventstore/outbox"
	"github.com/laenenai/eventstore/publisher/inproc"
)

// Integration tests for the outbox drain + inproc publisher against
// the SQLite adapter. Uses the Counter domain for event production.

// TestOutbox_DrainPublishesAndMarksAllPending verifies the happy path:
// events appended -> drain runs -> subscriber sees every event ->
// outbox rows are marked published (subsequent drain returns 0).
func TestOutbox_DrainPublishesAndMarksAllPending(t *testing.T) {
	store, agg := newStoreAndCounter(t)
	want := seedEvents(t, agg, []string{"t-drain-a", "t-drain-b"}, 3)

	pub := inproc.New()
	var received atomic.Int64
	pub.Subscribe(func(ctx context.Context, env es.Envelope) error {
		received.Add(1)
		return nil
	})

	d := &outbox.Drain{
		Store:     store.(es.OutboxStore),
		Publisher: pub,
	}
	published, _, err := d.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if published != want {
		t.Errorf("published: got %d want %d", published, want)
	}
	if int(received.Load()) != want {
		t.Errorf("subscriber received: got %d want %d", received.Load(), want)
	}

	// Second drain: no rows to process.
	published2, _, err := d.Run(context.Background())
	if err != nil {
		t.Fatalf("second Run: %v", err)
	}
	if published2 != 0 {
		t.Errorf("second drain: got %d want 0 (all rows already published)", published2)
	}
}

// TestOutbox_PublisherErrorMarksFailed verifies that a Publish error
// keeps the row pending and records the error.
func TestOutbox_PublisherErrorMarksFailed(t *testing.T) {
	store, agg := newStoreAndCounter(t)
	seedEvents(t, agg, []string{"t-fail"}, 2)

	pub := inproc.New()
	var attempts atomic.Int64
	pub.Subscribe(func(ctx context.Context, env es.Envelope) error {
		attempts.Add(1)
		return errors.New("subscriber failure")
	})

	d := &outbox.Drain{
		Store:     store.(es.OutboxStore),
		Publisher: pub,
	}
	published, _, err := d.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if published != 0 {
		t.Errorf("published: got %d want 0 (all should have failed)", published)
	}
	if attempts.Load() != 2 {
		t.Errorf("attempts: got %d want 2", attempts.Load())
	}

	// Rows should still be pending; a second drain re-attempts them.
	pending, err := store.(es.OutboxStore).PendingOutbox(context.Background(), "", 100)
	if err != nil {
		t.Fatalf("PendingOutbox: %v", err)
	}
	if len(pending) != 2 {
		t.Errorf("pending: got %d want 2", len(pending))
	}
	// Each row should have attempts=1 after one failed drain run.
	for _, r := range pending {
		if r.Attempts != 1 {
			t.Errorf("attempts on pending row: got %d want 1", r.Attempts)
		}
	}
}

// TestOutbox_TenantScopedDrain verifies that setting Tenant filters
// rows to a single tenant.
func TestOutbox_TenantScopedDrain(t *testing.T) {
	store, agg := newStoreAndCounter(t)
	seedEvents(t, agg, []string{"t-scope-x", "t-scope-y"}, 4)
	// 4 + 4 = 8 outbox rows total, 4 per tenant.

	pub := inproc.New()
	var seen []string
	pub.Subscribe(func(ctx context.Context, env es.Envelope) error {
		seen = append(seen, env.TenantID)
		return nil
	})

	d := &outbox.Drain{
		Store:     store.(es.OutboxStore),
		Publisher: pub,
		Tenant:    "t-scope-x",
	}
	published, _, err := d.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if published != 4 {
		t.Errorf("published for tenant-x: got %d want 4", published)
	}
	for _, tenant := range seen {
		if tenant != "t-scope-x" {
			t.Errorf("wrong tenant leaked through: %q", tenant)
		}
	}

	// Other tenant's rows should still be pending across all-tenants drain.
	pending, _ := store.(es.OutboxStore).PendingOutbox(context.Background(), "", 100)
	if len(pending) != 4 {
		t.Errorf("pending after tenant-scoped drain: got %d want 4 (other tenant)", len(pending))
	}
	for _, r := range pending {
		if r.Envelope.TenantID != "t-scope-y" {
			t.Errorf("unexpected tenant in pending: %q", r.Envelope.TenantID)
		}
	}
}

// TestOutbox_PartialFailureBatchContinues verifies that one failing
// row doesn't stop the drain from processing later rows within the
// same batch. Uses RunOnce (single batch) so the failing row stays
// failed — Run would retry it on the next iteration.
func TestOutbox_PartialFailureBatchContinues(t *testing.T) {
	store, agg := newStoreAndCounter(t)
	seedEvents(t, agg, []string{"t-partial"}, 3)

	pub := inproc.New()
	var calls atomic.Int64
	pub.Subscribe(func(ctx context.Context, env es.Envelope) error {
		n := calls.Add(1)
		if n == 2 {
			return errors.New("transient failure on second")
		}
		return nil
	})

	d := &outbox.Drain{
		Store:     store.(es.OutboxStore),
		Publisher: pub,
	}
	published, err := d.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if published != 2 {
		t.Errorf("published: got %d want 2 (1 failed, 2 succeeded)", published)
	}

	// The one failed row should still be pending.
	pending, _ := store.(es.OutboxStore).PendingOutbox(context.Background(), "", 100)
	if len(pending) != 1 {
		t.Errorf("pending after partial failure: got %d want 1", len(pending))
	}
}

// TestOutbox_Cleanup runs the cleanup path. We can't directly verify
// row deletion (CleanupPublished is :exec) but the drain should not
// error.
func TestOutbox_Cleanup(t *testing.T) {
	store, agg := newStoreAndCounter(t)
	seedEvents(t, agg, []string{"t-cleanup"}, 2)

	pub := inproc.New()
	pub.Subscribe(func(ctx context.Context, env es.Envelope) error { return nil })

	d := &outbox.Drain{
		Store:            store.(es.OutboxStore),
		Publisher:        pub,
		Tenant:           "t-cleanup",
		CleanupRetention: -time.Hour, // negative -> cutoff is in the future, retains nothing
	}
	// First Run publishes everything.
	if _, _, err := d.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// With default cleanup retention (positive), nothing is deleted yet.
	d.CleanupRetention = 24 * time.Hour
	_, _, err := d.Run(context.Background())
	if err != nil {
		t.Fatalf("second Run with cleanup: %v", err)
	}
	// We can't verify the cleanup did anything; just verify no error.
}

// TestOutbox_DrainValidation checks missing required fields.
func TestOutbox_DrainValidation(t *testing.T) {
	pub := inproc.New()
	cases := []struct {
		name  string
		drain *outbox.Drain
	}{
		{"missing store", &outbox.Drain{Publisher: pub}},
		{"missing publisher", &outbox.Drain{Store: nil}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := tc.drain.Run(context.Background())
			if err == nil {
				t.Errorf("expected validation error, got nil")
			}
		})
	}
}
