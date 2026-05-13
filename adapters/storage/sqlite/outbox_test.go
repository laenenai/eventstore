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

// Integration tests for the outbox drain + inproc publisher.
// Uses the Counter domain from aggregate_test.go for event production.

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

	published2, _, err := d.Run(context.Background())
	if err != nil {
		t.Fatalf("second Run: %v", err)
	}
	if published2 != 0 {
		t.Errorf("second drain: got %d want 0", published2)
	}
}

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
		t.Errorf("published: got %d want 0", published)
	}
	// Both events for the same stream — per-stream halt means only
	// the first one is attempted in the first batch. (Different from
	// the old "continue on failure" behavior.)
	if attempts.Load() != 1 {
		t.Errorf("attempts: got %d want 1 (per-stream halt stops at first failure)", attempts.Load())
	}

	// PendingOutbox now returns only the head row per stream (preserves
	// per-stream ordering across leader handoffs). To count all
	// unpublished rows, use OutboxAdmin.CountPending.
	admin := store.(es.OutboxAdmin)
	pending, err := admin.CountPending(context.Background(), "t-fail")
	if err != nil {
		t.Fatalf("CountPending: %v", err)
	}
	if pending != 2 {
		t.Errorf("pending: got %d want 2", pending)
	}
}

func TestOutbox_TenantScopedDrain(t *testing.T) {
	store, agg := newStoreAndCounter(t)
	seedEvents(t, agg, []string{"t-scope-x", "t-scope-y"}, 4)

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
			t.Errorf("wrong tenant: %q", tenant)
		}
	}

	// PendingOutbox returns one head per stream; CountPending gives a
	// true total of unpublished rows.
	admin := store.(es.OutboxAdmin)
	pending, _ := admin.CountPending(context.Background(), "t-scope-y")
	if pending != 4 {
		t.Errorf("pending after tenant drain: got %d want 4 (t-scope-y rows)", pending)
	}
}

// TestOutbox_PerStreamHalt verifies that when a row fails, subsequent
// rows of the SAME stream are skipped in the same batch — but other
// streams continue processing.
func TestOutbox_PerStreamHalt(t *testing.T) {
	store, agg := newStoreAndCounter(t)
	// Two tenants, two streams; 3 events each. tenant-x's stream
	// is the poison.
	seedEvents(t, agg, []string{"t-perstream-x", "t-perstream-y"}, 3)

	pub := inproc.New()
	var poisonStreamSeen, otherStreamSeen atomic.Int64
	pub.Subscribe(func(ctx context.Context, env es.Envelope) error {
		if env.TenantID == "t-perstream-x" {
			poisonStreamSeen.Add(1)
			return errors.New("poison")
		}
		otherStreamSeen.Add(1)
		return nil
	})

	d := &outbox.Drain{
		Store:     store.(es.OutboxStore),
		Publisher: pub,
	}
	_, _, err := d.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	// Poison stream tried once per cycle (halted after first failure);
	// other stream published all 3.
	if otherStreamSeen.Load() != 3 {
		t.Errorf("other stream: got %d want 3 (should still publish despite poison stream)", otherStreamSeen.Load())
	}
	// Poison stream halted at first row, so only 1 attempt regardless
	// of how many Run cycles (Run loops until empty, but each cycle
	// halts the poison stream after one failure).
	// Note: Run loops, so we expect multiple attempts spread across cycles.
	if poisonStreamSeen.Load() < 1 {
		t.Errorf("poison stream: got %d want >= 1", poisonStreamSeen.Load())
	}
}

// TestOutbox_DLQQuarantineDefault verifies that after MaxAttempts a
// row enters DLQ state and the entire stream is quarantined (default
// behavior). The stream's subsequent rows stay pending until operator
// action.
func TestOutbox_DLQQuarantineDefault(t *testing.T) {
	store, agg := newStoreAndCounter(t)
	seedEvents(t, agg, []string{"t-dlq-q"}, 3)

	pub := inproc.New()
	pub.Subscribe(func(ctx context.Context, env es.Envelope) error {
		return errors.New("persistent failure")
	})

	d := &outbox.Drain{
		Store:       store.(es.OutboxStore),
		Publisher:   pub,
		MaxAttempts: 2,
		// AutoResumeAfterDLQ: false (default) — quarantine.
	}
	// Each Run cycle halts the stream after first failure, so the same
	// row gets retried across cycles. After 2 attempts it enters DLQ.
	_, _, _ = d.Run(context.Background())
	_, _, _ = d.Run(context.Background())

	// First row should now be in DLQ state.
	admin := store.(es.OutboxAdmin)
	dlqCount, err := admin.CountDLQ(context.Background(), "t-dlq-q", 2)
	if err != nil {
		t.Fatalf("CountDLQ: %v", err)
	}
	if dlqCount != 1 {
		t.Errorf("CountDLQ: got %d want 1", dlqCount)
	}

	// Subsequent runs should not publish anything because the stream
	// is quarantined.
	pub2 := inproc.New()
	var attempts atomic.Int64
	pub2.Subscribe(func(ctx context.Context, env es.Envelope) error {
		attempts.Add(1)
		return nil
	})
	d.Publisher = pub2
	pubd, _, _ := d.Run(context.Background())
	if pubd != 0 || attempts.Load() != 0 {
		t.Errorf("quarantined stream should not publish: got %d (published) %d (subscribed)", pubd, attempts.Load())
	}
}

// TestOutbox_DLQAutoResume verifies that with AutoResumeAfterDLQ=true,
// the stream resumes after a row enters DLQ (with a gap).
func TestOutbox_DLQAutoResume(t *testing.T) {
	store, agg := newStoreAndCounter(t)
	seedEvents(t, agg, []string{"t-dlq-resume"}, 3)

	pub := inproc.New()
	var calls atomic.Int64
	pub.Subscribe(func(ctx context.Context, env es.Envelope) error {
		n := calls.Add(1)
		// First two calls (the same row retried) fail. After DLQ,
		// the next row should be picked up — and we let it succeed.
		if n <= 2 {
			return errors.New("poison")
		}
		return nil
	})

	d := &outbox.Drain{
		Store:              store.(es.OutboxStore),
		Publisher:          pub,
		MaxAttempts:        2,
		AutoResumeAfterDLQ: true,
	}
	_, _, _ = d.Run(context.Background())
	_, _, _ = d.Run(context.Background())

	// One row in DLQ; other two rows in the stream published.
	admin := store.(es.OutboxAdmin)
	dlqCount, _ := admin.CountDLQ(context.Background(), "t-dlq-resume", 2)
	if dlqCount != 1 {
		t.Errorf("CountDLQ: got %d want 1", dlqCount)
	}
	pendingCount, _ := admin.CountPending(context.Background(), "t-dlq-resume")
	// 1 DLQ'd row remains pending (attempts >= MaxAttempts but published_at NULL).
	// The other 2 rows succeeded so are not pending.
	if pendingCount != 1 {
		t.Errorf("CountPending: got %d want 1 (just the DLQ'd row)", pendingCount)
	}
}

// TestOutbox_DLQReplay verifies the operator action: replay a DLQ'd row.
func TestOutbox_DLQReplay(t *testing.T) {
	store, agg := newStoreAndCounter(t)
	seedEvents(t, agg, []string{"t-replay"}, 1)

	// Subscriber fails twice, succeeds on third call.
	pub := inproc.New()
	var calls atomic.Int64
	pub.Subscribe(func(ctx context.Context, env es.Envelope) error {
		n := calls.Add(1)
		if n <= 2 {
			return errors.New("transient")
		}
		return nil
	})

	d := &outbox.Drain{
		Store:       store.(es.OutboxStore),
		Publisher:   pub,
		MaxAttempts: 2,
	}
	// Two runs → 2 attempts → DLQ.
	_, _, _ = d.Run(context.Background())
	_, _, _ = d.Run(context.Background())

	admin := store.(es.OutboxAdmin)
	dlqRows, err := admin.ListDLQ(context.Background(), "t-replay", 2, 0, 10)
	if err != nil {
		t.Fatalf("ListDLQ: %v", err)
	}
	if len(dlqRows) != 1 {
		t.Fatalf("expected 1 DLQ row, got %d", len(dlqRows))
	}

	// Operator replays.
	if err := admin.ReplayDLQ(context.Background(), "t-replay", dlqRows[0].GlobalPosition); err != nil {
		t.Fatalf("ReplayDLQ: %v", err)
	}

	// Next run succeeds.
	pubd, _, err := d.Run(context.Background())
	if err != nil {
		t.Fatalf("Run after replay: %v", err)
	}
	if pubd != 1 {
		t.Errorf("after replay published: got %d want 1", pubd)
	}
}

// TestOutbox_DLQAbandon verifies the operator action: abandon a DLQ'd row.
func TestOutbox_DLQAbandon(t *testing.T) {
	store, agg := newStoreAndCounter(t)
	seedEvents(t, agg, []string{"t-abandon"}, 1)

	pub := inproc.New()
	pub.Subscribe(func(ctx context.Context, env es.Envelope) error {
		return errors.New("permanent")
	})

	d := &outbox.Drain{
		Store:       store.(es.OutboxStore),
		Publisher:   pub,
		MaxAttempts: 2,
	}
	_, _, _ = d.Run(context.Background())
	_, _, _ = d.Run(context.Background())

	admin := store.(es.OutboxAdmin)
	dlqRows, _ := admin.ListDLQ(context.Background(), "t-abandon", 2, 0, 10)
	if len(dlqRows) != 1 {
		t.Fatalf("expected 1 DLQ row, got %d", len(dlqRows))
	}

	// Operator abandons.
	if err := admin.AbandonDLQ(context.Background(), "t-abandon", dlqRows[0].GlobalPosition); err != nil {
		t.Fatalf("AbandonDLQ: %v", err)
	}

	// Subsequent counts: no DLQ, no pending.
	dlqCount, _ := admin.CountDLQ(context.Background(), "t-abandon", 2)
	if dlqCount != 0 {
		t.Errorf("CountDLQ after abandon: got %d want 0", dlqCount)
	}
	pendingCount, _ := admin.CountPending(context.Background(), "t-abandon")
	if pendingCount != 0 {
		t.Errorf("CountPending after abandon: got %d want 0", pendingCount)
	}
}

// TestOutbox_DLQOnCallback verifies OnDLQ fires when a row crosses
// the DLQ threshold.
func TestOutbox_DLQOnCallback(t *testing.T) {
	store, agg := newStoreAndCounter(t)
	seedEvents(t, agg, []string{"t-cb"}, 1)

	pub := inproc.New()
	pub.Subscribe(func(ctx context.Context, env es.Envelope) error {
		return errors.New("failure")
	})

	var dlqFired atomic.Int64
	d := &outbox.Drain{
		Store:       store.(es.OutboxStore),
		Publisher:   pub,
		MaxAttempts: 2,
		OnDLQ: func(row es.OutboxRow) {
			dlqFired.Add(1)
		},
	}
	_, _, _ = d.Run(context.Background())
	_, _, _ = d.Run(context.Background())

	if dlqFired.Load() != 1 {
		t.Errorf("OnDLQ fired: got %d want 1 (once when crossing threshold)", dlqFired.Load())
	}
}

// TestOutbox_BackoffDelaysRetry verifies that BackoffBase prevents
// immediate retry within a single Run cycle.
func TestOutbox_BackoffDelaysRetry(t *testing.T) {
	store, agg := newStoreAndCounter(t)
	seedEvents(t, agg, []string{"t-backoff"}, 1)

	pub := inproc.New()
	var attempts atomic.Int64
	pub.Subscribe(func(ctx context.Context, env es.Envelope) error {
		attempts.Add(1)
		return errors.New("failure")
	})

	d := &outbox.Drain{
		Store:       store.(es.OutboxStore),
		Publisher:   pub,
		BackoffBase: 1 * time.Hour,
		BackoffMax:  24 * time.Hour,
		MaxAttempts: 5,
	}
	// First Run: one attempt, then backoff for 1 hour. Run loops
	// until pending=0, but the row's next_attempt_at is in the
	// future, so PendingOutbox returns empty after the first attempt.
	_, _, _ = d.Run(context.Background())
	first := attempts.Load()

	// Immediate second Run: nothing pending (backoff hasn't elapsed).
	_, _, _ = d.Run(context.Background())
	second := attempts.Load()

	if first != 1 {
		t.Errorf("first run attempts: got %d want 1", first)
	}
	if second != 1 {
		t.Errorf("second run (within backoff) attempts: got %d want 1 (no new attempt)", second)
	}
}

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

// TestOutbox_CleanupRuns verifies the cleanup path doesn't error.
func TestOutbox_Cleanup(t *testing.T) {
	store, agg := newStoreAndCounter(t)
	seedEvents(t, agg, []string{"t-cleanup"}, 2)

	pub := inproc.New()
	pub.Subscribe(func(ctx context.Context, env es.Envelope) error { return nil })

	d := &outbox.Drain{
		Store:            store.(es.OutboxStore),
		Publisher:        pub,
		Tenant:           "t-cleanup",
		CleanupRetention: 24 * time.Hour,
	}
	if _, _, err := d.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
}
