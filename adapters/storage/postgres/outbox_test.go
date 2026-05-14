package postgres_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/laenenai/eventstore/es"
	"github.com/laenenai/eventstore/outbox"
	"github.com/laenenai/eventstore/adapters/publisher/inproc"
)

// Integration tests for the outbox drain + inproc publisher against the
// Postgres adapter. Mirrors the SQLite suite — same scenarios, same
// expectations — but exercises the real Postgres engine (head-filter
// NOT EXISTS, advisory locks, etc.). Tenant ids are derived from
// t.Name() so the shared testcontainer can host them all without state
// bleed between tests.

func TestOutbox_PG_DrainPublishesAndMarksAllPending(t *testing.T) {
	agg := newCounterRuntime(t)
	tenant := tnt(t, "drain")
	want := seedStreams(t, agg, tenant, []string{"1", "2"}, 3)

	pub := inproc.New()
	var received atomic.Int64
	pub.Subscribe(func(ctx context.Context, env es.Envelope) error {
		received.Add(1)
		return nil
	})

	d := &outbox.Drain{Store: adapter, Publisher: pub, Tenant: tenant}
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

func TestOutbox_PG_PublisherErrorMarksFailed(t *testing.T) {
	agg := newCounterRuntime(t)
	tenant := tnt(t, "fail")
	seedEvents(t, agg, []string{tenant}, 2)

	pub := inproc.New()
	var attempts atomic.Int64
	pub.Subscribe(func(ctx context.Context, env es.Envelope) error {
		attempts.Add(1)
		return errors.New("subscriber failure")
	})

	d := &outbox.Drain{Store: adapter, Publisher: pub, Tenant: tenant}
	published, _, err := d.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if published != 0 {
		t.Errorf("published: got %d want 0", published)
	}
	// Per-stream halt: only the head row is attempted; row 2 stays
	// behind it.
	if attempts.Load() != 1 {
		t.Errorf("attempts: got %d want 1 (per-stream halt)", attempts.Load())
	}

	pending, err := adapter.CountPending(context.Background(), tenant)
	if err != nil {
		t.Fatalf("CountPending: %v", err)
	}
	if pending != 2 {
		t.Errorf("pending: got %d want 2", pending)
	}
}

func TestOutbox_PG_TenantScopedDrain(t *testing.T) {
	agg := newCounterRuntime(t)
	tX := tnt(t, "x")
	tY := tnt(t, "y")
	seedEvents(t, agg, []string{tX, tY}, 4)

	pub := inproc.New()
	var seen []string
	pub.Subscribe(func(ctx context.Context, env es.Envelope) error {
		seen = append(seen, env.TenantID)
		return nil
	})

	d := &outbox.Drain{Store: adapter, Publisher: pub, Tenant: tX}
	published, _, err := d.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if published != 4 {
		t.Errorf("published for tenant X: got %d want 4", published)
	}
	for _, tn := range seen {
		if tn != tX {
			t.Errorf("wrong tenant: %q", tn)
		}
	}
	pending, _ := adapter.CountPending(context.Background(), tY)
	if pending != 4 {
		t.Errorf("pending after tenant drain: got %d want 4 (tY rows)", pending)
	}
}

// TestOutbox_PG_PerStreamHalt — failing stream halts but other streams
// continue publishing in the same run. Two streams under one tenant so
// the drain can be tenant-scoped (avoids picking up leftover rows from
// the shared testcontainer).
func TestOutbox_PG_PerStreamHalt(t *testing.T) {
	agg := newCounterRuntime(t)
	tenant := tnt(t, "ps")
	seedStreams(t, agg, tenant, []string{"poison", "good"}, 3)

	pub := inproc.New()
	var poisonSeen, otherSeen atomic.Int64
	pub.Subscribe(func(ctx context.Context, env es.Envelope) error {
		if env.StreamID.Canonical() == "counter:poison" {
			poisonSeen.Add(1)
			return errors.New("poison")
		}
		otherSeen.Add(1)
		return nil
	})

	d := &outbox.Drain{Store: adapter, Publisher: pub, Tenant: tenant}
	if _, _, err := d.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if otherSeen.Load() != 3 {
		t.Errorf("good stream: got %d want 3", otherSeen.Load())
	}
	if poisonSeen.Load() < 1 {
		t.Errorf("poison stream: got %d want >= 1", poisonSeen.Load())
	}
}

// TestOutbox_PG_DLQQuarantineDefault — after MaxAttempts the row hits
// DLQ and the stream is quarantined; later runs publish nothing for it.
func TestOutbox_PG_DLQQuarantineDefault(t *testing.T) {
	agg := newCounterRuntime(t)
	tenant := tnt(t, "q")
	seedEvents(t, agg, []string{tenant}, 3)

	pub := inproc.New()
	pub.Subscribe(func(ctx context.Context, env es.Envelope) error {
		return errors.New("persistent")
	})

	d := &outbox.Drain{Store: adapter, Publisher: pub, Tenant: tenant, MaxAttempts: 2}
	_, _, _ = d.Run(context.Background())
	_, _, _ = d.Run(context.Background())

	dlqCount, err := adapter.CountDLQ(context.Background(), tenant, 2)
	if err != nil {
		t.Fatalf("CountDLQ: %v", err)
	}
	if dlqCount != 1 {
		t.Errorf("CountDLQ: got %d want 1", dlqCount)
	}

	pub2 := inproc.New()
	var attempts atomic.Int64
	pub2.Subscribe(func(ctx context.Context, env es.Envelope) error {
		attempts.Add(1)
		return nil
	})
	d.Publisher = pub2
	pubd, _, _ := d.Run(context.Background())
	if pubd != 0 || attempts.Load() != 0 {
		t.Errorf("quarantined stream should not publish: got %d published, %d subscribed",
			pubd, attempts.Load())
	}
}

// TestOutbox_PG_DLQAutoResume — with AutoResumeAfterDLQ=true, the
// stream skips past the DLQ'd head and proceeds with the rest.
func TestOutbox_PG_DLQAutoResume(t *testing.T) {
	agg := newCounterRuntime(t)
	tenant := tnt(t, "resume")
	seedEvents(t, agg, []string{tenant}, 3)

	pub := inproc.New()
	var calls atomic.Int64
	pub.Subscribe(func(ctx context.Context, env es.Envelope) error {
		n := calls.Add(1)
		if n <= 2 {
			return errors.New("poison")
		}
		return nil
	})

	d := &outbox.Drain{
		Store: adapter, Publisher: pub, Tenant: tenant,
		MaxAttempts: 2, AutoResumeAfterDLQ: true,
	}
	_, _, _ = d.Run(context.Background())
	_, _, _ = d.Run(context.Background())

	dlqCount, _ := adapter.CountDLQ(context.Background(), tenant, 2)
	if dlqCount != 1 {
		t.Errorf("CountDLQ: got %d want 1", dlqCount)
	}
	pendingCount, _ := adapter.CountPending(context.Background(), tenant)
	if pendingCount != 1 {
		t.Errorf("CountPending: got %d want 1 (just the DLQ'd row)", pendingCount)
	}
}

// TestOutbox_PG_DLQReplay — operator action ReplayDLQ resets attempts
// and the row is published on the next run.
func TestOutbox_PG_DLQReplay(t *testing.T) {
	agg := newCounterRuntime(t)
	tenant := tnt(t, "replay")
	seedEvents(t, agg, []string{tenant}, 1)

	pub := inproc.New()
	var calls atomic.Int64
	pub.Subscribe(func(ctx context.Context, env es.Envelope) error {
		n := calls.Add(1)
		if n <= 2 {
			return errors.New("transient")
		}
		return nil
	})

	d := &outbox.Drain{Store: adapter, Publisher: pub, Tenant: tenant, MaxAttempts: 2}
	_, _, _ = d.Run(context.Background())
	_, _, _ = d.Run(context.Background())

	dlqRows, err := adapter.ListDLQ(context.Background(), tenant, 2, 0, 10)
	if err != nil {
		t.Fatalf("ListDLQ: %v", err)
	}
	if len(dlqRows) != 1 {
		t.Fatalf("expected 1 DLQ row, got %d", len(dlqRows))
	}

	if err := adapter.ReplayDLQ(context.Background(), tenant, dlqRows[0].GlobalPosition); err != nil {
		t.Fatalf("ReplayDLQ: %v", err)
	}

	pubd, _, err := d.Run(context.Background())
	if err != nil {
		t.Fatalf("Run after replay: %v", err)
	}
	if pubd != 1 {
		t.Errorf("after replay published: got %d want 1", pubd)
	}
}

// TestOutbox_PG_DLQAbandon — operator marks the row as published
// without delivering it; counts go back to zero.
func TestOutbox_PG_DLQAbandon(t *testing.T) {
	agg := newCounterRuntime(t)
	tenant := tnt(t, "abandon")
	seedEvents(t, agg, []string{tenant}, 1)

	pub := inproc.New()
	pub.Subscribe(func(ctx context.Context, env es.Envelope) error {
		return errors.New("permanent")
	})

	d := &outbox.Drain{Store: adapter, Publisher: pub, Tenant: tenant, MaxAttempts: 2}
	_, _, _ = d.Run(context.Background())
	_, _, _ = d.Run(context.Background())

	dlqRows, _ := adapter.ListDLQ(context.Background(), tenant, 2, 0, 10)
	if len(dlqRows) != 1 {
		t.Fatalf("expected 1 DLQ row, got %d", len(dlqRows))
	}

	if err := adapter.AbandonDLQ(context.Background(), tenant, dlqRows[0].GlobalPosition); err != nil {
		t.Fatalf("AbandonDLQ: %v", err)
	}

	dlqCount, _ := adapter.CountDLQ(context.Background(), tenant, 2)
	if dlqCount != 0 {
		t.Errorf("CountDLQ after abandon: got %d want 0", dlqCount)
	}
	pendingCount, _ := adapter.CountPending(context.Background(), tenant)
	if pendingCount != 0 {
		t.Errorf("CountPending after abandon: got %d want 0", pendingCount)
	}
}

func TestOutbox_PG_DLQOnCallback(t *testing.T) {
	agg := newCounterRuntime(t)
	tenant := tnt(t, "cb")
	seedEvents(t, agg, []string{tenant}, 1)

	pub := inproc.New()
	pub.Subscribe(func(ctx context.Context, env es.Envelope) error {
		return errors.New("failure")
	})

	var dlqFired atomic.Int64
	d := &outbox.Drain{
		Store: adapter, Publisher: pub, Tenant: tenant, MaxAttempts: 2,
		OnDLQ: func(row es.OutboxRow) { dlqFired.Add(1) },
	}
	_, _, _ = d.Run(context.Background())
	_, _, _ = d.Run(context.Background())

	if dlqFired.Load() != 1 {
		t.Errorf("OnDLQ fired: got %d want 1", dlqFired.Load())
	}
}

// TestOutbox_PG_BackoffDelaysRetry — once next_attempt_at is in the
// future, an immediate second Run sees nothing eligible.
func TestOutbox_PG_BackoffDelaysRetry(t *testing.T) {
	agg := newCounterRuntime(t)
	tenant := tnt(t, "backoff")
	seedEvents(t, agg, []string{tenant}, 1)

	pub := inproc.New()
	var attempts atomic.Int64
	pub.Subscribe(func(ctx context.Context, env es.Envelope) error {
		attempts.Add(1)
		return errors.New("failure")
	})

	d := &outbox.Drain{
		Store: adapter, Publisher: pub, Tenant: tenant,
		BackoffBase: 1 * time.Hour,
		BackoffMax:  24 * time.Hour,
		MaxAttempts: 5,
	}
	_, _, _ = d.Run(context.Background())
	first := attempts.Load()
	_, _, _ = d.Run(context.Background())
	second := attempts.Load()

	if first != 1 {
		t.Errorf("first run attempts: got %d want 1", first)
	}
	if second != 1 {
		t.Errorf("second run (within backoff) attempts: got %d want 1", second)
	}
}

// TestOutbox_PG_DrainLockerExclusive — Postgres-specific: the
// pg_try_advisory_lock backed DrainLocker prevents a second concurrent
// drain from running while the first holds the lock.
func TestOutbox_PG_DrainLockerExclusive(t *testing.T) {
	agg := newCounterRuntime(t)
	tenant := tnt(t, "lock")
	seedEvents(t, agg, []string{tenant}, 3)

	// First drain holds the lock via a publisher that blocks until
	// released. The second drain attempts to acquire and should
	// be a no-op (returns 0 published, no error).
	holdRelease := make(chan struct{})
	holdEntered := make(chan struct{})
	pubBlocker := inproc.New()
	var once sync.Once
	pubBlocker.Subscribe(func(ctx context.Context, env es.Envelope) error {
		once.Do(func() { close(holdEntered) })
		<-holdRelease
		return nil
	})

	drainA := &outbox.Drain{
		Store: adapter, Publisher: pubBlocker,
		Tenant: tenant, LockKey: tenant + "-lock",
	}

	go func() {
		_, _, _ = drainA.Run(context.Background())
	}()

	<-holdEntered

	// Second drain tries to acquire — should bail out (locked).
	pubB := inproc.New()
	var sawB atomic.Int64
	pubB.Subscribe(func(ctx context.Context, env es.Envelope) error {
		sawB.Add(1)
		return nil
	})
	drainB := &outbox.Drain{
		Store: adapter, Publisher: pubB,
		Tenant: tenant, LockKey: tenant + "-lock",
	}
	pubdB, _, err := drainB.Run(context.Background())
	if err != nil {
		t.Fatalf("drainB Run: %v", err)
	}
	if pubdB != 0 || sawB.Load() != 0 {
		t.Errorf("drainB should be locked out: got %d published, %d subscribed",
			pubdB, sawB.Load())
	}

	close(holdRelease)
}

func TestOutbox_PG_DrainValidation(t *testing.T) {
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

func TestOutbox_PG_Cleanup(t *testing.T) {
	agg := newCounterRuntime(t)
	tenant := tnt(t, "cleanup")
	seedEvents(t, agg, []string{tenant}, 2)

	pub := inproc.New()
	pub.Subscribe(func(ctx context.Context, env es.Envelope) error { return nil })

	d := &outbox.Drain{
		Store: adapter, Publisher: pub, Tenant: tenant,
		CleanupRetention: 24 * time.Hour,
	}
	if _, _, err := d.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
}
