package sqlite_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"

	"github.com/laenenai/eventstore/es"
	"github.com/laenenai/eventstore/estest"
	counterv1 "github.com/laenenai/eventstore/gen/test/counter/v1"
	"github.com/laenenai/eventstore/state_stream"
)

// recorder is an in-process StatePublisher that captures every
// delivery for assertion. Goroutine-safe.
type recorder struct {
	mu       sync.Mutex
	calls    []es.StateEnvelope
	failNext atomic.Int64 // if >0, next N calls return error and decrement
}

func (r *recorder) PublishState(_ context.Context, env es.StateEnvelope) error {
	if n := r.failNext.Load(); n > 0 {
		r.failNext.Add(-1)
		return errors.New("transient")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	cp := env
	cp.State = append([]byte(nil), env.State...)
	r.calls = append(r.calls, cp)
	return nil
}

func (r *recorder) snapshot() []es.StateEnvelope {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]es.StateEnvelope(nil), r.calls...)
}

// TestStateStream_BackfillThenDeltas exercises:
//   - Cold start: every existing stream's current state is delivered.
//   - Steady state: subsequent Appends bump version; drain delivers
//     the new state for each, advancing positions.
//   - Position monotonicity: a second drain pass with no changes is a
//     no-op.
func TestStateStream_BackfillThenDeltas(t *testing.T) {
	store, rt := newCounterProtoRuntime(t)
	tenant := "t-ss-backfill"
	ctx := es.WithTenant(context.Background(), tenant)

	// Seed three streams with one event each.
	for _, id := range []string{"a", "b", "c"} {
		sid := estest.MustStream(t, tenant, "counter", id)
		if _, err := rt.Handle(ctx, sid, &counterv1.Init{Min: 0, Max: 10, Initial: 1}); err != nil {
			t.Fatalf("Init %s: %v", id, err)
		}
	}

	rec := &recorder{}
	drain := &state_stream.Drain{
		SubscriberName: "search-mirror",
		Tenant:         tenant,
		Store:          store,
		Publisher:      rec,
	}

	// Cold start → backfill of three streams.
	n, err := drain.Run(context.Background())
	if err != nil {
		t.Fatalf("Run cold: %v", err)
	}
	if n != 3 {
		t.Errorf("cold delivered: got %d want 3", n)
	}

	// Steady-state run with no changes → no deliveries.
	n, err = drain.Run(context.Background())
	if err != nil {
		t.Fatalf("Run steady: %v", err)
	}
	if n != 0 {
		t.Errorf("steady-state delivered: got %d want 0", n)
	}

	// Append once to "a" → drain delivers the new version.
	sidA := estest.MustStream(t, tenant, "counter", "a")
	if _, err := rt.Handle(ctx, sidA, &counterv1.Increment{By: 2}); err != nil {
		t.Fatalf("Inc a: %v", err)
	}
	n, err = drain.Run(context.Background())
	if err != nil {
		t.Fatalf("Run delta: %v", err)
	}
	if n != 1 {
		t.Errorf("delta delivered: got %d want 1", n)
	}

	// Confirm the delivered envelope is the latest state of a.
	all := rec.snapshot()
	if len(all) != 4 {
		t.Fatalf("total deliveries: got %d want 4", len(all))
	}
	last := all[len(all)-1]
	var c counterv1.Counter
	_ = protojson.Unmarshal(last.State, &c)
	if c.Count != 3 {
		t.Errorf("latest state count: got %d want 3 (1 init + 2 inc)", c.Count)
	}
}

// TestStateStream_CoalescingOnRetry verifies the central design
// property: if a delivery fails and the stream advances before the
// next drain cycle, only the latest state is delivered — not the
// version that previously failed.
func TestStateStream_CoalescingOnRetry(t *testing.T) {
	store, rt := newCounterProtoRuntime(t)
	tenant := "t-ss-coalesce"
	ctx := es.WithTenant(context.Background(), tenant)
	sid := estest.MustStream(t, tenant, "counter", "1")

	if _, err := rt.Handle(ctx, sid, &counterv1.Init{Min: 0, Max: 100, Initial: 0}); err != nil {
		t.Fatalf("Init: %v", err)
	}

	rec := &recorder{}
	rec.failNext.Store(1) // first delivery fails

	drain := &state_stream.Drain{
		SubscriberName: "flaky",
		Tenant:         tenant,
		Store:          store,
		Publisher:      rec,
	}

	// First batch: delivery fails. Position not advanced. RunOnce
	// (not Run) so the failure doesn't auto-retry within the same call
	// — we want to exercise the across-cycle coalescing path.
	res, err := drain.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce 1: %v", err)
	}
	if res.Delivered != 0 || res.Failed != 1 {
		t.Errorf("first batch: got Delivered=%d Failed=%d want 0/1", res.Delivered, res.Failed)
	}
	if got := rec.snapshot(); len(got) != 0 {
		t.Errorf("after failed delivery: got %d successful, want 0", len(got))
	}

	// Stream advances 3 versions before retry.
	for i := 0; i < 3; i++ {
		if _, err := rt.Handle(ctx, sid, &counterv1.Increment{By: 1}); err != nil {
			t.Fatalf("Inc %d: %v", i, err)
		}
	}

	// Second batch: failNext is now 0, drain succeeds. Delivers exactly
	// one envelope with the LATEST state — coalescing-on-retry replaces
	// the failed version with the current one.
	res, err = drain.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce 2: %v", err)
	}
	if res.Delivered != 1 {
		t.Errorf("second batch delivered: got %d want 1", res.Delivered)
	}
	all := rec.snapshot()
	if len(all) != 1 {
		t.Fatalf("deliveries after retry: got %d want 1 (coalesced)", len(all))
	}
	if all[0].Version != 4 {
		t.Errorf("delivered version: got %d want 4 (latest after 3 inc)", all[0].Version)
	}
	var c counterv1.Counter
	_ = protojson.Unmarshal(all[0].State, &c)
	if c.Count != 3 {
		t.Errorf("delivered count: got %d want 3", c.Count)
	}
}

// TestStateStream_ResetForcesRedelivery verifies the operator
// workflow: after Reset, the next drain delivers the current state
// of every stream again.
func TestStateStream_ResetForcesRedelivery(t *testing.T) {
	store, rt := newCounterProtoRuntime(t)
	tenant := "t-ss-reset"
	ctx := es.WithTenant(context.Background(), tenant)

	sid := estest.MustStream(t, tenant, "counter", "x")
	if _, err := rt.Handle(ctx, sid, &counterv1.Init{Min: 0, Max: 10, Initial: 7}); err != nil {
		t.Fatalf("Init: %v", err)
	}

	rec := &recorder{}
	drain := &state_stream.Drain{
		SubscriberName: "reset-test",
		Tenant:         tenant,
		Store:          store,
		Publisher:      rec,
	}
	if _, err := drain.Run(context.Background()); err != nil {
		t.Fatalf("Run 1: %v", err)
	}
	if len(rec.snapshot()) != 1 {
		t.Fatalf("after first run: got %d want 1 delivery", len(rec.snapshot()))
	}

	admin := store.(es.StateStreamAdmin)
	deleted, err := admin.ResetStateStreamSubscriber(context.Background(), "reset-test", tenant)
	if err != nil {
		t.Fatalf("Reset: %v", err)
	}
	if deleted != 1 {
		t.Errorf("reset deleted: got %d want 1", deleted)
	}

	// Next drain re-delivers the same stream's current state.
	if _, err := drain.Run(context.Background()); err != nil {
		t.Fatalf("Run after reset: %v", err)
	}
	if len(rec.snapshot()) != 2 {
		t.Errorf("after reset+run: got %d total deliveries want 2", len(rec.snapshot()))
	}
}

// TestStateStream_Status reports lag accurately.
func TestStateStream_Status(t *testing.T) {
	store, rt := newCounterProtoRuntime(t)
	tenant := "t-ss-status"
	ctx := es.WithTenant(context.Background(), tenant)

	for _, id := range []string{"a", "b"} {
		sid := estest.MustStream(t, tenant, "counter", id)
		if _, err := rt.Handle(ctx, sid, &counterv1.Init{Min: 0, Max: 10, Initial: 1}); err != nil {
			t.Fatalf("Init %s: %v", id, err)
		}
	}

	admin := store.(es.StateStreamAdmin)
	st, err := admin.StateStreamStatus(context.Background(), "status-test", tenant)
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if st.StreamsBehind != 2 {
		t.Errorf("streams behind cold: got %d want 2", st.StreamsBehind)
	}
}
