package cmdworkflow_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	sqliteadapter "github.com/laenenai/eventstore/adapters/storage/sqlite"
	"github.com/laenenai/eventstore/aggregate"
	"github.com/laenenai/eventstore/cmdworkflow"
	"github.com/laenenai/eventstore/adapters/cmdworkflow/inproc"
	"github.com/laenenai/eventstore/es"
	"github.com/laenenai/eventstore/estest"
	counterv1 "github.com/laenenai/eventstore/gen/test/counter/v1"
)

// counterDecider — a minimal proto-state Counter decider re-used
// across these tests.
var counterDecider = es.Decider[*counterv1.Counter, counterv1.Command, counterv1.Event]{
	Initial: func() *counterv1.Counter { return &counterv1.Counter{} },
	Decide: func(s *counterv1.Counter, c counterv1.Command) ([]counterv1.Event, []es.ConstraintOp, error) {
		switch cmd := c.(type) {
		case *counterv1.Init:
			if s.Initialized {
				return nil, nil, errors.New("counter: already initialized")
			}
			return []counterv1.Event{
				&counterv1.Initialized{Min: cmd.Min, Max: cmd.Max, Value: cmd.Initial},
			}, nil, nil
		case *counterv1.Increment:
			if !s.Initialized {
				return nil, nil, errors.New("counter: not initialized")
			}
			if s.Count+cmd.By > s.Max {
				return nil, nil, errors.New("counter: exceeds max")
			}
			return []counterv1.Event{&counterv1.Incremented{By: cmd.By}}, nil, nil
		case *counterv1.Decrement:
			if !s.Initialized {
				return nil, nil, errors.New("counter: not initialized")
			}
			return []counterv1.Event{&counterv1.Decremented{By: cmd.By}}, nil, nil
		}
		return nil, nil, errors.New("counter: unknown command")
	},
	Evolve: func(s *counterv1.Counter, e counterv1.Event) *counterv1.Counter {
		out := &counterv1.Counter{
			Initialized: s.Initialized,
			Count:       s.Count,
			Min:         s.Min,
			Max:         s.Max,
		}
		switch evt := e.(type) {
		case *counterv1.Initialized:
			out.Initialized = true
			out.Min = evt.Min
			out.Max = evt.Max
			out.Count = evt.Value
		case *counterv1.Incremented:
			out.Count += evt.By
		case *counterv1.Decremented:
			out.Count -= evt.By
		}
		return out
	},
}

func setup(t *testing.T) (es.Store, *aggregate.Runtime[*counterv1.Counter, counterv1.Command, counterv1.Event]) {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	a := sqliteadapter.New(db)
	if err := a.Migrate(context.Background()); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	rt := &aggregate.Runtime[*counterv1.Counter, counterv1.Command, counterv1.Event]{
		Store:      a,
		Decider:    counterDecider,
		Codec:      counterv1.EventCodec{},
		StateCodec: aggregate.ProtoStateCodec[*counterv1.Counter]{},
	}
	return a, rt
}

// memoryDLQ captures DLQ rows for assertion.
type memoryDLQ struct{ rows []cmdworkflow.SubscriberDLQRow }

func (m *memoryDLQ) InsertSubscriberDLQ(_ context.Context, row cmdworkflow.SubscriberDLQRow) error {
	m.rows = append(m.rows, row)
	return nil
}

// TestFilter_Matches: type-url, stream-glob, tenants, custom — each
// gate composes (AND).
func TestFilter_Matches(t *testing.T) {
	env := es.Envelope{
		TypeURL:  "test.counter.v1.Incremented",
		StreamID: es.StreamID{Tenant: "acme", Type: "counter", ID: "1"},
		TenantID: "acme",
	}
	for _, tc := range []struct {
		name string
		f    cmdworkflow.EventFilter
		want bool
	}{
		{"empty filter", cmdworkflow.EventFilter{}, true},
		{"matching typeurl", cmdworkflow.EventFilter{TypeURLs: []string{"test.counter.v1.Incremented"}}, true},
		{"wrong typeurl", cmdworkflow.EventFilter{TypeURLs: []string{"foo"}}, false},
		{"matching glob", cmdworkflow.EventFilter{StreamGlob: "counter:*"}, true},
		{"non-matching glob", cmdworkflow.EventFilter{StreamGlob: "invoice:*"}, false},
		{"matching tenant", cmdworkflow.EventFilter{Tenants: []string{"acme"}}, true},
		{"wrong tenant", cmdworkflow.EventFilter{Tenants: []string{"other"}}, false},
		{"all pass", cmdworkflow.EventFilter{
			TypeURLs: []string{"test.counter.v1.Incremented"},
			Tenants:  []string{"acme"},
			Custom:   func(es.Envelope) bool { return true },
		}, true},
		{"custom rejects", cmdworkflow.EventFilter{
			Custom: func(es.Envelope) bool { return false },
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.f.Matches(env); got != tc.want {
				t.Errorf("Matches: got %v want %v", got, tc.want)
			}
		})
	}
}

// TestHandleCmd_SyncSubscriberDelivered: the happy path — Sync
// subscriber sees every event matched by its filter, HandleCmd
// returns the post-Decide state.
func TestHandleCmd_SyncSubscriberDelivered(t *testing.T) {
	store, rt := setup(t)
	wf := inproc.New()
	bus := cmdworkflow.New[*counterv1.Counter, counterv1.Command](rt, store, wf)

	var delivered atomic.Int32
	bus.Register(cmdworkflow.Subscriber[counterv1.Command]{
		Name:        "sync-listener",
		Mode:        cmdworkflow.Sync,
		MaxRetries:  0,
		OnExhausted: cmdworkflow.DLQ,
		Handle: func(_ context.Context, env es.Envelope) error {
			delivered.Add(1)
			return nil
		},
	})

	tenant := "t-sync"
	ctx := es.WithTenant(context.Background(), tenant)
	sid := estest.MustStream(t, tenant, "counter", "1")

	state, err := bus.HandleCmd(ctx, sid, &counterv1.Init{Min: 0, Max: 100, Initial: 5})
	if err != nil {
		t.Fatalf("HandleCmd Init: %v", err)
	}
	if state == nil || !state.Initialized || state.Count != 5 {
		t.Fatalf("post-Init state: got %+v", state)
	}
	if delivered.Load() != 1 {
		t.Errorf("delivered after Init: got %d want 1", delivered.Load())
	}

	state, err = bus.HandleCmd(ctx, sid, &counterv1.Increment{By: 3})
	if err != nil {
		t.Fatalf("HandleCmd Increment: %v", err)
	}
	if state.Count != 8 {
		t.Errorf("post-Increment count: got %d want 8", state.Count)
	}
	if delivered.Load() != 2 {
		t.Errorf("delivered after Increment: got %d want 2", delivered.Load())
	}
}

// TestHandleCmd_FilterSkipsUnmatched: subscribers whose filter
// doesn't match are not invoked. No journal entries either, but at
// the unit-test level we observe the call count.
func TestHandleCmd_FilterSkipsUnmatched(t *testing.T) {
	store, rt := setup(t)
	wf := inproc.New()
	bus := cmdworkflow.New[*counterv1.Counter, counterv1.Command](rt, store, wf)

	var initSeen, incSeen atomic.Int32
	bus.Register(cmdworkflow.Subscriber[counterv1.Command]{
		Name: "inits-only",
		Filter: cmdworkflow.EventFilter{
			TypeURLs: []string{"test.counter.v1.Initialized"},
		},
		Mode:        cmdworkflow.Sync,
		OnExhausted: cmdworkflow.DLQ,
		Handle: func(_ context.Context, _ es.Envelope) error {
			initSeen.Add(1)
			return nil
		},
	})
	bus.Register(cmdworkflow.Subscriber[counterv1.Command]{
		Name: "incs-only",
		Filter: cmdworkflow.EventFilter{
			TypeURLs: []string{"test.counter.v1.Incremented"},
		},
		Mode:        cmdworkflow.Sync,
		OnExhausted: cmdworkflow.DLQ,
		Handle: func(_ context.Context, _ es.Envelope) error {
			incSeen.Add(1)
			return nil
		},
	})

	tenant := "t-filter"
	ctx := es.WithTenant(context.Background(), tenant)
	sid := estest.MustStream(t, tenant, "counter", "1")
	if _, err := bus.HandleCmd(ctx, sid, &counterv1.Init{Min: 0, Max: 100, Initial: 0}); err != nil {
		t.Fatalf("Init: %v", err)
	}
	if _, err := bus.HandleCmd(ctx, sid, &counterv1.Increment{By: 1}); err != nil {
		t.Fatalf("Increment: %v", err)
	}
	if initSeen.Load() != 1 {
		t.Errorf("initsOnly: got %d want 1", initSeen.Load())
	}
	if incSeen.Load() != 1 {
		t.Errorf("incsOnly: got %d want 1", incSeen.Load())
	}
}

// TestHandleCmd_RetryThenSuccess: a transient Handle error retried
// within MaxRetries succeeds; no DLQ row.
func TestHandleCmd_RetryThenSuccess(t *testing.T) {
	store, rt := setup(t)
	wf := inproc.New()
	dlq := &memoryDLQ{}
	bus := cmdworkflow.New[*counterv1.Counter, counterv1.Command](rt, store, wf).WithDLQ(dlq)

	var attempts atomic.Int32
	bus.Register(cmdworkflow.Subscriber[counterv1.Command]{
		Name:        "flaky-sub",
		Mode:        cmdworkflow.Sync,
		MaxRetries:  3,
		OnExhausted: cmdworkflow.DLQ,
		Handle: func(_ context.Context, _ es.Envelope) error {
			if attempts.Add(1) < 3 {
				return errors.New("transient")
			}
			return nil
		},
	})

	tenant := "t-retry"
	ctx := es.WithTenant(context.Background(), tenant)
	sid := estest.MustStream(t, tenant, "counter", "1")
	if _, err := bus.HandleCmd(ctx, sid, &counterv1.Init{Min: 0, Max: 100, Initial: 0}); err != nil {
		t.Fatalf("HandleCmd: %v", err)
	}
	if attempts.Load() != 3 {
		t.Errorf("attempts: got %d want 3", attempts.Load())
	}
	if len(dlq.rows) != 0 {
		t.Errorf("dlq rows: got %d want 0", len(dlq.rows))
	}
}

// TestHandleCmd_ExhaustedDLQ: retries exhausted → DLQ row written.
func TestHandleCmd_ExhaustedDLQ(t *testing.T) {
	store, rt := setup(t)
	wf := inproc.New()
	dlq := &memoryDLQ{}
	bus := cmdworkflow.New[*counterv1.Counter, counterv1.Command](rt, store, wf).WithDLQ(dlq)

	bus.Register(cmdworkflow.Subscriber[counterv1.Command]{
		Name:        "always-fails",
		Mode:        cmdworkflow.Sync,
		MaxRetries:  2,
		OnExhausted: cmdworkflow.DLQ,
		Handle: func(_ context.Context, _ es.Envelope) error {
			return errors.New("permanent")
		},
	})

	tenant := "t-dlq"
	ctx := es.WithTenant(context.Background(), tenant)
	sid := estest.MustStream(t, tenant, "counter", "1")
	if _, err := bus.HandleCmd(ctx, sid, &counterv1.Init{Min: 0, Max: 100, Initial: 0}); err != nil {
		t.Fatalf("HandleCmd: %v", err)
	}
	if len(dlq.rows) != 1 {
		t.Fatalf("dlq rows: got %d want 1", len(dlq.rows))
	}
	row := dlq.rows[0]
	if row.SubscriberName != "always-fails" || row.LastError == "" || row.Attempts != 3 {
		t.Errorf("dlq row: %+v", row)
	}
}

// TestHandleCmd_ExhaustedDrop: Drop policy yields no DLQ row and no
// error propagation. The command still succeeds.
func TestHandleCmd_ExhaustedDrop(t *testing.T) {
	store, rt := setup(t)
	wf := inproc.New()
	dlq := &memoryDLQ{}
	bus := cmdworkflow.New[*counterv1.Counter, counterv1.Command](rt, store, wf).WithDLQ(dlq)

	bus.Register(cmdworkflow.Subscriber[counterv1.Command]{
		Name:        "dropper",
		Mode:        cmdworkflow.Sync,
		MaxRetries:  1,
		OnExhausted: cmdworkflow.Drop,
		Handle: func(_ context.Context, _ es.Envelope) error {
			return errors.New("nope")
		},
	})

	tenant := "t-drop"
	ctx := es.WithTenant(context.Background(), tenant)
	sid := estest.MustStream(t, tenant, "counter", "1")
	if _, err := bus.HandleCmd(ctx, sid, &counterv1.Init{Min: 0, Max: 100, Initial: 0}); err != nil {
		t.Fatalf("HandleCmd: %v", err)
	}
	if len(dlq.rows) != 0 {
		t.Errorf("dlq rows: got %d want 0", len(dlq.rows))
	}
}

// TestHandleCmd_Compensate: when an exhausted Sync subscriber's
// OnExhausted is Compensate, the workflow appends a compensating
// command and the resulting state reflects both events.
func TestHandleCmd_Compensate(t *testing.T) {
	store, rt := setup(t)
	wf := inproc.New()
	bus := cmdworkflow.New[*counterv1.Counter, counterv1.Command](rt, store, wf)

	// Saga step: matches Incremented events. Always fails. On
	// exhaustion, emits a Decrement to roll the count back.
	bus.Register(cmdworkflow.Subscriber[counterv1.Command]{
		Name: "saga-step",
		Filter: cmdworkflow.EventFilter{
			TypeURLs: []string{"test.counter.v1.Incremented"},
		},
		Mode:        cmdworkflow.Sync,
		MaxRetries:  0,
		OnExhausted: cmdworkflow.Compensate,
		Handle: func(_ context.Context, _ es.Envelope) error {
			return errors.New("downstream rejected")
		},
		Compensate: func(_ context.Context, env es.Envelope) (counterv1.Command, error) {
			// Roll back the +5 increment that triggered us.
			return &counterv1.Decrement{By: 5}, nil
		},
	})

	tenant := "t-comp"
	ctx := es.WithTenant(context.Background(), tenant)
	sid := estest.MustStream(t, tenant, "counter", "saga")
	if _, err := bus.HandleCmd(ctx, sid, &counterv1.Init{Min: -100, Max: 100, Initial: 0}); err != nil {
		t.Fatalf("Init: %v", err)
	}

	state, err := bus.HandleCmd(ctx, sid, &counterv1.Increment{By: 5})
	if err != nil {
		t.Fatalf("HandleCmd Increment (with comp): %v", err)
	}
	// Expected: +5 then -5 → 0. Original event is durable; compensation
	// is also durable. Audit trail shows both.
	if state.Count != 0 {
		t.Errorf("post-compensation count: got %d want 0", state.Count)
	}
}

// TestHandleCmd_AsyncEventuallyDelivered: Async subscribers run as
// spawned workflows; HandleCmd returns before they complete. Wait()
// blocks for them.
func TestHandleCmd_AsyncEventuallyDelivered(t *testing.T) {
	store, rt := setup(t)
	wf := inproc.New()
	bus := cmdworkflow.New[*counterv1.Counter, counterv1.Command](rt, store, wf)

	var asyncDone atomic.Int32
	bus.Register(cmdworkflow.Subscriber[counterv1.Command]{
		Name:        "async-sub",
		Mode:        cmdworkflow.Async,
		MaxRetries:  3,
		OnExhausted: cmdworkflow.Drop,
		Handle: func(_ context.Context, _ es.Envelope) error {
			time.Sleep(20 * time.Millisecond)
			asyncDone.Add(1)
			return nil
		},
	})

	tenant := "t-async"
	ctx := es.WithTenant(context.Background(), tenant)
	sid := estest.MustStream(t, tenant, "counter", "1")
	if _, err := bus.HandleCmd(ctx, sid, &counterv1.Init{Min: 0, Max: 100, Initial: 0}); err != nil {
		t.Fatalf("HandleCmd: %v", err)
	}
	// Async subscriber hasn't completed yet (probably).
	wf.Wait() // blocks for spawned workflows
	if asyncDone.Load() != 1 {
		t.Errorf("async delivered: got %d want 1", asyncDone.Load())
	}
}

// TestHandleCmd_IdempotencyKeyDeterministic: WithIdempotencyKey
// produces stable command_ids across calls with the same key.
// Subscribers observe the same command_id, enabling ADR 0015 dedup.
//
// This test does NOT verify cross-call dedup at the framework level —
// that's the workflow runtime's job (Restate / DBOS). With the inproc
// adapter, repeated calls execute repeatedly (see option docs).
func TestHandleCmd_IdempotencyKeyDeterministic(t *testing.T) {
	store, rt := setup(t)
	wf := inproc.New()
	bus := cmdworkflow.New[*counterv1.Counter, counterv1.Command](rt, store, wf)

	var seenCmdIDs []string
	bus.Register(cmdworkflow.Subscriber[counterv1.Command]{
		Name:        "cmdid-recorder",
		Mode:        cmdworkflow.Sync,
		OnExhausted: cmdworkflow.DLQ,
		Handle: func(_ context.Context, env es.Envelope) error {
			seenCmdIDs = append(seenCmdIDs, env.CommandID.String())
			return nil
		},
	})

	tenant := "t-idemp"
	ctx := es.WithTenant(context.Background(), tenant)
	sid := estest.MustStream(t, tenant, "counter", "1")

	if _, err := bus.HandleCmd(ctx, sid, &counterv1.Init{Min: 0, Max: 100, Initial: 0}); err != nil {
		t.Fatalf("Init: %v", err)
	}
	// First Increment with key — record observed command_id.
	if _, err := bus.HandleCmd(ctx, sid, &counterv1.Increment{By: 1},
		cmdworkflow.WithIdempotencyKey("req-abc")); err != nil {
		t.Fatalf("first keyed call: %v", err)
	}
	firstKeyedCmdID := seenCmdIDs[len(seenCmdIDs)-1]

	// Different stream, same key — same command_id derived.
	sid2 := estest.MustStream(t, tenant, "counter", "2")
	if _, err := bus.HandleCmd(ctx, sid2, &counterv1.Init{Min: 0, Max: 100, Initial: 0}); err != nil {
		t.Fatalf("Init sid2: %v", err)
	}
	if _, err := bus.HandleCmd(ctx, sid2, &counterv1.Increment{By: 1},
		cmdworkflow.WithIdempotencyKey("req-abc")); err != nil {
		t.Fatalf("second keyed call (different stream): %v", err)
	}
	secondKeyedCmdID := seenCmdIDs[len(seenCmdIDs)-1]

	if firstKeyedCmdID != secondKeyedCmdID {
		t.Errorf("deterministic command_id: first=%s second=%s — must match for same key",
			firstKeyedCmdID, secondKeyedCmdID)
	}

	// Different key → different command_id.
	if _, err := bus.HandleCmd(ctx, sid, &counterv1.Increment{By: 1},
		cmdworkflow.WithIdempotencyKey("req-xyz")); err != nil {
		t.Fatalf("different-key call: %v", err)
	}
	differentKeyCmdID := seenCmdIDs[len(seenCmdIDs)-1]
	if differentKeyCmdID == firstKeyedCmdID {
		t.Errorf("different keys produced same command_id: %s — namespace collision", differentKeyCmdID)
	}
}

// TestHandleCmd_SyncSubscribersRunInParallel verifies that multiple
// Sync subscribers fan out concurrently via RunAsync — total time
// should be ~max(per-subscriber), not sum-of(per-subscriber).
func TestHandleCmd_SyncSubscribersRunInParallel(t *testing.T) {
	store, rt := setup(t)
	wf := inproc.New()
	bus := cmdworkflow.New[*counterv1.Counter, counterv1.Command](rt, store, wf)

	const sleep = 80 * time.Millisecond
	const numSubs = 4

	for i := 0; i < numSubs; i++ {
		name := fmt.Sprintf("slow-sub-%d", i)
		bus.Register(cmdworkflow.Subscriber[counterv1.Command]{
			Name:        name,
			Mode:        cmdworkflow.Sync,
			OnExhausted: cmdworkflow.DLQ,
			Handle: func(_ context.Context, _ es.Envelope) error {
				time.Sleep(sleep)
				return nil
			},
		})
	}

	tenant := "t-par"
	ctx := es.WithTenant(context.Background(), tenant)
	sid := estest.MustStream(t, tenant, "counter", "1")

	start := time.Now()
	if _, err := bus.HandleCmd(ctx, sid, &counterv1.Init{Min: 0, Max: 100, Initial: 0}); err != nil {
		t.Fatalf("HandleCmd: %v", err)
	}
	elapsed := time.Since(start)

	// Serial would be 4 * 80ms = 320ms. Parallel should be ~80ms
	// plus small overhead. Allow up to 2x single-subscriber time for
	// goroutine scheduling slack; well below serial.
	maxParallel := sleep * 2
	if elapsed > maxParallel {
		t.Errorf("HandleCmd with %d parallel Sync subs took %v; want < %v (serial would be %v)",
			numSubs, elapsed, maxParallel, sleep*numSubs)
	}
}

// TestHandleCmd_AttemptTimeout: a subscriber that hangs hits the
// per-attempt deadline, which counts as a failure → retry.
func TestHandleCmd_AttemptTimeout(t *testing.T) {
	store, rt := setup(t)
	wf := inproc.New()
	dlq := &memoryDLQ{}
	bus := cmdworkflow.New[*counterv1.Counter, counterv1.Command](rt, store, wf).WithDLQ(dlq)

	var attempts atomic.Int32
	bus.Register(cmdworkflow.Subscriber[counterv1.Command]{
		Name:           "hanger",
		Mode:           cmdworkflow.Sync,
		MaxRetries:     1,
		OnExhausted:    cmdworkflow.DLQ,
		AttemptTimeout: 30 * time.Millisecond,
		Handle: func(ctx context.Context, _ es.Envelope) error {
			attempts.Add(1)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Second):
				return nil
			}
		},
	})

	tenant := "t-timeout"
	ctx := es.WithTenant(context.Background(), tenant)
	sid := estest.MustStream(t, tenant, "counter", "1")
	if _, err := bus.HandleCmd(ctx, sid, &counterv1.Init{Min: 0, Max: 100, Initial: 0}); err != nil {
		t.Fatalf("HandleCmd: %v", err)
	}
	if attempts.Load() != 2 {
		t.Errorf("attempts: got %d want 2", attempts.Load())
	}
	if len(dlq.rows) != 1 {
		t.Errorf("dlq rows: got %d want 1", len(dlq.rows))
	}
}
