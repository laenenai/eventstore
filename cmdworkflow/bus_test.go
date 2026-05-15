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

	"github.com/laenenai/eventstore/adapters/cmdworkflow/inproc"
	sqliteadapter "github.com/laenenai/eventstore/adapters/storage/sqlite"
	"github.com/laenenai/eventstore/aggregate"
	"github.com/laenenai/eventstore/cmdworkflow"
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

// newBus is the canonical fixture: SQLite + inproc + counter runtime
// + the framework's per-batch Workflow.
func newBus(t *testing.T) (
	*cmdworkflow.Workflow[*counterv1.Counter, counterv1.Command, counterv1.Event],
	*inproc.Runtime,
	*aggregate.Runtime[*counterv1.Counter, counterv1.Command, counterv1.Event],
	es.Store,
) {
	store, rt := setup(t)
	wf := inproc.New()
	bus := cmdworkflow.New[*counterv1.Counter, counterv1.Command, counterv1.Event](
		rt, store, wf, counterv1.EventCodec{},
	)
	return bus, wf, rt, store
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

// TestHandleCmd_BatchSemantics: the headline new invariant — a Sync
// subscriber receives ONE Handle call per command, with the whole
// matched envelope batch, the post-Decide state, and the typed
// events. Two commands → two Handle calls; the batch + state shape
// is correct on each.
func TestHandleCmd_BatchSemantics(t *testing.T) {
	bus, _, _, _ := newBus(t)

	var calls atomic.Int32
	var seenStateCounts []int64
	var seenEventCounts []int
	bus.Register(cmdworkflow.Subscriber[*counterv1.Counter, counterv1.Command, counterv1.Event]{
		Name:        "batch-watcher",
		Mode:        cmdworkflow.Sync,
		OnExhausted: cmdworkflow.DLQ,
		Handle: func(_ context.Context, envs []es.Envelope, state *counterv1.Counter, events []counterv1.Event) error {
			calls.Add(1)
			seenStateCounts = append(seenStateCounts, state.Count)
			seenEventCounts = append(seenEventCounts, len(events))
			if len(envs) != len(events) {
				return fmt.Errorf("envs/events length mismatch: %d vs %d", len(envs), len(events))
			}
			return nil
		},
	})

	tenant := "t-batch"
	ctx := es.WithTenant(context.Background(), tenant)
	sid := estest.MustStream(t, tenant, "counter", "1")

	if _, err := bus.HandleCmd(ctx, sid, &counterv1.Init{Min: 0, Max: 100, Initial: 5}); err != nil {
		t.Fatalf("Init: %v", err)
	}
	if _, err := bus.HandleCmd(ctx, sid, &counterv1.Increment{By: 3}); err != nil {
		t.Fatalf("Increment: %v", err)
	}

	// Two commands → two Handle invocations, NOT two-per-event (each
	// of these commands emits exactly one event, but the contract is
	// "one call per command-batch" not "one call per event").
	if calls.Load() != 2 {
		t.Errorf("Handle calls: got %d want 2", calls.Load())
	}
	// Post-Decide state: Init → 5; Increment → 8.
	if len(seenStateCounts) != 2 || seenStateCounts[0] != 5 || seenStateCounts[1] != 8 {
		t.Errorf("state per batch: got %v want [5 8]", seenStateCounts)
	}
	// Each command produced exactly one matched event.
	if len(seenEventCounts) != 2 || seenEventCounts[0] != 1 || seenEventCounts[1] != 1 {
		t.Errorf("events per batch: got %v want [1 1]", seenEventCounts)
	}
}

// TestHandleCmd_FilterNarrowsBatch: a subscriber filtered to
// Incremented sees ONLY the Increment command's batch (and not Init).
// The filter applies per-event; events that don't match are dropped
// from the batch. A subscriber with no matching events is skipped
// entirely (Handle not called).
func TestHandleCmd_FilterNarrowsBatch(t *testing.T) {
	bus, _, _, _ := newBus(t)

	var initOnlyCalls, incOnlyCalls atomic.Int32
	bus.Register(cmdworkflow.Subscriber[*counterv1.Counter, counterv1.Command, counterv1.Event]{
		Name: "inits-only",
		Filter: cmdworkflow.EventFilter{
			TypeURLs: []string{"test.counter.v1.Initialized"},
		},
		Mode:        cmdworkflow.Sync,
		OnExhausted: cmdworkflow.DLQ,
		Handle: func(_ context.Context, _ []es.Envelope, _ *counterv1.Counter, _ []counterv1.Event) error {
			initOnlyCalls.Add(1)
			return nil
		},
	})
	bus.Register(cmdworkflow.Subscriber[*counterv1.Counter, counterv1.Command, counterv1.Event]{
		Name: "incs-only",
		Filter: cmdworkflow.EventFilter{
			TypeURLs: []string{"test.counter.v1.Incremented"},
		},
		Mode:        cmdworkflow.Sync,
		OnExhausted: cmdworkflow.DLQ,
		Handle: func(_ context.Context, envs []es.Envelope, _ *counterv1.Counter, events []counterv1.Event) error {
			incOnlyCalls.Add(1)
			if len(envs) != 1 || len(events) != 1 {
				return fmt.Errorf("expected 1 envelope, got %d", len(envs))
			}
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
	if initOnlyCalls.Load() != 1 {
		t.Errorf("inits-only Handle calls: got %d want 1", initOnlyCalls.Load())
	}
	if incOnlyCalls.Load() != 1 {
		t.Errorf("incs-only Handle calls: got %d want 1", incOnlyCalls.Load())
	}
}

// TestHandleCmd_StateMatchesRunnerLoad: the state passed to Handle
// must equal what runner.Load returns after the command commits.
func TestHandleCmd_StateMatchesRunnerLoad(t *testing.T) {
	bus, _, rt, _ := newBus(t)

	var seenState *counterv1.Counter
	bus.Register(cmdworkflow.Subscriber[*counterv1.Counter, counterv1.Command, counterv1.Event]{
		Name:        "state-watcher",
		Mode:        cmdworkflow.Sync,
		OnExhausted: cmdworkflow.DLQ,
		Handle: func(_ context.Context, _ []es.Envelope, state *counterv1.Counter, _ []counterv1.Event) error {
			seenState = state
			return nil
		},
	})

	tenant := "t-state"
	ctx := es.WithTenant(context.Background(), tenant)
	sid := estest.MustStream(t, tenant, "counter", "1")
	if _, err := bus.HandleCmd(ctx, sid, &counterv1.Init{Min: 0, Max: 100, Initial: 7}); err != nil {
		t.Fatalf("Init: %v", err)
	}
	loaded, _, err := rt.Load(ctx, sid)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if seenState == nil || seenState.Count != loaded.Count || seenState.Max != loaded.Max {
		t.Errorf("state seen by Handle (%+v) != runner.Load (%+v)", seenState, loaded)
	}
}

// TestHandleCmd_RetryThenSuccess: a transient Handle error retried
// within MaxRetries succeeds; no DLQ row. The retry budget is per-
// BATCH — one Handle call counts as one attempt.
func TestHandleCmd_RetryThenSuccess(t *testing.T) {
	bus, _, _, _ := newBus(t)
	dlq := &memoryDLQ{}
	bus.WithDLQ(dlq)

	var attempts atomic.Int32
	bus.Register(cmdworkflow.Subscriber[*counterv1.Counter, counterv1.Command, counterv1.Event]{
		Name:        "flaky-sub",
		Mode:        cmdworkflow.Sync,
		MaxRetries:  3,
		OnExhausted: cmdworkflow.DLQ,
		Handle: func(_ context.Context, _ []es.Envelope, _ *counterv1.Counter, _ []counterv1.Event) error {
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

// TestHandleCmd_ExhaustedDLQ: retries exhausted → ONE DLQ row written
// containing the whole batch's event ids and type URLs.
func TestHandleCmd_ExhaustedDLQ(t *testing.T) {
	bus, _, _, _ := newBus(t)
	dlq := &memoryDLQ{}
	bus.WithDLQ(dlq)

	bus.Register(cmdworkflow.Subscriber[*counterv1.Counter, counterv1.Command, counterv1.Event]{
		Name:        "always-fails",
		Mode:        cmdworkflow.Sync,
		MaxRetries:  2,
		OnExhausted: cmdworkflow.DLQ,
		Handle: func(_ context.Context, _ []es.Envelope, _ *counterv1.Counter, _ []counterv1.Event) error {
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
	if row.SubscriberName != "always-fails" {
		t.Errorf("row subscriber: %s", row.SubscriberName)
	}
	if row.Attempts != 3 {
		t.Errorf("row attempts: got %d want 3", row.Attempts)
	}
	if len(row.EventIDs) != 1 || len(row.TypeURLs) != 1 {
		t.Errorf("batch fields: event_ids=%v type_urls=%v", row.EventIDs, row.TypeURLs)
	}
	if row.TypeURLs[0] != "test.counter.v1.Initialized" {
		t.Errorf("type_urls[0]: got %s", row.TypeURLs[0])
	}
}

// TestHandleCmd_ExhaustedDrop: Drop policy yields no DLQ row and no
// error propagation. The command still succeeds.
func TestHandleCmd_ExhaustedDrop(t *testing.T) {
	bus, _, _, _ := newBus(t)
	dlq := &memoryDLQ{}
	bus.WithDLQ(dlq)

	bus.Register(cmdworkflow.Subscriber[*counterv1.Counter, counterv1.Command, counterv1.Event]{
		Name:        "dropper",
		Mode:        cmdworkflow.Sync,
		MaxRetries:  1,
		OnExhausted: cmdworkflow.Drop,
		Handle: func(_ context.Context, _ []es.Envelope, _ *counterv1.Counter, _ []counterv1.Event) error {
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
// command. Compensate receives the same batch (envs + state + events)
// as Handle.
func TestHandleCmd_Compensate(t *testing.T) {
	bus, _, _, _ := newBus(t)

	var compEnvs []es.Envelope
	var compState *counterv1.Counter
	var compEvents []counterv1.Event
	bus.Register(cmdworkflow.Subscriber[*counterv1.Counter, counterv1.Command, counterv1.Event]{
		Name: "saga-step",
		Filter: cmdworkflow.EventFilter{
			TypeURLs: []string{"test.counter.v1.Incremented"},
		},
		Mode:        cmdworkflow.Sync,
		MaxRetries:  0,
		OnExhausted: cmdworkflow.Compensate,
		Handle: func(_ context.Context, _ []es.Envelope, _ *counterv1.Counter, _ []counterv1.Event) error {
			return errors.New("downstream rejected")
		},
		Compensate: func(_ context.Context, envs []es.Envelope, state *counterv1.Counter, events []counterv1.Event) (counterv1.Command, error) {
			compEnvs = envs
			compState = state
			compEvents = events
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
	// Expected: +5 then -5 → 0.
	if state.Count != 0 {
		t.Errorf("post-compensation count: got %d want 0", state.Count)
	}

	// Compensate saw the batch shape.
	if len(compEnvs) != 1 || len(compEvents) != 1 {
		t.Errorf("Compensate batch: envs=%d events=%d", len(compEnvs), len(compEvents))
	}
	// Compensate saw post-Decide state (count = 5 after Increment, before compensation).
	if compState == nil || compState.Count != 5 {
		t.Errorf("Compensate state: got %+v", compState)
	}
}

// TestHandleCmd_AsyncEventuallyDelivered: Async subscribers run as
// spawned workflows; HandleCmd returns before they complete.
func TestHandleCmd_AsyncEventuallyDelivered(t *testing.T) {
	bus, wfRt, _, _ := newBus(t)

	var asyncCalls atomic.Int32
	bus.Register(cmdworkflow.Subscriber[*counterv1.Counter, counterv1.Command, counterv1.Event]{
		Name:        "async-sub",
		Mode:        cmdworkflow.Async,
		MaxRetries:  3,
		OnExhausted: cmdworkflow.Drop,
		Handle: func(_ context.Context, _ []es.Envelope, _ *counterv1.Counter, _ []counterv1.Event) error {
			time.Sleep(20 * time.Millisecond)
			asyncCalls.Add(1)
			return nil
		},
	})

	tenant := "t-async"
	ctx := es.WithTenant(context.Background(), tenant)
	sid := estest.MustStream(t, tenant, "counter", "1")
	if _, err := bus.HandleCmd(ctx, sid, &counterv1.Init{Min: 0, Max: 100, Initial: 0}); err != nil {
		t.Fatalf("HandleCmd: %v", err)
	}
	wfRt.Wait() // blocks for spawned workflows
	if asyncCalls.Load() != 1 {
		t.Errorf("async delivered: got %d want 1", asyncCalls.Load())
	}
}

// TestHandleCmd_IdempotencyKeyDeterministic: WithIdempotencyKey
// produces stable command_ids across calls with the same key. The
// new batch-shaped subscriber observes them on the envelopes within
// the batch.
func TestHandleCmd_IdempotencyKeyDeterministic(t *testing.T) {
	bus, _, _, _ := newBus(t)

	var seenCmdIDs []string
	bus.Register(cmdworkflow.Subscriber[*counterv1.Counter, counterv1.Command, counterv1.Event]{
		Name:        "cmdid-recorder",
		Mode:        cmdworkflow.Sync,
		OnExhausted: cmdworkflow.DLQ,
		Handle: func(_ context.Context, envs []es.Envelope, _ *counterv1.Counter, _ []counterv1.Event) error {
			for _, env := range envs {
				seenCmdIDs = append(seenCmdIDs, env.CommandID.String())
			}
			return nil
		},
	})

	tenant := "t-idemp"
	ctx := es.WithTenant(context.Background(), tenant)
	sid := estest.MustStream(t, tenant, "counter", "1")

	if _, err := bus.HandleCmd(ctx, sid, &counterv1.Init{Min: 0, Max: 100, Initial: 0}); err != nil {
		t.Fatalf("Init: %v", err)
	}
	if _, err := bus.HandleCmd(ctx, sid, &counterv1.Increment{By: 1},
		cmdworkflow.WithIdempotencyKey("req-abc")); err != nil {
		t.Fatalf("first keyed call: %v", err)
	}
	firstKeyedCmdID := seenCmdIDs[len(seenCmdIDs)-1]

	sid2 := estest.MustStream(t, tenant, "counter", "2")
	if _, err := bus.HandleCmd(ctx, sid2, &counterv1.Init{Min: 0, Max: 100, Initial: 0}); err != nil {
		t.Fatalf("Init sid2: %v", err)
	}
	if _, err := bus.HandleCmd(ctx, sid2, &counterv1.Increment{By: 1},
		cmdworkflow.WithIdempotencyKey("req-abc")); err != nil {
		t.Fatalf("second keyed call: %v", err)
	}
	secondKeyedCmdID := seenCmdIDs[len(seenCmdIDs)-1]

	if firstKeyedCmdID != secondKeyedCmdID {
		t.Errorf("deterministic command_id: first=%s second=%s", firstKeyedCmdID, secondKeyedCmdID)
	}

	if _, err := bus.HandleCmd(ctx, sid, &counterv1.Increment{By: 1},
		cmdworkflow.WithIdempotencyKey("req-xyz")); err != nil {
		t.Fatalf("different-key call: %v", err)
	}
	differentKeyCmdID := seenCmdIDs[len(seenCmdIDs)-1]
	if differentKeyCmdID == firstKeyedCmdID {
		t.Errorf("different keys produced same command_id: %s", differentKeyCmdID)
	}
}

// TestHandleCmd_SyncSubscribersRunInParallel: multiple Sync subscribers
// for the same command fan out concurrently via RunAsync — total time
// should be ~max(per-subscriber), not sum-of(per-subscriber).
func TestHandleCmd_SyncSubscribersRunInParallel(t *testing.T) {
	bus, _, _, _ := newBus(t)

	const sleep = 80 * time.Millisecond
	const numSubs = 4

	for i := 0; i < numSubs; i++ {
		name := fmt.Sprintf("slow-sub-%d", i)
		bus.Register(cmdworkflow.Subscriber[*counterv1.Counter, counterv1.Command, counterv1.Event]{
			Name:        name,
			Mode:        cmdworkflow.Sync,
			OnExhausted: cmdworkflow.DLQ,
			Handle: func(_ context.Context, _ []es.Envelope, _ *counterv1.Counter, _ []counterv1.Event) error {
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

	maxParallel := sleep * 2
	if elapsed > maxParallel {
		t.Errorf("HandleCmd with %d parallel Sync subs took %v; want < %v (serial would be %v)",
			numSubs, elapsed, maxParallel, sleep*numSubs)
	}
}

// TestHandleCmd_AttemptTimeout: a subscriber that hangs hits the
// per-attempt deadline, which counts as a failure → retry.
func TestHandleCmd_AttemptTimeout(t *testing.T) {
	bus, _, _, _ := newBus(t)
	dlq := &memoryDLQ{}
	bus.WithDLQ(dlq)

	var attempts atomic.Int32
	bus.Register(cmdworkflow.Subscriber[*counterv1.Counter, counterv1.Command, counterv1.Event]{
		Name:           "hanger",
		Mode:           cmdworkflow.Sync,
		MaxRetries:     1,
		OnExhausted:    cmdworkflow.DLQ,
		AttemptTimeout: 30 * time.Millisecond,
		Handle: func(ctx context.Context, _ []es.Envelope, _ *counterv1.Counter, _ []counterv1.Event) error {
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

// TestHandleCmd_NoMatchingEventsSkipsSubscriber: a subscriber whose
// Filter rejects every envelope is skipped entirely — no Handle call,
// no journal entry. Verified by Handle never running for an event
// type the filter doesn't accept.
func TestHandleCmd_NoMatchingEventsSkipsSubscriber(t *testing.T) {
	bus, _, _, _ := newBus(t)

	var calls atomic.Int32
	bus.Register(cmdworkflow.Subscriber[*counterv1.Counter, counterv1.Command, counterv1.Event]{
		Name: "decs-only",
		Filter: cmdworkflow.EventFilter{
			TypeURLs: []string{"test.counter.v1.Decremented"},
		},
		Mode:        cmdworkflow.Sync,
		OnExhausted: cmdworkflow.DLQ,
		Handle: func(_ context.Context, _ []es.Envelope, _ *counterv1.Counter, _ []counterv1.Event) error {
			calls.Add(1)
			return nil
		},
	})

	tenant := "t-skip"
	ctx := es.WithTenant(context.Background(), tenant)
	sid := estest.MustStream(t, tenant, "counter", "1")
	if _, err := bus.HandleCmd(ctx, sid, &counterv1.Init{Min: 0, Max: 100, Initial: 0}); err != nil {
		t.Fatalf("Init: %v", err)
	}
	if _, err := bus.HandleCmd(ctx, sid, &counterv1.Increment{By: 1}); err != nil {
		t.Fatalf("Increment: %v", err)
	}
	if calls.Load() != 0 {
		t.Errorf("skipped subscriber Handle calls: got %d want 0", calls.Load())
	}
}
