package sqlite_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	_ "modernc.org/sqlite"

	sqliteadapter "github.com/laenenai/eventstore/adapters/storage/sqlite"
	"github.com/laenenai/eventstore/aggregate"
	"github.com/laenenai/eventstore/es"
	"github.com/laenenai/eventstore/estest"
	counterv1 "github.com/laenenai/eventstore/gen/test/counter/v1"
)

// These tests verify aggregate.Runtime against a real SQLite store via
// :memory:, using the Counter domain whose Go types are now produced
// by the codegen plugin (protoc-gen-es-go). The decider and state
// definitions remain hand-written — codegen of those is a later step.

// Counter state — a plain Go struct. State doesn't have to be a proto
// type; the decider is generic over (S, C, E) and only the wire shapes
// (C, E) need to come from codegen.
type counterState struct {
	Initialized bool
	Count       int64
	Min, Max    int64
}

var (
	errAlreadyInit = errors.New("counter: already initialized")
	errNotInit     = errors.New("counter: not initialized")
	errExceedMax   = errors.New("counter: would exceed max")
	errBelowMin    = errors.New("counter: would fall below min")
	errUnknownCmd  = errors.New("counter: unknown command")
)

// counterDecider is the only hand-written piece per aggregate now —
// the Command and Event sum types and their Codecs come from codegen.
var counterDecider = es.Decider[counterState, counterv1.Command, counterv1.Event]{
	Initial: func() counterState { return counterState{} },

	Decide: func(s counterState, c counterv1.Command) ([]counterv1.Event, []es.ConstraintOp, error) {
		switch cmd := c.(type) {
		case *counterv1.Init:
			if s.Initialized {
				return nil, nil, errAlreadyInit
			}
			return []counterv1.Event{
				&counterv1.Initialized{Min: cmd.Min, Max: cmd.Max, Value: cmd.Initial},
			}, nil, nil

		case *counterv1.Increment:
			if !s.Initialized {
				return nil, nil, errNotInit
			}
			if s.Count+cmd.By > s.Max {
				return nil, nil, errExceedMax
			}
			return []counterv1.Event{&counterv1.Incremented{By: cmd.By}}, nil, nil

		case *counterv1.Decrement:
			if !s.Initialized {
				return nil, nil, errNotInit
			}
			if s.Count-cmd.By < s.Min {
				return nil, nil, errBelowMin
			}
			return []counterv1.Event{&counterv1.Decremented{By: cmd.By}}, nil, nil

		default:
			return nil, nil, errUnknownCmd
		}
	},

	Evolve: func(s counterState, e counterv1.Event) counterState {
		switch evt := e.(type) {
		case *counterv1.Initialized:
			s.Initialized = true
			s.Min = evt.Min
			s.Max = evt.Max
			s.Count = evt.Value
		case *counterv1.Incremented:
			s.Count += evt.By
		case *counterv1.Decremented:
			s.Count -= evt.By
		}
		return s
	},
}

// newRuntime opens an in-memory SQLite DB, migrates the framework
// schema, and wires the Counter runtime using the codegen-emitted
// EventCodec. The CommandCodec is not used by the runtime itself —
// commands are passed directly via Handle as Go values, never marshaled
// to bytes for the runtime's own purposes — but the framework emits
// both because consumers will need the CommandCodec when wiring sagas
// or bus dispatch.
func newRuntime(t *testing.T) *aggregate.Runtime[counterState, counterv1.Command, counterv1.Event] {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("sql.Open(:memory:): %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	a := sqliteadapter.New(db)
	if err := a.Migrate(context.Background()); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	return &aggregate.Runtime[counterState, counterv1.Command, counterv1.Event]{
		Store:   a,
		Decider: counterDecider,
		Codec:   counterv1.EventCodec{},
	}
}

// ----- Aggregate runtime tests --------------------------------------------

func TestAggregate_LoadEmptyStream(t *testing.T) {
	rt := newRuntime(t)
	sid := estest.MustStream(t, "t-empty", "counter", "1")

	state, version, err := rt.Load(context.Background(), sid)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if version != 0 {
		t.Errorf("version: got %d want 0", version)
	}
	if state.Initialized {
		t.Errorf("expected zero state, got %+v", state)
	}
}

func TestAggregate_HandleSingleCommand(t *testing.T) {
	rt := newRuntime(t)
	sid := estest.MustStream(t, "t-single", "counter", "1")

	result, err := rt.Handle(context.Background(), sid, &counterv1.Init{Min: 0, Max: 100, Initial: 5})
	if err != nil {
		t.Fatalf("Handle init: %v", err)
	}
	if result.StartVersion != 1 || result.EndVersion != 1 {
		t.Errorf("versions: got %d..%d want 1..1", result.StartVersion, result.EndVersion)
	}

	state, version, err := rt.Load(context.Background(), sid)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !state.Initialized || state.Count != 5 {
		t.Errorf("state: got %+v want initialized=true count=5", state)
	}
	if version != 1 {
		t.Errorf("version: got %d want 1", version)
	}
}

func TestAggregate_HandleMultipleCommands(t *testing.T) {
	rt := newRuntime(t)
	sid := estest.MustStream(t, "t-multi", "counter", "1")
	ctx := context.Background()

	if _, err := rt.Handle(ctx, sid, &counterv1.Init{Min: 0, Max: 100, Initial: 10}); err != nil {
		t.Fatalf("init: %v", err)
	}
	if _, err := rt.Handle(ctx, sid, &counterv1.Increment{By: 5}); err != nil {
		t.Fatalf("inc: %v", err)
	}
	if _, err := rt.Handle(ctx, sid, &counterv1.Increment{By: 3}); err != nil {
		t.Fatalf("inc: %v", err)
	}
	if _, err := rt.Handle(ctx, sid, &counterv1.Decrement{By: 2}); err != nil {
		t.Fatalf("dec: %v", err)
	}

	state, version, err := rt.Load(ctx, sid)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if state.Count != 16 {
		t.Errorf("Count: got %d want 16", state.Count)
	}
	if version != 4 {
		t.Errorf("version: got %d want 4", version)
	}
}

func TestAggregate_DecideError(t *testing.T) {
	rt := newRuntime(t)
	sid := estest.MustStream(t, "t-err", "counter", "1")
	ctx := context.Background()

	if _, err := rt.Handle(ctx, sid, &counterv1.Init{Min: 0, Max: 10, Initial: 5}); err != nil {
		t.Fatalf("init: %v", err)
	}

	_, err := rt.Handle(ctx, sid, &counterv1.Increment{By: 100})
	if !errors.Is(err, errExceedMax) {
		t.Fatalf("expected errExceedMax, got %v", err)
	}

	v, err := rt.Store.CurrentStreamVersion(ctx, sid)
	if err != nil {
		t.Fatalf("CurrentStreamVersion: %v", err)
	}
	if v != 1 {
		t.Errorf("version after failed decide: got %d want 1", v)
	}
}

func TestAggregate_NoOpCommand(t *testing.T) {
	noopDecider := es.Decider[counterState, counterv1.Command, counterv1.Event]{
		Initial: counterDecider.Initial,
		Decide: func(_ counterState, _ counterv1.Command) ([]counterv1.Event, []es.ConstraintOp, error) {
			return nil, nil, nil
		},
		Evolve: counterDecider.Evolve,
	}
	db, _ := sql.Open("sqlite", ":memory:")
	t.Cleanup(func() { _ = db.Close() })
	a := sqliteadapter.New(db)
	if err := a.Migrate(context.Background()); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	rt := &aggregate.Runtime[counterState, counterv1.Command, counterv1.Event]{
		Store: a, Decider: noopDecider, Codec: counterv1.EventCodec{},
	}

	sid := estest.MustStream(t, "t-noop", "counter", "1")
	result, err := rt.Handle(context.Background(), sid, &counterv1.Increment{By: 1})
	if err != nil {
		t.Fatalf("noop Handle: %v", err)
	}
	if result.StartVersion != 0 || result.EndVersion != 0 {
		t.Errorf("expected zero AppendResult, got %+v", result)
	}

	v, _ := rt.Store.CurrentStreamVersion(context.Background(), sid)
	if v != 0 {
		t.Errorf("expected stream version 0, got %d", v)
	}
}

func TestAggregate_HandleReloads(t *testing.T) {
	rt := newRuntime(t)
	sid := estest.MustStream(t, "t-reload", "counter", "1")
	ctx := context.Background()

	if _, err := rt.Handle(ctx, sid, &counterv1.Init{Min: 0, Max: 100, Initial: 0}); err != nil {
		t.Fatalf("init: %v", err)
	}
	if _, err := rt.Handle(ctx, sid, &counterv1.Increment{By: 1}); err != nil {
		t.Fatalf("first inc: %v", err)
	}
	if _, err := rt.Handle(ctx, sid, &counterv1.Increment{By: 1}); err != nil {
		t.Fatalf("second inc: %v", err)
	}

	final, _, _ := rt.Load(ctx, sid)
	if final.Count != 2 {
		t.Errorf("expected count=2, got %d", final.Count)
	}
}
