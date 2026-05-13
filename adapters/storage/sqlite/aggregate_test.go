package sqlite_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	_ "modernc.org/sqlite"

	sqliteadapter "github.com/laenenai/eventstore/adapters/storage/sqlite"
	"github.com/laenenai/eventstore/aggregate"
	"github.com/laenenai/eventstore/es"
	"github.com/laenenai/eventstore/estest"
)

// These tests verify the aggregate.Runtime against a real SQLite store
// using ":memory:" mode. The Counter domain is inlined here as a
// hand-written illustration of what codegen will eventually emit
// (sealed interfaces + variants + codec). For consumer tests of their
// own deciders, the same pattern applies: open sql.Open("sqlite", ":memory:"),
// migrate, wire the runtime.

// ----- Counter domain (hand-written stand-in for codegen output) ----------

type counterState struct {
	Initialized bool
	Count       int64
	Min, Max    int64
}

type counterCmd interface{ isCounterCmd() }
type initCmd struct{ Min, Max, Initial int64 }
type incCmd struct{ By int64 }
type decCmd struct{ By int64 }

func (initCmd) isCounterCmd() {}
func (incCmd) isCounterCmd()  {}
func (decCmd) isCounterCmd()  {}

type counterEvt interface{ isCounterEvt() }
type initialized struct{ Min, Max, Value int64 }
type incremented struct{ By int64 }
type decremented struct{ By int64 }

func (initialized) isCounterEvt() {}
func (incremented) isCounterEvt() {}
func (decremented) isCounterEvt() {}

var (
	errAlreadyInit = errors.New("counter: already initialized")
	errNotInit     = errors.New("counter: not initialized")
	errExceedMax   = errors.New("counter: would exceed max")
	errBelowMin    = errors.New("counter: would fall below min")
	errUnknownCmd  = errors.New("counter: unknown command")
)

var counterDecider = es.Decider[counterState, counterCmd, counterEvt]{
	Initial: func() counterState { return counterState{} },
	Decide: func(s counterState, c counterCmd) ([]counterEvt, []es.ConstraintOp, error) {
		switch cmd := c.(type) {
		case initCmd:
			if s.Initialized {
				return nil, nil, errAlreadyInit
			}
			return []counterEvt{initialized{Min: cmd.Min, Max: cmd.Max, Value: cmd.Initial}}, nil, nil
		case incCmd:
			if !s.Initialized {
				return nil, nil, errNotInit
			}
			if s.Count+cmd.By > s.Max {
				return nil, nil, errExceedMax
			}
			return []counterEvt{incremented{By: cmd.By}}, nil, nil
		case decCmd:
			if !s.Initialized {
				return nil, nil, errNotInit
			}
			if s.Count-cmd.By < s.Min {
				return nil, nil, errBelowMin
			}
			return []counterEvt{decremented{By: cmd.By}}, nil, nil
		default:
			return nil, nil, errUnknownCmd
		}
	},
	Evolve: func(s counterState, e counterEvt) counterState {
		switch evt := e.(type) {
		case initialized:
			s.Initialized = true
			s.Min = evt.Min
			s.Max = evt.Max
			s.Count = evt.Value
		case incremented:
			s.Count += evt.By
		case decremented:
			s.Count -= evt.By
		}
		return s
	},
}

type counterCodec struct{}

func (counterCodec) Encode(e counterEvt) (aggregate.EncodedEvent, error) {
	payload, err := json.Marshal(e)
	if err != nil {
		return aggregate.EncodedEvent{}, err
	}
	var typeURL string
	switch e.(type) {
	case initialized:
		typeURL = "counter.v1.Initialized"
	case incremented:
		typeURL = "counter.v1.Incremented"
	case decremented:
		typeURL = "counter.v1.Decremented"
	default:
		return aggregate.EncodedEvent{}, fmt.Errorf("unknown event type %T", e)
	}
	return aggregate.EncodedEvent{Payload: payload, TypeURL: typeURL, SchemaVersion: 1}, nil
}

func (counterCodec) Decode(typeURL string, _ uint32, payload []byte) (counterEvt, error) {
	switch typeURL {
	case "counter.v1.Initialized":
		var e initialized
		return e, json.Unmarshal(payload, &e)
	case "counter.v1.Incremented":
		var e incremented
		return e, json.Unmarshal(payload, &e)
	case "counter.v1.Decremented":
		var e decremented
		return e, json.Unmarshal(payload, &e)
	}
	return nil, fmt.Errorf("unknown type_url %q", typeURL)
}

// ----- Test fixtures ------------------------------------------------------

// newRuntime opens an in-memory SQLite DB, migrates the framework
// schema, and wires the Counter runtime against it. Returns the
// runtime; cleanup is via t.Cleanup.
func newRuntime(t *testing.T) *aggregate.Runtime[counterState, counterCmd, counterEvt] {
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
	return &aggregate.Runtime[counterState, counterCmd, counterEvt]{
		Store:   a,
		Decider: counterDecider,
		Codec:   counterCodec{},
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

	result, err := rt.Handle(context.Background(), sid, initCmd{Min: 0, Max: 100, Initial: 5})
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

	if _, err := rt.Handle(ctx, sid, initCmd{Min: 0, Max: 100, Initial: 10}); err != nil {
		t.Fatalf("init: %v", err)
	}
	if _, err := rt.Handle(ctx, sid, incCmd{By: 5}); err != nil {
		t.Fatalf("inc: %v", err)
	}
	if _, err := rt.Handle(ctx, sid, incCmd{By: 3}); err != nil {
		t.Fatalf("inc: %v", err)
	}
	if _, err := rt.Handle(ctx, sid, decCmd{By: 2}); err != nil {
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

	if _, err := rt.Handle(ctx, sid, initCmd{Min: 0, Max: 10, Initial: 5}); err != nil {
		t.Fatalf("init: %v", err)
	}

	_, err := rt.Handle(ctx, sid, incCmd{By: 100})
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
	noopDecider := es.Decider[counterState, counterCmd, counterEvt]{
		Initial: counterDecider.Initial,
		Decide: func(_ counterState, _ counterCmd) ([]counterEvt, []es.ConstraintOp, error) {
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
	rt := &aggregate.Runtime[counterState, counterCmd, counterEvt]{
		Store: a, Decider: noopDecider, Codec: counterCodec{},
	}

	sid := estest.MustStream(t, "t-noop", "counter", "1")
	result, err := rt.Handle(context.Background(), sid, incCmd{By: 1})
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
	// Two successive Handles each Load fresh state, so two increments
	// after init produce final count == start + by + by.
	rt := newRuntime(t)
	sid := estest.MustStream(t, "t-reload", "counter", "1")
	ctx := context.Background()

	if _, err := rt.Handle(ctx, sid, initCmd{Min: 0, Max: 100, Initial: 0}); err != nil {
		t.Fatalf("init: %v", err)
	}
	if _, err := rt.Handle(ctx, sid, incCmd{By: 1}); err != nil {
		t.Fatalf("first inc: %v", err)
	}
	if _, err := rt.Handle(ctx, sid, incCmd{By: 1}); err != nil {
		t.Fatalf("second inc: %v", err)
	}

	final, _, _ := rt.Load(ctx, sid)
	if final.Count != 2 {
		t.Errorf("expected count=2, got %d", final.Count)
	}
}
