package aggregate_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/laenenai/eventstore/aggregate"
	"github.com/laenenai/eventstore/es"
	"github.com/laenenai/eventstore/estest"
)

// Tests use an inline toy "Counter" domain to exercise the aggregate
// runtime end-to-end without depending on a real storage adapter.

// ----- Counter domain ------------------------------------------------------

type counterState struct {
	Initialized bool
	Count       int64
	Min, Max    int64
}

// Sealed sum-type interfaces. Real domains use codegen (ADR 0004) to
// emit these; we write them by hand here since codegen is Phase 3.

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

// counterCodec is a hand-written codec for the Counter domain. Real
// domains get this from codegen.
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
	return aggregate.EncodedEvent{
		Payload:       payload,
		TypeURL:       typeURL,
		SchemaVersion: 1,
	}, nil
}

func (counterCodec) Decode(typeURL string, schemaVersion uint32, payload []byte) (counterEvt, error) {
	switch typeURL {
	case "counter.v1.Initialized":
		var e initialized
		if err := json.Unmarshal(payload, &e); err != nil {
			return nil, err
		}
		return e, nil
	case "counter.v1.Incremented":
		var e incremented
		if err := json.Unmarshal(payload, &e); err != nil {
			return nil, err
		}
		return e, nil
	case "counter.v1.Decremented":
		var e decremented
		if err := json.Unmarshal(payload, &e); err != nil {
			return nil, err
		}
		return e, nil
	}
	return nil, fmt.Errorf("unknown type_url %q", typeURL)
}

// ----- Tests ---------------------------------------------------------------

func newCounterRuntime() *aggregate.Runtime[counterState, counterCmd, counterEvt] {
	return &aggregate.Runtime[counterState, counterCmd, counterEvt]{
		Store:   estest.NewInMemoryStore(),
		Decider: counterDecider,
		Codec:   counterCodec{},
	}
}

func TestRuntime_LoadEmptyStream(t *testing.T) {
	rt := newCounterRuntime()
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

func TestRuntime_HandleSingleCommand(t *testing.T) {
	rt := newCounterRuntime()
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

func TestRuntime_HandleMultipleCommands(t *testing.T) {
	rt := newCounterRuntime()
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
	if state.Count != 16 { // 10 + 5 + 3 - 2
		t.Errorf("Count: got %d want 16", state.Count)
	}
	if version != 4 {
		t.Errorf("version: got %d want 4", version)
	}
}

func TestRuntime_DecideError(t *testing.T) {
	rt := newCounterRuntime()
	sid := estest.MustStream(t, "t-err", "counter", "1")
	ctx := context.Background()

	// Init first.
	if _, err := rt.Handle(ctx, sid, initCmd{Min: 0, Max: 10, Initial: 5}); err != nil {
		t.Fatalf("init: %v", err)
	}

	// Try to exceed max.
	_, err := rt.Handle(ctx, sid, incCmd{By: 100})
	if !errors.Is(err, errExceedMax) {
		t.Fatalf("expected errExceedMax, got %v", err)
	}

	// Stream must remain at version 1 (the init).
	v, err := rt.Store.CurrentStreamVersion(ctx, sid)
	if err != nil {
		t.Fatalf("CurrentStreamVersion: %v", err)
	}
	if v != 1 {
		t.Errorf("version after failed decide: got %d want 1", v)
	}
}

func TestRuntime_NoOpCommand(t *testing.T) {
	// A decider that returns no events for a particular command (a
	// "no-op") should not produce an append.
	noopDecider := es.Decider[counterState, counterCmd, counterEvt]{
		Initial: counterDecider.Initial,
		Decide: func(s counterState, c counterCmd) ([]counterEvt, []es.ConstraintOp, error) {
			return nil, nil, nil
		},
		Evolve: counterDecider.Evolve,
	}
	rt := &aggregate.Runtime[counterState, counterCmd, counterEvt]{
		Store:   estest.NewInMemoryStore(),
		Decider: noopDecider,
		Codec:   counterCodec{},
	}

	sid := estest.MustStream(t, "t-noop", "counter", "1")
	result, err := rt.Handle(context.Background(), sid, incCmd{By: 1})
	if err != nil {
		t.Fatalf("noop Handle: %v", err)
	}
	if result.StartVersion != 0 || result.EndVersion != 0 {
		t.Errorf("expected zero-value AppendResult, got %+v", result)
	}

	v, _ := rt.Store.CurrentStreamVersion(context.Background(), sid)
	if v != 0 {
		t.Errorf("expected stream version 0, got %d", v)
	}
}

func TestRuntime_OptimisticConcurrency(t *testing.T) {
	rt := newCounterRuntime()
	sid := estest.MustStream(t, "t-oc", "counter", "1")
	ctx := context.Background()

	if _, err := rt.Handle(ctx, sid, initCmd{Min: 0, Max: 100, Initial: 0}); err != nil {
		t.Fatalf("init: %v", err)
	}

	// Simulate two concurrent handlers: both load at version 1, then
	// both try to append at version 2. The second should retry by
	// loading again and proceeding from the new version.
	state1, v1, _ := rt.Load(ctx, sid)
	state2, v2, _ := rt.Load(ctx, sid)
	if v1 != v2 || v1 != 1 {
		t.Fatalf("expected both loads at v=1, got %d / %d", v1, v2)
	}

	// First call commits.
	if _, err := rt.Handle(ctx, sid, incCmd{By: 1}); err != nil {
		t.Fatalf("first inc: %v", err)
	}

	// Second call uses stale state; the runtime's Load will fetch the
	// current state inside Handle, so it actually re-loads and
	// proceeds correctly.
	_, err := rt.Handle(ctx, sid, incCmd{By: 1})
	if err != nil {
		t.Fatalf("second inc: %v", err)
	}

	// Both increments should have applied.
	final, _, _ := rt.Load(ctx, sid)
	if final.Count != 2 {
		t.Errorf("expected count=2 after two increments, got %d (state1=%+v state2=%+v)",
			final.Count, state1, state2)
	}
}
