package postgres_test

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"

	"github.com/laenenai/eventstore/aggregate"
	"github.com/laenenai/eventstore/es"
	"github.com/laenenai/eventstore/estest"
	counterv1 "github.com/laenenai/eventstore/gen/test/counter/v1"
)

// Tier-1 state_cache tests against the Postgres adapter, mirroring
// the SQLite suite. Uses the proto-typed Counter state with
// ProtoStateCodec to exercise the JSONB column end-to-end.

var counterProtoDecider = es.Decider[*counterv1.Counter, counterv1.Command, counterv1.Event]{
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
			if s.Count-cmd.By < s.Min {
				return nil, nil, errors.New("counter: below min")
			}
			return []counterv1.Event{&counterv1.Decremented{By: cmd.By}}, nil, nil
		default:
			return nil, nil, errors.New("counter: unknown command")
		}
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

func newCounterProtoRuntime() *aggregate.Runtime[*counterv1.Counter, counterv1.Command, counterv1.Event] {
	return &aggregate.Runtime[*counterv1.Counter, counterv1.Command, counterv1.Event]{
		Store:      adapter,
		Decider:    counterProtoDecider,
		Codec:      counterv1.EventCodec{},
		StateCodec: aggregate.ProtoStateCodec[*counterv1.Counter]{},
	}
}

func TestStateCache_PG_WrittenInTxWithAppend(t *testing.T) {
	rt := newCounterProtoRuntime()
	tenant := tnt(t, "cache")
	ctx := es.WithTenant(context.Background(), tenant)
	sid := estest.MustStream(t, tenant, "counter", "1")

	if _, err := rt.Handle(ctx, sid, &counterv1.Init{Min: 0, Max: 100, Initial: 5}); err != nil {
		t.Fatalf("init: %v", err)
	}

	row, err := adapter.GetState(context.Background(), tenant, sid.Canonical())
	if err != nil {
		t.Fatalf("GetState after init: %v", err)
	}
	if row.Version != 1 {
		t.Errorf("version: got %d want 1", row.Version)
	}

	var c counterv1.Counter
	if err := protojson.Unmarshal(row.State, &c); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !c.Initialized || c.Count != 5 || c.Min != 0 || c.Max != 100 {
		t.Errorf("state: got %+v", &c)
	}

	if _, err := rt.Handle(ctx, sid, &counterv1.Increment{By: 3}); err != nil {
		t.Fatalf("inc: %v", err)
	}
	row, _ = adapter.GetState(context.Background(), tenant, sid.Canonical())
	if row.Version != 2 {
		t.Errorf("version after inc: got %d want 2", row.Version)
	}
	_ = protojson.Unmarshal(row.State, &c)
	if c.Count != 8 {
		t.Errorf("count after inc: got %d want 8", c.Count)
	}
}

func TestStateCache_PG_DisabledWhenCodecUnset(t *testing.T) {
	tenant := tnt(t, "off")
	ctx := es.WithTenant(context.Background(), tenant)
	rt := &aggregate.Runtime[*counterv1.Counter, counterv1.Command, counterv1.Event]{
		Store:   adapter,
		Decider: counterProtoDecider,
		Codec:   counterv1.EventCodec{},
		// StateCodec unset
	}
	sid := estest.MustStream(t, tenant, "counter", "1")
	if _, err := rt.Handle(ctx, sid, &counterv1.Init{Min: 0, Max: 10, Initial: 1}); err != nil {
		t.Fatalf("init: %v", err)
	}
	if _, err := adapter.GetState(context.Background(), tenant, sid.Canonical()); !errors.Is(err, es.ErrStateNotFound) {
		t.Errorf("GetState with no codec: got err=%v want ErrStateNotFound", err)
	}
}

func TestStateCache_PG_ListStates(t *testing.T) {
	rt := newCounterProtoRuntime()
	tenant := tnt(t, "list")
	ctx := es.WithTenant(context.Background(), tenant)
	for _, id := range []string{"1", "2", "3", "4", "5"} {
		sid := estest.MustStream(t, tenant, "counter", id)
		if _, err := rt.Handle(ctx, sid, &counterv1.Init{Min: 0, Max: 100, Initial: int64(len(id))}); err != nil {
			t.Fatalf("init %s: %v", id, err)
		}
	}

	page1, err := adapter.ListStates(context.Background(), tenant, "test.counter.v1.Counter", "", 2)
	if err != nil {
		t.Fatalf("ListStates page 1: %v", err)
	}
	if len(page1) != 2 {
		t.Errorf("page1 size: got %d want 2", len(page1))
	}
	page2, err := adapter.ListStates(context.Background(), tenant, "test.counter.v1.Counter", page1[len(page1)-1].StreamID, 100)
	if err != nil {
		t.Fatalf("ListStates page 2: %v", err)
	}
	if len(page2) != 3 {
		t.Errorf("page2 size: got %d want 3", len(page2))
	}
}

func TestStateCache_PG_Rebuild(t *testing.T) {
	rt := newCounterProtoRuntime()
	tenant := tnt(t, "rebuild")
	ctx := es.WithTenant(context.Background(), tenant)
	sid := estest.MustStream(t, tenant, "counter", "1")

	if _, err := rt.Handle(ctx, sid, &counterv1.Init{Min: 0, Max: 100, Initial: 0}); err != nil {
		t.Fatalf("init: %v", err)
	}
	for i := 0; i < 7; i++ {
		if _, err := rt.Handle(ctx, sid, &counterv1.Increment{By: 1}); err != nil {
			t.Fatalf("inc %d: %v", i, err)
		}
	}

	pre, err := adapter.GetState(context.Background(), tenant, sid.Canonical())
	if err != nil {
		t.Fatalf("GetState pre-rebuild: %v", err)
	}

	rb := any(adapter).(aggregate.StateCacheRebuilder)
	written, err := aggregate.RebuildStateCache(context.Background(), rb, rt, tenant)
	if err != nil {
		t.Fatalf("RebuildStateCache: %v", err)
	}
	if written != 1 {
		t.Errorf("written: got %d want 1", written)
	}

	post, err := adapter.GetState(context.Background(), tenant, sid.Canonical())
	if err != nil {
		t.Fatalf("GetState post-rebuild: %v", err)
	}
	if post.Version != pre.Version {
		t.Errorf("version pre=%d post=%d", pre.Version, post.Version)
	}

	var preC, postC counterv1.Counter
	_ = protojson.Unmarshal(pre.State, &preC)
	_ = protojson.Unmarshal(post.State, &postC)
	if preC.Count != postC.Count {
		t.Errorf("count pre=%d post=%d", preC.Count, postC.Count)
	}
}
