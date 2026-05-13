package sqlite_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"

	_ "modernc.org/sqlite"

	sqliteadapter "github.com/laenenai/eventstore/adapters/storage/sqlite"
	"github.com/laenenai/eventstore/aggregate"
	"github.com/laenenai/eventstore/es"
	"github.com/laenenai/eventstore/estest"
	counterv1 "github.com/laenenai/eventstore/gen/test/counter/v1"
)

// Tier-1 state_cache tests against the SQLite adapter. Uses the
// proto-typed Counter state (counterv1.Counter) so the existing
// hand-written counterState fixture for aggregate_test.go stays
// untouched — opt-in per-aggregate (ADR 0020).

// counterProtoDecider is the Decider with *counterv1.Counter as state.
// Same business rules as counterDecider — just the typed-state variant
// used to exercise the Tier-1 cache.
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
		// Defensive: never mutate the input — clone first.
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

func newCounterProtoRuntime(t *testing.T) (es.Store, *aggregate.Runtime[*counterv1.Counter, counterv1.Command, counterv1.Event]) {
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
		Decider:    counterProtoDecider,
		Codec:      counterv1.EventCodec{},
		StateCodec: aggregate.ProtoStateCodec[*counterv1.Counter]{},
	}
	return a, rt
}

// TestStateCache_WrittenInTxWithAppend verifies the cache row appears
// immediately after a successful Handle (read-your-writes).
func TestStateCache_WrittenInTxWithAppend(t *testing.T) {
	store, rt := newCounterProtoRuntime(t)
	ctx := es.WithTenant(context.Background(), "t-cache")
	sid := estest.MustStream(t, "t-cache", "counter", "1")

	if _, err := rt.Handle(ctx, sid, &counterv1.Init{Min: 0, Max: 100, Initial: 5}); err != nil {
		t.Fatalf("init: %v", err)
	}

	reader := store.(es.StateCacheReader)
	row, err := reader.GetState(context.Background(), "t-cache", sid.Canonical())
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
		t.Errorf("state: got %+v want Initialized=true Count=5 Min=0 Max=100", &c)
	}

	// Second command — state must reflect new count.
	if _, err := rt.Handle(ctx, sid, &counterv1.Increment{By: 3}); err != nil {
		t.Fatalf("inc: %v", err)
	}
	row, _ = reader.GetState(context.Background(), "t-cache", sid.Canonical())
	if row.Version != 2 {
		t.Errorf("version after inc: got %d want 2", row.Version)
	}
	_ = protojson.Unmarshal(row.State, &c)
	if c.Count != 8 {
		t.Errorf("count after inc: got %d want 8", c.Count)
	}
}

// TestStateCache_DisabledWhenCodecUnset verifies the cache stays empty
// when StateCodec is nil (opt-in semantics).
func TestStateCache_DisabledWhenCodecUnset(t *testing.T) {
	db, _ := sql.Open("sqlite", ":memory:")
	t.Cleanup(func() { _ = db.Close() })
	a := sqliteadapter.New(db)
	if err := a.Migrate(context.Background()); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	rt := &aggregate.Runtime[*counterv1.Counter, counterv1.Command, counterv1.Event]{
		Store:   a,
		Decider: counterProtoDecider,
		Codec:   counterv1.EventCodec{},
		// StateCodec deliberately unset.
	}
	ctx := es.WithTenant(context.Background(), "t-off")
	sid := estest.MustStream(t, "t-off", "counter", "1")
	if _, err := rt.Handle(ctx, sid, &counterv1.Init{Min: 0, Max: 10, Initial: 1}); err != nil {
		t.Fatalf("init: %v", err)
	}

	if _, err := a.GetState(context.Background(), "t-off", sid.Canonical()); !errors.Is(err, es.ErrStateNotFound) {
		t.Errorf("GetState with no codec: got err=%v want ErrStateNotFound", err)
	}
}

// TestStateCache_ListStates exercises pagination.
func TestStateCache_ListStates(t *testing.T) {
	store, rt := newCounterProtoRuntime(t)
	tenant := "t-list"
	ctx := es.WithTenant(context.Background(), tenant)
	for _, id := range []string{"1", "2", "3", "4", "5"} {
		sid := estest.MustStream(t, tenant, "counter", id)
		if _, err := rt.Handle(ctx, sid, &counterv1.Init{Min: 0, Max: 100, Initial: int64(len(id))}); err != nil {
			t.Fatalf("init %s: %v", id, err)
		}
	}

	reader := store.(es.StateCacheReader)
	// First page of 2.
	page1, err := reader.ListStates(context.Background(), tenant, "test.counter.v1.Counter", "", 2)
	if err != nil {
		t.Fatalf("ListStates page 1: %v", err)
	}
	if len(page1) != 2 {
		t.Errorf("page1 size: got %d want 2", len(page1))
	}

	// Next page using afterStreamID.
	page2, err := reader.ListStates(context.Background(), tenant, "test.counter.v1.Counter", page1[len(page1)-1].StreamID, 100)
	if err != nil {
		t.Fatalf("ListStates page 2: %v", err)
	}
	if len(page2) != 3 {
		t.Errorf("page2 size: got %d want 3", len(page2))
	}
}

// TestStateCache_Rebuild verifies events → fold → cache produces the
// same state as the live write path.
func TestStateCache_Rebuild(t *testing.T) {
	store, rt := newCounterProtoRuntime(t)
	tenant := "t-rebuild"
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

	// Capture the live cache state.
	reader := store.(es.StateCacheReader)
	pre, err := reader.GetState(context.Background(), tenant, sid.Canonical())
	if err != nil {
		t.Fatalf("GetState pre-rebuild: %v", err)
	}

	// Wipe + rebuild.
	rb := store.(aggregate.StateCacheRebuilder)
	written, err := aggregate.RebuildStateCache(context.Background(), rb, rt, tenant)
	if err != nil {
		t.Fatalf("RebuildStateCache: %v", err)
	}
	if written != 1 {
		t.Errorf("written: got %d want 1", written)
	}

	post, err := reader.GetState(context.Background(), tenant, sid.Canonical())
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
	if preC.Min != postC.Min || preC.Max != postC.Max {
		t.Errorf("min/max pre=(%d,%d) post=(%d,%d)", preC.Min, preC.Max, postC.Min, postC.Max)
	}
}
