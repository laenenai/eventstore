package postgres_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/laenenai/eventstore/aggregate"
	"github.com/laenenai/eventstore/es"
	"github.com/laenenai/eventstore/estest"
	counterv1 "github.com/laenenai/eventstore/gen/test/counter/v1"
)

// Shared test fixtures for the Postgres adapter: a small Counter
// aggregate (mirror of the SQLite test fixture) and a couple of
// helpers that produce per-test isolated tenant ids so the suite can
// share one testcontainer.

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

// newCounterRuntime returns a Counter aggregate runtime backed by the
// shared Postgres adapter. The adapter and pool are set up by TestMain.
func newCounterRuntime(t *testing.T) *aggregate.Runtime[counterState, counterv1.Command, counterv1.Event] {
	t.Helper()
	return &aggregate.Runtime[counterState, counterv1.Command, counterv1.Event]{
		Store:   adapter,
		Decider: counterDecider,
		Codec:   counterv1.EventCodec{},
	}
}

// tnt builds a per-test tenant id. The shared testcontainer means
// state survives across tests; namespacing by sanitized t.Name() keeps
// each test's data disjoint without per-test DB tear-down.
func tnt(t *testing.T, base string) string {
	t.Helper()
	safe := strings.ReplaceAll(t.Name(), "/", "-")
	safe = strings.ToLower(safe)
	return safe + "-" + base
}

// seedEvents creates an initialized Counter on each given tenant and
// appends perTenant-1 increment events afterwards (so each tenant gets
// exactly perTenant events). Returns the total event count.
func seedEvents(t *testing.T, rt *aggregate.Runtime[counterState, counterv1.Command, counterv1.Event], tenants []string, perTenant int) int {
	t.Helper()
	for _, tenant := range tenants {
		ctx := es.WithTenant(context.Background(), tenant)
		sid := estest.MustStream(t, tenant, "counter", "1")
		if _, err := rt.Handle(ctx, sid, &counterv1.Init{Min: 0, Max: 1000, Initial: 0}); err != nil {
			t.Fatalf("init %s: %v", tenant, err)
		}
		for i := 1; i < perTenant; i++ {
			if _, err := rt.Handle(ctx, sid, &counterv1.Increment{By: 1}); err != nil {
				t.Fatalf("inc %s: %v", tenant, err)
			}
		}
	}
	return len(tenants) * perTenant
}

// seedStreams appends perStream events to each of the given stream
// suffixes, all under the SAME tenant. Use for tests that need
// multiple streams but a single-tenant drain (so the test doesn't see
// leftover rows from other tests in the shared DB).
func seedStreams(t *testing.T, rt *aggregate.Runtime[counterState, counterv1.Command, counterv1.Event], tenant string, streamSuffixes []string, perStream int) int {
	t.Helper()
	ctx := es.WithTenant(context.Background(), tenant)
	for _, suf := range streamSuffixes {
		sid := estest.MustStream(t, tenant, "counter", suf)
		if _, err := rt.Handle(ctx, sid, &counterv1.Init{Min: 0, Max: 1000, Initial: 0}); err != nil {
			t.Fatalf("init %s/%s: %v", tenant, suf, err)
		}
		for i := 1; i < perStream; i++ {
			if _, err := rt.Handle(ctx, sid, &counterv1.Increment{By: 1}); err != nil {
				t.Fatalf("inc %s/%s: %v", tenant, suf, err)
			}
		}
	}
	return len(streamSuffixes) * perStream
}
