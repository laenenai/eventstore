//go:build restate

package restate_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	restatesdk "github.com/restatedev/sdk-go"
	"github.com/restatedev/sdk-go/ingress"

	cwrestate "github.com/laenenai/eventstore/adapters/cmdworkflow/restate"
	"github.com/laenenai/eventstore/adapters/cmdworkflow/restate/testsupport"
	sqliteadapter "github.com/laenenai/eventstore/adapters/storage/sqlite"
	"github.com/laenenai/eventstore/aggregate"
	"github.com/laenenai/eventstore/cmdworkflow"
	"github.com/laenenai/eventstore/es"
	counterv1 "github.com/laenenai/eventstore/gen/test/counter/v1"
)

// Build-tagged `restate` — pulls a real restatedev/restate container
// via testcontainers. Skip the default `go test ./...` run; opt in with
//
//	go test -tags restate ./adapters/cmdworkflow/restate/...

// counterDecider — mirror of the existing test counter decider so we
// can reuse counterv1 codegen for state/commands/events.
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
			return []counterv1.Event{&counterv1.Incremented{By: cmd.By}}, nil, nil
		}
		return nil, nil, errors.New("counter: unknown command")
	},
	Evolve: func(s *counterv1.Counter, e counterv1.Event) *counterv1.Counter {
		out := &counterv1.Counter{
			Initialized: s.Initialized, Count: s.Count, Min: s.Min, Max: s.Max,
		}
		switch evt := e.(type) {
		case *counterv1.Initialized:
			out.Initialized = true
			out.Min = evt.Min
			out.Max = evt.Max
			out.Count = evt.Value
		case *counterv1.Incremented:
			out.Count += evt.By
		}
		return out
	},
}

// CounterService is a hand-written Restate service that exposes one
// handler per command type for the Counter aggregate. Phase 2c
// codegen will emit this; for Phase 2a it's hand-written so we can
// validate the bridging.
type CounterService struct {
	wf *cmdworkflow.Workflow[*counterv1.Counter, counterv1.Command]
}

// Init handler — Restate will route POST /CounterService/Init to here.
func (s *CounterService) Init(ctx restatesdk.Context, req initRequest) (counterResult, error) {
	tenant := req.Tenant
	if tenant == "" {
		tenant = "default"
	}
	stdCtx := cwrestate.WithContext(es.WithTenant(context.Background(), tenant), ctx)
	sid, err := es.NewStreamID(tenant, "counter", req.ID)
	if err != nil {
		return counterResult{}, err
	}
	state, err := s.wf.HandleCmd(stdCtx, sid, &counterv1.Init{
		Min: req.Min, Max: req.Max, Initial: req.Initial,
	})
	if err != nil {
		return counterResult{}, err
	}
	return counterResultFrom(state), nil
}

// Increment handler — POST /CounterService/Increment.
func (s *CounterService) Increment(ctx restatesdk.Context, req incrementRequest) (counterResult, error) {
	tenant := req.Tenant
	if tenant == "" {
		tenant = "default"
	}
	stdCtx := cwrestate.WithContext(es.WithTenant(context.Background(), tenant), ctx)
	sid, err := es.NewStreamID(tenant, "counter", req.ID)
	if err != nil {
		return counterResult{}, err
	}
	state, err := s.wf.HandleCmd(stdCtx, sid, &counterv1.Increment{By: req.By})
	if err != nil {
		return counterResult{}, err
	}
	return counterResultFrom(state), nil
}

// Wire types — JSON-serializable shapes Restate uses on the wire.
type initRequest struct {
	Tenant  string `json:"tenant"`
	ID      string `json:"id"`
	Min     int64  `json:"min"`
	Max     int64  `json:"max"`
	Initial int64  `json:"initial"`
}

type incrementRequest struct {
	Tenant string `json:"tenant"`
	ID     string `json:"id"`
	By     int64  `json:"by"`
}

type counterResult struct {
	Initialized bool  `json:"initialized"`
	Count       int64 `json:"count"`
	Min         int64 `json:"min"`
	Max         int64 `json:"max"`
}

func counterResultFrom(c *counterv1.Counter) counterResult {
	return counterResult{
		Initialized: c.Initialized, Count: c.Count, Min: c.Min, Max: c.Max,
	}
}

// testFixture wires SQLite eventstore + Workflow + Restate test env.
type testFixture struct {
	env *testsupport.Env
	wf  *cmdworkflow.Workflow[*counterv1.Counter, counterv1.Command]
}

func newFixture(t *testing.T) *testFixture {
	t.Helper()

	// Shared in-memory SQLite — works across the SDK server's
	// connection pool (see examples/cmdworkflow note).
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	a := sqliteadapter.New(db)
	if err := a.Migrate(context.Background()); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	runtime := cwrestate.New()
	rt := aggregate.NewProto(a, counterDecider, counterv1.EventCodec{})
	wf := cmdworkflow.New[*counterv1.Counter, counterv1.Command](rt, a, runtime).WithDLQ(a)

	svc := &CounterService{wf: wf}

	// testsupport.Start spawns the Restate testcontainer, registers
	// the SDK server, and tears everything down via t.Cleanup.
	env := testsupport.Start(t, restatesdk.Reflect(svc))

	return &testFixture{env: env, wf: wf}
}

// TestRestate_Smoke verifies end-to-end: Restate testcontainer +
// SDK server + our cmdworkflow workflow + SQLite eventstore. One
// Init + one Increment, verify the returned state reflects both.
func TestRestate_Smoke(t *testing.T) {
	fx := newFixture(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	initSvc := ingress.Service[initRequest, counterResult](fx.env.Ingress(), "CounterService", "Init")
	initResult, err := initSvc.Request(ctx, initRequest{
		Tenant: "acme", ID: "smoke-1", Min: 0, Max: 100, Initial: 0,
	})
	if err != nil {
		t.Fatalf("Init: %v", err)
	}
	if !initResult.Initialized || initResult.Count != 0 {
		t.Errorf("after Init: got %+v", initResult)
	}

	incSvc := ingress.Service[incrementRequest, counterResult](fx.env.Ingress(), "CounterService", "Increment")
	incResult, err := incSvc.Request(ctx, incrementRequest{
		Tenant: "acme", ID: "smoke-1", By: 7,
	})
	if err != nil {
		t.Fatalf("Increment: %v", err)
	}
	if incResult.Count != 7 {
		t.Errorf("after Increment: count=%d want 7", incResult.Count)
	}
}
