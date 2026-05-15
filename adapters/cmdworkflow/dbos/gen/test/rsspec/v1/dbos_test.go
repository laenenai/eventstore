package rsspecv1dbos_test

import (
	"context"
	"database/sql"
	"reflect"
	"testing"

	_ "modernc.org/sqlite"

	cwinproc "github.com/laenenai/eventstore/adapters/cmdworkflow/inproc"
	dbosgen "github.com/laenenai/eventstore/adapters/cmdworkflow/dbos/gen/test/rsspec/v1"
	sqliteadapter "github.com/laenenai/eventstore/adapters/storage/sqlite"
	"github.com/laenenai/eventstore/aggregate"
	"github.com/laenenai/eventstore/cmdworkflow"
	"github.com/laenenai/eventstore/es"
	v1 "github.com/laenenai/eventstore/gen/test/rsspec/v1"
)

// Tests for the codegen plugin's runtime=dbos emitter (ADR 0026).
// Parallel to the Restate emitter tests; shared synthetic proto
// (rsspec.proto). DBOS's runtime is NOT exercised here — these tests
// pin the emitter's shape and dispatch contract.
//
// The DBOS service is the same shape as Restate's, with two
// differences worth pinning:
//
//   - No ServiceName() method (DBOS uses RegisterWorkflow names,
//     not a /<Service>/<Method> convention).
//   - The runtime context type is dbos1.DBOSContext, not sdk_go.Context.

var decider = es.Decider[*v1.RsState, v1.Command, v1.Event]{
	Initial: func() *v1.RsState { return &v1.RsState{} },

	Decide: func(s *v1.RsState, c v1.Command) ([]v1.Event, []es.ConstraintOp, error) {
		switch cmd := c.(type) {
		case *v1.CreateRs:
			return []v1.Event{&v1.RsCreated{RsId: cmd.RsId, Label: cmd.Label}}, nil, nil
		case *v1.UpdateRs:
			return []v1.Event{&v1.RsUpdated{RsId: cmd.RsId, NewLabel: cmd.NewLabel}}, nil, nil
		case *v1.DeleteRs:
			return []v1.Event{&v1.RsDeleted{RsId: cmd.RsId, Reason: cmd.Reason}}, nil, nil
		}
		return nil, nil, nil
	},

	Evolve: func(s *v1.RsState, e v1.Event) *v1.RsState {
		out := &v1.RsState{RsId: s.RsId, Label: s.Label}
		switch evt := e.(type) {
		case *v1.RsCreated:
			out.RsId = evt.RsId
			out.Label = evt.Label
		case *v1.RsUpdated:
			out.Label = evt.NewLabel
		case *v1.RsDeleted:
			out.RsId = ""
		}
		return out
	},
}

func newService(t *testing.T) *dbosgen.DBOSService {
	t.Helper()
	d, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { _ = d.Close() })
	a := sqliteadapter.New(d)
	if err := a.Migrate(context.Background()); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	wf := cmdworkflow.New[*v1.RsState, v1.Command, v1.Event](
		aggregate.NewProto(a, decider, v1.EventCodec{}),
		a, cwinproc.New(), v1.EventCodec{},
	)
	return dbosgen.NewDBOSService(wf)
}

// ---- Method existence + signature ------------------------------------

func TestDBOSHandlers_OneMethodPerCommandVariant(t *testing.T) {
	expected := []string{"CreateRs", "UpdateRs", "DeleteRs", "AsyncDispatch"}
	rv := reflect.TypeOf((*dbosgen.DBOSService)(nil))
	for _, name := range expected {
		if _, ok := rv.MethodByName(name); !ok {
			t.Errorf("expected method %s on *DBOSService", name)
		}
	}
}

func TestDBOSService_HasNoServiceNameMethod(t *testing.T) {
	// DBOS doesn't use a /<Service>/<Method> URL convention; ServiceName
	// is Restate-specific. If the emitter accidentally added one, it
	// would mislead operators trying to register the workflows.
	rv := reflect.TypeOf((*dbosgen.DBOSService)(nil))
	if _, ok := rv.MethodByName("ServiceName"); ok {
		t.Error("DBOSService should NOT have a ServiceName method")
	}
}

// ---- Tenant + stream_id extraction (annotation-driven) ---------------

func TestDBOSHandlers_CreateRs_RoutesAndPersists(t *testing.T) {
	// CreateRs uses `tenant` field (Go: Tenant, accessor GetTenant()) —
	// the emitter must read the annotation, not the field name.
	s := newService(t)
	state, err := s.CreateRs(nil, &v1.CreateRs{
		Tenant: "tenant-A",
		RsId:   "rs-1",
		Label:  "hello",
	})
	if err != nil {
		t.Fatalf("CreateRs: %v", err)
	}
	if state == nil || state.RsId != "rs-1" || state.Label != "hello" {
		t.Errorf("post-Decide state: %+v", state)
	}
}

func TestDBOSHandlers_UpdateRs_RoutesUsingTenantIdField(t *testing.T) {
	s := newService(t)
	if _, err := s.CreateRs(nil, &v1.CreateRs{
		Tenant: "tenant-B", RsId: "rs-2", Label: "v1",
	}); err != nil {
		t.Fatalf("seed CreateRs: %v", err)
	}
	state, err := s.UpdateRs(nil, &v1.UpdateRs{
		TenantId: "tenant-B", RsId: "rs-2", NewLabel: "v2",
	})
	if err != nil {
		t.Fatalf("UpdateRs: %v", err)
	}
	if state.Label != "v2" {
		t.Errorf("label after update: got %q want v2", state.Label)
	}
}

func TestDBOSHandlers_DeleteRs_RoutesAndTriggersTerminal(t *testing.T) {
	s := newService(t)
	if _, err := s.CreateRs(nil, &v1.CreateRs{
		Tenant: "tenant-C", RsId: "rs-3", Label: "to-delete",
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	state, err := s.DeleteRs(nil, &v1.DeleteRs{
		TenantId: "tenant-C", RsId: "rs-3", Reason: "obsolete",
	})
	if err != nil {
		t.Fatalf("DeleteRs: %v", err)
	}
	if state.RsId != "" {
		t.Errorf("delete should zero rs_id: %+v", state)
	}
}

// ---- Missing tenant rejected downstream ------------------------------

func TestDBOSHandlers_EmptyTenantIsRejectedDownstream(t *testing.T) {
	s := newService(t)
	_, err := s.CreateRs(nil, &v1.CreateRs{
		Tenant: "", RsId: "rs-x", Label: "no tenant",
	})
	if err == nil {
		t.Error("expected error when tenant is empty")
	}
}

// ---- NewDBOSService side-effects --------------------------------------

func TestNewDBOSService_DoesNotPanicAndSetsWorkflow(t *testing.T) {
	s := newService(t)
	if s.Workflow == nil {
		t.Error("NewDBOSService should wire Workflow")
	}
}
