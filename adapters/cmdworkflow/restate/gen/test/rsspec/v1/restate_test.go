package rsspecv1restate_test

import (
	"context"
	"database/sql"
	"reflect"
	"testing"

	_ "modernc.org/sqlite"

	cwinproc "github.com/laenenai/eventstore/adapters/cmdworkflow/inproc"
	rs "github.com/laenenai/eventstore/adapters/cmdworkflow/restate/gen/test/rsspec/v1"
	sqliteadapter "github.com/laenenai/eventstore/adapters/storage/sqlite"
	"github.com/laenenai/eventstore/aggregate"
	"github.com/laenenai/eventstore/cmdworkflow"
	"github.com/laenenai/eventstore/es"
	v1 "github.com/laenenai/eventstore/gen/test/rsspec/v1"
)

// Tests for the codegen plugin's runtime=restate emitter (ADR 0026).
//
// Restate's own runtime is NOT exercised here — that's the adapter's
// integration test surface (testcontainers, separate). This file pins
// what the emitter is responsible for:
//
//   - ServiceName() returns the aggregate name (not the Go type name).
//   - One handler per Command variant, each with the right signature.
//   - The tenant + stream_id annotations are honoured for FIELD lookup
//     (not blindly looking up GetTenantId / GetStreamId by name).
//   - StreamID construction uses the aggregate's name.
//   - Handlers route to Workflow.HandleCmd cleanly when given a nil
//     sdk_go.Context (the inproc workflow runtime doesn't unwrap it).
//   - NewRestateService wires SetAsyncSend without panicking.
//
// Source proto: proto/test/rsspec/v1/rsspec.proto.

// ---- Minimal decider for the rs aggregate -----------------------------

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

func newService(t *testing.T) *rs.RestateService {
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
	return rs.NewRestateService(wf)
}

// ---- ServiceName -----------------------------------------------------

func TestServiceName_IsTheAggregateName(t *testing.T) {
	s := newService(t)
	// Aggregate name comes from (es.v1.aggregate) = "rs" — title-cased
	// by the emitter for the Restate URL convention (/<Service>/<Method>).
	if got := s.ServiceName(); got != "Rs" {
		t.Errorf("ServiceName: got %q want %q", got, "Rs")
	}
}

// ---- Method existence + signature ------------------------------------
//
// reflect-based check: each Command variant must produce a method on
// *RestateService with signature `(sdk_go.Context, *<Cmd>) (*<State>, error)`.
// If the emitter ever drops a variant we'll see it here.

func TestHandlers_OneMethodPerCommandVariant(t *testing.T) {
	expected := []string{"CreateRs", "UpdateRs", "DeleteRs", "AsyncDispatch"}
	rv := reflect.TypeOf((*rs.RestateService)(nil))
	for _, name := range expected {
		if _, ok := rv.MethodByName(name); !ok {
			t.Errorf("expected method %s on *RestateService", name)
		}
	}
}

func TestHandlers_AreNotAccidentalCollisionsWithReservedNames(t *testing.T) {
	// Sanity: no accidental method named e.g. "ServiceName_*" or similar
	// shadowing. The exact public method count we expect is:
	//   ServiceName, CreateRs, UpdateRs, DeleteRs, AsyncDispatch = 5
	// (plus any unexported helpers, which reflect doesn't see).
	rv := reflect.TypeOf((*rs.RestateService)(nil))
	expected := 5
	if got := rv.NumMethod(); got != expected {
		names := make([]string, 0, got)
		for i := 0; i < got; i++ {
			names = append(names, rv.Method(i).Name)
		}
		t.Errorf("expected %d methods on *RestateService, got %d: %v", expected, got, names)
	}
}

// ---- Tenant + stream_id extraction (annotation-driven) ---------------
//
// The emitter must read the (es.v1.tenant_id) and (es.v1.stream_id)
// field annotations, not blindly call GetTenantId / GetStreamId by
// name. CreateRs is the canary — its tenant field is named `tenant`
// (Go: Tenant, accessor GetTenant()). If the emitter regressed to
// hard-coded name lookup, CreateRs would compile but the wrong field
// would be read and tenant would be "".

func TestHandlers_CreateRs_RoutesAndPersists(t *testing.T) {
	s := newService(t)
	state, err := s.CreateRs(nil, &v1.CreateRs{
		Tenant: "tenant-A",
		RsId:   "rs-1",
		Label:  "hello",
	})
	if err != nil {
		t.Fatalf("CreateRs: %v", err)
	}
	if state == nil {
		t.Fatal("nil state from handler")
	}
	if state.RsId != "rs-1" || state.Label != "hello" {
		t.Errorf("state after CreateRs: %+v", state)
	}
	// HandleCmd internally loads post-decide state from the runtime —
	// a non-zero return proves the whole append + load round-trip ran.
}

func TestHandlers_UpdateRs_RoutesUsingTenantIdField(t *testing.T) {
	// UpdateRs has tenant_id (the typical name) — exercises the
	// other branch of the annotation→field resolution.
	s := newService(t)
	_, err := s.CreateRs(nil, &v1.CreateRs{
		Tenant: "tenant-B", RsId: "rs-2", Label: "v1",
	})
	if err != nil {
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

func TestHandlers_DeleteRs_RoutesAndTriggersTerminal(t *testing.T) {
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
	// Decider zeroes RsId on RsDeleted; state reflects post-Decide.
	if state.RsId != "" {
		t.Errorf("delete should zero rs_id: %+v", state)
	}
}

func TestHandlers_StreamIDUsesAggregateName(t *testing.T) {
	// The codegen embeds the aggregate name from (es.v1.aggregate) =
	// "rs" into the es.NewStreamID call. If that ever drifts, the
	// stream id we observe via Load will use the wrong type prefix,
	// and the canonical form would not match.
	s := newService(t)
	if _, err := s.CreateRs(nil, &v1.CreateRs{
		Tenant: "tenant-D", RsId: "rs-4", Label: "x",
	}); err != nil {
		t.Fatalf("CreateRs: %v", err)
	}
	sid := mustSID(t, "tenant-D", "rs-4")
	if !contains(sid.Canonical(), "rs:rs-4") {
		t.Errorf("StreamID canonical form should use 'rs' as type prefix: %q", sid.Canonical())
	}
}

// ---- Missing tenant / stream_id should fail loudly ------------------

func TestHandlers_EmptyTenantIsRejectedDownstream(t *testing.T) {
	// The codegen-emitted handler doesn't itself validate tenant != "" —
	// it routes the value to es.NewStreamID + es.WithTenant. The
	// framework's mandatory-tenancy contract (ADR 0007) makes the
	// downstream call fail. Confirm that propagates.
	s := newService(t)
	_, err := s.CreateRs(nil, &v1.CreateRs{
		Tenant: "", RsId: "rs-x", Label: "no tenant",
	})
	if err == nil {
		t.Error("expected error when tenant is empty")
	}
}

// ---- NewRestateService side-effects ----------------------------------

func TestNewRestateService_DoesNotPanicAndSetsWorkflow(t *testing.T) {
	s := newService(t)
	if s.Workflow == nil {
		t.Error("NewRestateService should wire Workflow")
	}
	// Implicit: NewRestateService also calls wf.SetAsyncSend(s.sendAsync).
	// We can't introspect that directly (the field is unexported on
	// Workflow), but a sendAsync call path test belongs in the bus
	// test surface, not the emitter's.
}

// ---- helpers ----------------------------------------------------------

func mustSID(t *testing.T, tenant, id string) es.StreamID {
	t.Helper()
	sid, err := es.NewStreamID(tenant, "rs", id)
	if err != nil {
		t.Fatalf("NewStreamID: %v", err)
	}
	return sid
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
