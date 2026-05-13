package party_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	_ "modernc.org/sqlite"

	sqliteadapter "github.com/laenenai/eventstore/adapters/storage/sqlite"
	"github.com/laenenai/eventstore/aggregate"
	"github.com/laenenai/eventstore/es"
	"github.com/laenenai/eventstore/estest"
	"github.com/laenenai/eventstore/examples/party"
	partyv1 "github.com/laenenai/eventstore/gen/myapp/party/v1"
)

// Tests run against SQLite via modernc.org/sqlite. Default is ":memory:".
// Set EVENTSTORE_TEST_DB=/path/to/dir to persist DB files for debugging.

const tenant = "t-party"

func newRuntime(t *testing.T) *aggregate.Runtime[*party.State, partyv1.Command, partyv1.Event] {
	t.Helper()

	dsn := ":memory:"
	if dir := os.Getenv("EVENTSTORE_TEST_DB"); dir != "" {
		_ = os.MkdirAll(dir, 0o755)
		safeName := strings.ReplaceAll(t.Name(), "/", "_")
		dsn = fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)&_pragma=foreign_keys(ON)",
			filepath.Join(dir, safeName+".db"))
	}

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	a := sqliteadapter.New(db)
	if err := a.Migrate(context.Background()); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	return &aggregate.Runtime[*party.State, partyv1.Command, partyv1.Event]{
		Store:   a,
		Decider: party.Decider,
		Codec:   party.EventCodec,
	}
}

func register(t *testing.T, rt *aggregate.Runtime[*party.State, partyv1.Command, partyv1.Event], partyID string) es.StreamID {
	t.Helper()
	ctx := es.WithTenant(context.Background(), tenant)
	sid := estest.MustStream(t, tenant, "party", partyID)
	_, err := rt.Handle(ctx, sid, &partyv1.Register{
		Name:        &partyv1.Name{First: "Alice", Last: "Smith"},
		Email:       partyID + "@example.com",
		Phone:       "+1-555-0100",
		Address:     &partyv1.Address{Line1: "1 Main St", City: "Springfield", Country: "US"},
		DateOfBirth: "1990-01-01",
		ActorId:     "system",
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	return sid
}

// ----- Lifecycle ----------------------------------------------------------

func TestRegister_Success(t *testing.T) {
	rt := newRuntime(t)
	sid := register(t, rt, "p1")

	state, version, err := rt.Load(context.Background(), sid)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if version != 1 {
		t.Errorf("version: got %d want 1", version)
	}
	if state.GetEmail() != "p1@example.com" {
		t.Errorf("Email: got %q want p1@example.com", state.GetEmail())
	}
	if state.GetStatus() != partyv1.Status_STATUS_ACTIVE {
		t.Errorf("Status: got %v want ACTIVE", state.GetStatus())
	}
}

func TestRegister_TwiceFails(t *testing.T) {
	rt := newRuntime(t)
	sid := register(t, rt, "p1")

	ctx := es.WithTenant(context.Background(), tenant)
	_, err := rt.Handle(ctx, sid, &partyv1.Register{
		Name:        &partyv1.Name{First: "Bob", Last: "Jones"},
		Email:       "bob@example.com",
		Phone:       "+1-555-0200",
		Address:     &partyv1.Address{Line1: "2 Main St", City: "Springfield", Country: "US"},
		DateOfBirth: "1995-01-01",
		ActorId:     "system",
	})
	if !errors.Is(err, party.ErrAlreadyRegistered) {
		t.Fatalf("expected ErrAlreadyRegistered, got %v", err)
	}
}

func TestRegister_EmailUniqueAcrossStreams(t *testing.T) {
	rt := newRuntime(t)
	register(t, rt, "p1")

	ctx := es.WithTenant(context.Background(), tenant)
	sid2 := estest.MustStream(t, tenant, "party", "p2")
	_, err := rt.Handle(ctx, sid2, &partyv1.Register{
		Name:        &partyv1.Name{First: "Bob", Last: "Jones"},
		Email:       "p1@example.com", // duplicate
		Phone:       "+1-555-0200",
		Address:     &partyv1.Address{Line1: "2 Main St", City: "Springfield", Country: "US"},
		DateOfBirth: "1995-01-01",
		ActorId:     "system",
	})
	if !errors.Is(err, es.ErrConstraintViolated) {
		t.Fatalf("expected ErrConstraintViolated, got %v", err)
	}
}

// ----- Maker-checker workflow ---------------------------------------------

func TestProposeName_HappyPath(t *testing.T) {
	rt := newRuntime(t)
	sid := register(t, rt, "p1")
	ctx := es.WithTenant(context.Background(), tenant)

	if _, err := rt.Handle(ctx, sid, &partyv1.ProposeName{
		ChangeId:   "c1",
		Proposed:   &partyv1.Name{First: "Alice", Last: "Johnson"},
		ProposedBy: "maker-1",
		Reason:     "marriage",
	}); err != nil {
		t.Fatalf("ProposeName: %v", err)
	}

	state, _, _ := rt.Load(ctx, sid)
	if len(state.GetPendingChanges()) != 1 {
		t.Fatalf("expected 1 pending change, got %d", len(state.GetPendingChanges()))
	}
	if state.GetName().GetLast() != "Smith" {
		t.Errorf("Name not yet applied; got %v", state.GetName())
	}

	if _, err := rt.Handle(ctx, sid, &partyv1.Approve{
		ChangeId:   "c1",
		ApprovedBy: "checker-1",
		Comment:    "looks good",
	}); err != nil {
		t.Fatalf("Approve: %v", err)
	}

	state, _, _ = rt.Load(ctx, sid)
	if state.GetName().GetLast() != "Johnson" {
		t.Errorf("Name not applied; got %v", state.GetName())
	}
	if len(state.GetPendingChanges()) != 0 {
		t.Errorf("expected pending cleared, got %d", len(state.GetPendingChanges()))
	}
}

func TestProposeName_SelfApprovalForbidden(t *testing.T) {
	rt := newRuntime(t)
	sid := register(t, rt, "p1")
	ctx := es.WithTenant(context.Background(), tenant)

	_, _ = rt.Handle(ctx, sid, &partyv1.ProposeName{
		ChangeId: "c1", Proposed: &partyv1.Name{First: "Alice", Last: "Doe"},
		ProposedBy: "maker-1", Reason: "test",
	})

	_, err := rt.Handle(ctx, sid, &partyv1.Approve{
		ChangeId: "c1", ApprovedBy: "maker-1",
	})
	if !errors.Is(err, party.ErrSelfApproval) {
		t.Fatalf("expected ErrSelfApproval, got %v", err)
	}
}

func TestProposeName_AtMostOnePending(t *testing.T) {
	rt := newRuntime(t)
	sid := register(t, rt, "p1")
	ctx := es.WithTenant(context.Background(), tenant)

	if _, err := rt.Handle(ctx, sid, &partyv1.ProposeName{
		ChangeId: "c1", Proposed: &partyv1.Name{First: "A", Last: "B"},
		ProposedBy: "maker-1", Reason: "first",
	}); err != nil {
		t.Fatalf("first propose: %v", err)
	}

	_, err := rt.Handle(ctx, sid, &partyv1.ProposeName{
		ChangeId: "c2", Proposed: &partyv1.Name{First: "C", Last: "D"},
		ProposedBy: "maker-1", Reason: "second",
	})
	if !errors.Is(err, party.ErrPendingExists) {
		t.Fatalf("expected ErrPendingExists, got %v", err)
	}
}

func TestProposeName_RejectedByChecker(t *testing.T) {
	rt := newRuntime(t)
	sid := register(t, rt, "p1")
	ctx := es.WithTenant(context.Background(), tenant)

	_, _ = rt.Handle(ctx, sid, &partyv1.ProposeName{
		ChangeId: "c1", Proposed: &partyv1.Name{First: "X", Last: "Y"},
		ProposedBy: "maker-1", Reason: "test",
	})

	if _, err := rt.Handle(ctx, sid, &partyv1.Reject{
		ChangeId: "c1", RejectedBy: "checker-1", Reason: "incorrect",
	}); err != nil {
		t.Fatalf("Reject: %v", err)
	}

	state, _, _ := rt.Load(ctx, sid)
	if len(state.GetPendingChanges()) != 0 {
		t.Errorf("pending should be cleared after reject, got %d", len(state.GetPendingChanges()))
	}
	if state.GetName().GetFirst() == "X" {
		t.Errorf("rejected change was applied; got %v", state.GetName())
	}
}

func TestPropose_WithdrawnByProposer(t *testing.T) {
	rt := newRuntime(t)
	sid := register(t, rt, "p1")
	ctx := es.WithTenant(context.Background(), tenant)

	_, _ = rt.Handle(ctx, sid, &partyv1.ProposeName{
		ChangeId: "c1", Proposed: &partyv1.Name{First: "X", Last: "Y"},
		ProposedBy: "maker-1", Reason: "test",
	})

	_, err := rt.Handle(ctx, sid, &partyv1.Withdraw{
		ChangeId: "c1", WithdrawnBy: "other",
	})
	if !errors.Is(err, party.ErrNotProposer) {
		t.Fatalf("expected ErrNotProposer, got %v", err)
	}

	if _, err := rt.Handle(ctx, sid, &partyv1.Withdraw{
		ChangeId: "c1", WithdrawnBy: "maker-1",
	}); err != nil {
		t.Fatalf("Withdraw: %v", err)
	}

	state, _, _ := rt.Load(ctx, sid)
	if len(state.GetPendingChanges()) != 0 {
		t.Errorf("pending should be cleared after withdraw")
	}
}

// ----- Email maker-checker + uniqueness ----------------------------------

func TestProposeEmail_ApprovalReleasesOldClaimsNew(t *testing.T) {
	rt := newRuntime(t)
	sid := register(t, rt, "p1")
	ctx := es.WithTenant(context.Background(), tenant)

	if _, err := rt.Handle(ctx, sid, &partyv1.ProposeEmail{
		ChangeId: "ec1", Proposed: "alice-new@example.com",
		ProposedBy: "maker-1", Reason: "email change",
	}); err != nil {
		t.Fatalf("ProposeEmail: %v", err)
	}

	if _, err := rt.Handle(ctx, sid, &partyv1.Approve{
		ChangeId: "ec1", ApprovedBy: "checker-1",
	}); err != nil {
		t.Fatalf("Approve: %v", err)
	}

	state, _, _ := rt.Load(ctx, sid)
	if state.GetEmail() != "alice-new@example.com" {
		t.Errorf("Email: got %q want alice-new@example.com", state.GetEmail())
	}

	// Old email is now free
	sid2 := estest.MustStream(t, tenant, "party", "p2")
	_, err := rt.Handle(ctx, sid2, &partyv1.Register{
		Name:        &partyv1.Name{First: "Bob", Last: "Jones"},
		Email:       "p1@example.com",
		Address:     &partyv1.Address{Line1: "2", City: "X", Country: "US"},
		DateOfBirth: "1990-01-01",
		ActorId:     "system",
	})
	if err != nil {
		t.Fatalf("re-register with released email: %v", err)
	}

	// New email is taken
	sid3 := estest.MustStream(t, tenant, "party", "p3")
	_, err = rt.Handle(ctx, sid3, &partyv1.Register{
		Name:        &partyv1.Name{First: "Carol", Last: "K"},
		Email:       "alice-new@example.com",
		Address:     &partyv1.Address{Line1: "3", City: "X", Country: "US"},
		DateOfBirth: "1990-01-01",
		ActorId:     "system",
	})
	if !errors.Is(err, es.ErrConstraintViolated) {
		t.Fatalf("expected ErrConstraintViolated for taken email, got %v", err)
	}
}

// ----- Auto-apply ---------------------------------------------------------

func TestUpdatePhone_AutoApplied(t *testing.T) {
	rt := newRuntime(t)
	sid := register(t, rt, "p1")
	ctx := es.WithTenant(context.Background(), tenant)

	if _, err := rt.Handle(ctx, sid, &partyv1.UpdatePhone{
		NewPhone: "+44-20-7946-0958", ActorId: "self",
	}); err != nil {
		t.Fatalf("UpdatePhone: %v", err)
	}

	state, _, _ := rt.Load(ctx, sid)
	if state.GetPhone() != "+44-20-7946-0958" {
		t.Errorf("Phone not updated; got %q", state.GetPhone())
	}
	if len(state.GetPendingChanges()) != 0 {
		t.Errorf("expected no pending changes for auto-apply, got %d", len(state.GetPendingChanges()))
	}
}

func TestUpdateAddress_AutoApplied(t *testing.T) {
	rt := newRuntime(t)
	sid := register(t, rt, "p1")
	ctx := es.WithTenant(context.Background(), tenant)

	if _, err := rt.Handle(ctx, sid, &partyv1.UpdateAddress{
		NewAddress: &partyv1.Address{Line1: "10 Downing St", City: "London", Country: "GB"},
		ActorId:    "self",
	}); err != nil {
		t.Fatalf("UpdateAddress: %v", err)
	}

	state, _, _ := rt.Load(ctx, sid)
	if state.GetAddress().GetCountry() != "GB" {
		t.Errorf("Address not updated; got %+v", state.GetAddress())
	}
}

// ----- Status transitions ------------------------------------------------

func TestSuspend_BlocksMutations(t *testing.T) {
	rt := newRuntime(t)
	sid := register(t, rt, "p1")
	ctx := es.WithTenant(context.Background(), tenant)

	if _, err := rt.Handle(ctx, sid, &partyv1.Suspend{ActorId: "admin", Reason: "fraud review"}); err != nil {
		t.Fatalf("Suspend: %v", err)
	}

	_, err := rt.Handle(ctx, sid, &partyv1.UpdatePhone{NewPhone: "+1", ActorId: "self"})
	if !errors.Is(err, party.ErrNotActive) {
		t.Fatalf("UpdatePhone on suspended: expected ErrNotActive, got %v", err)
	}

	_, err = rt.Handle(ctx, sid, &partyv1.ProposeName{
		ChangeId: "c1", Proposed: &partyv1.Name{First: "X", Last: "Y"},
		ProposedBy: "m", Reason: "test",
	})
	if !errors.Is(err, party.ErrNotActive) {
		t.Fatalf("ProposeName on suspended: expected ErrNotActive, got %v", err)
	}
}

func TestReactivate_RestoresMutations(t *testing.T) {
	rt := newRuntime(t)
	sid := register(t, rt, "p1")
	ctx := es.WithTenant(context.Background(), tenant)

	_, _ = rt.Handle(ctx, sid, &partyv1.Suspend{ActorId: "admin", Reason: "review"})
	if _, err := rt.Handle(ctx, sid, &partyv1.Reactivate{ActorId: "admin", Comment: "cleared"}); err != nil {
		t.Fatalf("Reactivate: %v", err)
	}

	if _, err := rt.Handle(ctx, sid, &partyv1.UpdatePhone{NewPhone: "+1", ActorId: "self"}); err != nil {
		t.Errorf("UpdatePhone after reactivate should succeed: %v", err)
	}
}

func TestClose_ReleasesEmailClaim(t *testing.T) {
	rt := newRuntime(t)
	sid := register(t, rt, "p1")
	ctx := es.WithTenant(context.Background(), tenant)

	if _, err := rt.Handle(ctx, sid, &partyv1.Close{ActorId: "admin", Reason: "off-boarded"}); err != nil {
		t.Fatalf("Close: %v", err)
	}

	_, err := rt.Handle(ctx, sid, &partyv1.Reactivate{ActorId: "admin"})
	if !errors.Is(err, party.ErrNotSuspended) {
		t.Fatalf("Reactivate on closed: expected ErrNotSuspended, got %v", err)
	}

	sid2 := estest.MustStream(t, tenant, "party", "p2")
	_, err = rt.Handle(ctx, sid2, &partyv1.Register{
		Name:        &partyv1.Name{First: "B", Last: "C"},
		Email:       "p1@example.com",
		Address:     &partyv1.Address{Line1: "1", City: "X", Country: "US"},
		DateOfBirth: "1990-01-01",
		ActorId:     "system",
	})
	if err != nil {
		t.Fatalf("re-register with released email after close: %v", err)
	}
}
