package employee_test

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"strings"
	"testing"

	_ "modernc.org/sqlite"

	"github.com/laenenai/eventstore/adapters/kms/inproc"
	sqliteadapter "github.com/laenenai/eventstore/adapters/storage/sqlite"
	"github.com/laenenai/eventstore/aggregate"
	"github.com/laenenai/eventstore/es"
	"github.com/laenenai/eventstore/examples/employee"
	employeev1 "github.com/laenenai/eventstore/gen/myapp/employee/v1"
	partyv1 "github.com/laenenai/eventstore/gen/myapp/party/v1"
	"github.com/laenenai/eventstore/shred"
)

// End-to-end crypto-shredding against the Employee aggregate.

func newRuntime(t *testing.T) (es.Store, *shred.Shredder,
	*aggregate.Runtime[*employeev1.Employee, employeev1.Command, employeev1.Event],
) {
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
	s := shred.New(inproc.New(), a)
	rt := &aggregate.Runtime[*employeev1.Employee, employeev1.Command, employeev1.Event]{
		Store:    a,
		Decider:  employee.Decider,
		Codec:    employeev1.EventCodec{},
		Shredder: s,
	}
	return a, s, rt
}

func mustStream(t *testing.T, tenant, id string) es.StreamID {
	t.Helper()
	sid, err := es.ParseCanonical(tenant, "employee:"+id)
	if err != nil {
		t.Fatalf("ParseCanonical: %v", err)
	}
	return sid
}

func TestEmployee_HireAndReadRoundTrip(t *testing.T) {
	store, _, rt := newRuntime(t)
	tenant := "t-hr"
	ctx := es.WithTenant(context.Background(), tenant)
	sid := mustStream(t, tenant, "emp-42")

	const plainName = "Alice Smith"
	const plainEmail = "alice@example.com"

	if _, err := rt.Handle(ctx, sid, &employeev1.Hire{
		EmployeeId:  "emp-42",
		LegalName:   plainName,
		Email:       plainEmail,
		DateOfBirth: "1990-04-12",
		Department:  "engineering",
		InitialRole: "swe-2",
	}); err != nil {
		t.Fatalf("Hire: %v", err)
	}

	// 1. On-disk bytes must NOT contain plaintext PII.
	envs, _ := store.ReadStream(context.Background(), sid, 0)
	if len(envs) != 1 {
		t.Fatalf("env count: got %d want 1", len(envs))
	}
	if bytes.Contains(envs[0].Payload, []byte(plainName)) {
		t.Errorf("raw payload leaks plaintext legal_name")
	}
	if bytes.Contains(envs[0].Payload, []byte(plainEmail)) {
		t.Errorf("raw payload leaks plaintext email")
	}
	// Non-PII fields stay plaintext.
	if !bytes.Contains(envs[0].Payload, []byte("engineering")) {
		t.Errorf("non-PII department should be plaintext in payload")
	}

	// 2. Load through the runtime — DecryptPII restores plaintext.
	state, _, err := rt.Load(context.Background(), sid)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if state.LegalName != plainName {
		t.Errorf("legal_name after decrypt: got %q want %q",
			state.LegalName, plainName)
	}
	if state.Email != plainEmail {
		t.Errorf("email after decrypt: got %q want %q",
			state.Email, plainEmail)
	}
	if state.Department != "engineering" {
		t.Errorf("department: got %q want engineering", state.Department)
	}
}

func TestEmployee_ForgetSubjectRedactsOnLoad(t *testing.T) {
	_, shredder, rt := newRuntime(t)
	tenant := "t-forget"
	ctx := es.WithTenant(context.Background(), tenant)
	sid := mustStream(t, tenant, "emp-99")

	if _, err := rt.Handle(ctx, sid, &employeev1.Hire{
		EmployeeId:  "emp-99",
		LegalName:   "Bob Jones",
		Email:       "bob@example.com",
		DateOfBirth: "1985-09-20",
		Department:  "sales",
		InitialRole: "ae-1",
	}); err != nil {
		t.Fatalf("Hire: %v", err)
	}
	if _, err := rt.Handle(ctx, sid, &employeev1.Promote{NewRole: "ae-2"}); err != nil {
		t.Fatalf("Promote: %v", err)
	}

	// GDPR-style deletion.
	if err := shredder.ForgetSubject(context.Background(), tenant, "emp-99"); err != nil {
		t.Fatalf("ForgetSubject: %v", err)
	}

	var redacted shred.RedactedFields
	rt.OnRedacted = func(r shred.RedactedFields) { redacted = r }

	state, _, err := rt.Load(context.Background(), sid)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if state.LegalName != "" {
		t.Errorf("legal_name should be empty after shred, got %q", state.LegalName)
	}
	if state.Email != "" {
		t.Errorf("email should be empty after shred")
	}
	// Non-PII fields and the subject_field stay readable forever.
	if state.EmployeeId != "emp-99" {
		t.Errorf("subject_field gone: %q", state.EmployeeId)
	}
	if state.Department != "sales" {
		t.Errorf("non-PII department lost: %q", state.Department)
	}
	if state.CurrentRole != "ae-2" {
		t.Errorf("non-PII role lost: %q", state.CurrentRole)
	}

	// Hired had 3 PII fields; we expect 3 redaction entries.
	if len(redacted) != 3 {
		t.Errorf("redaction entries: got %d want 3", len(redacted))
	}
	for _, r := range redacted {
		if r.Reason != "shredded" {
			t.Errorf("reason: got %q want shredded", r.Reason)
		}
		if r.Subject != "emp-99" {
			t.Errorf("subject: got %q want emp-99", r.Subject)
		}
	}
}

func TestEmployee_TerminatedIsTerminal(t *testing.T) {
	_, _, rt := newRuntime(t)
	tenant := "t-term"
	ctx := es.WithTenant(context.Background(), tenant)
	sid := mustStream(t, tenant, "emp-7")

	if _, err := rt.Handle(ctx, sid, &employeev1.Hire{
		EmployeeId:  "emp-7",
		LegalName:   "Carol",
		Email:       "c@example.com",
		DateOfBirth: "1995-01-01",
		Department:  "ops",
		InitialRole: "sre-1",
	}); err != nil {
		t.Fatalf("Hire: %v", err)
	}
	if _, err := rt.Handle(ctx, sid, &employeev1.Terminate{
		Reason: "voluntary departure",
	}); err != nil {
		t.Fatalf("Terminate: %v", err)
	}

	_, err := rt.Handle(ctx, sid, &employeev1.Promote{NewRole: "sre-2"})
	if !errors.Is(err, es.ErrTerminal) {
		t.Errorf("post-terminate Promote: got %v want ErrTerminal", err)
	}
}

// ============================================================================
// Access-level View() + LogValue() — ADR 0027 data-governance helpers
// ============================================================================

func TestHired_ViewRedactsPIIBelowCustomer(t *testing.T) {
	e := &employeev1.Hired{
		EmployeeId:  "emp-42",
		LegalName:   "Alice Smith",
		Email:       "alice@example.com",
		DateOfBirth: "1990-04-12",
		Department:  "engineering",
		InitialRole: "swe-2",
	}

	// At AccessLevelInternal, PERSONAL + QUASI_IDENTIFIER fields
	// are zero-valued. Subject + INTERNAL-classified fields stay.
	view := e.View(es.AccessLevelInternal)
	if view.EmployeeId != "emp-42" {
		t.Errorf("subject field must always be visible, got %q", view.EmployeeId)
	}
	if view.LegalName != "" {
		t.Errorf("legal_name (PERSONAL) must be redacted at Internal, got %q", view.LegalName)
	}
	if view.Email != "" {
		t.Errorf("email (PERSONAL) must be redacted at Internal, got %q", view.Email)
	}
	if view.DateOfBirth != "" {
		t.Errorf("date_of_birth (QUASI_IDENTIFIER) must be redacted at Internal, got %q", view.DateOfBirth)
	}
	if view.Department != "engineering" {
		t.Errorf("department (INTERNAL) should be visible at Internal, got %q", view.Department)
	}
	if view.InitialRole != "swe-2" {
		t.Errorf("initial_role (INTERNAL) should be visible at Internal, got %q", view.InitialRole)
	}

	// At AccessLevelCustomer the PII fields come back.
	cust := e.View(es.AccessLevelCustomer)
	if cust.LegalName != "Alice Smith" {
		t.Errorf("legal_name at Customer: got %q want Alice Smith", cust.LegalName)
	}
	if cust.Email != "alice@example.com" {
		t.Errorf("email at Customer: got %q", cust.Email)
	}
	if cust.DateOfBirth != "1990-04-12" {
		t.Errorf("date_of_birth at Customer: got %q", cust.DateOfBirth)
	}

	// AccessLevelPublic strips everything classified, leaves subject.
	pub := e.View(es.AccessLevelPublic)
	if pub.EmployeeId != "emp-42" {
		t.Errorf("subject should remain at Public, got %q", pub.EmployeeId)
	}
	if pub.Department != "" {
		t.Errorf("department (INTERNAL) must be redacted at Public, got %q", pub.Department)
	}
	if pub.LegalName != "" {
		t.Errorf("legal_name must be redacted at Public")
	}

	// Original untouched (deep copy).
	if e.LegalName != "Alice Smith" {
		t.Errorf("View must not mutate source; got %q", e.LegalName)
	}
}

func TestHired_LogValueRedactsPIIByDefault(t *testing.T) {
	e := &employeev1.Hired{
		EmployeeId:  "emp-42",
		LegalName:   "Alice Smith",
		Email:       "alice@example.com",
		DateOfBirth: "1990-04-12",
		Department:  "engineering",
		InitialRole: "swe-2",
	}

	buf := &bytes.Buffer{}
	logger := slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	logger.Info("hired", "event", e)

	out := buf.String()

	// Subject + INTERNAL fields visible.
	if !strings.Contains(out, "emp-42") {
		t.Errorf("subject must appear in log line: %s", out)
	}
	if !strings.Contains(out, "engineering") {
		t.Errorf("department (INTERNAL) must appear in log line: %s", out)
	}
	if !strings.Contains(out, "swe-2") {
		t.Errorf("initial_role (INTERNAL) must appear in log line: %s", out)
	}

	// PII fields must be redacted markers, never plaintext.
	if strings.Contains(out, "Alice Smith") {
		t.Errorf("legal_name plaintext leaked into log: %s", out)
	}
	if strings.Contains(out, "alice@example.com") {
		t.Errorf("email plaintext leaked into log: %s", out)
	}
	if strings.Contains(out, "1990-04-12") {
		t.Errorf("date_of_birth plaintext leaked into log: %s", out)
	}

	if !strings.Contains(out, "[REDACTED:PERSONAL]") {
		t.Errorf("expected [REDACTED:PERSONAL] marker, got: %s", out)
	}
	if !strings.Contains(out, "[REDACTED:QUASI_IDENTIFIER]") {
		t.Errorf("expected [REDACTED:QUASI_IDENTIFIER] marker, got: %s", out)
	}
}

func TestEmployeeState_ViewAndLogValue(t *testing.T) {
	// State messages also get View/LogValue (codegen walks every
	// emittable message, not only event variants).
	s := &employeev1.Employee{
		EmployeeId:  "emp-99",
		LegalName:   "Bob",
		Email:       "bob@example.com",
		DateOfBirth: "1985-09-20",
		Department:  "sales",
		CurrentRole: "ae-2",
		Status:      employeev1.Status_STATUS_ACTIVE,
	}
	v := s.View(es.AccessLevelInternal)
	if v.EmployeeId != "emp-99" {
		t.Errorf("state subject lost: %q", v.EmployeeId)
	}
	if v.LegalName != "" {
		t.Errorf("state PII not redacted at Internal: %q", v.LegalName)
	}
	if v.Department != "sales" {
		t.Errorf("state INTERNAL field lost: %q", v.Department)
	}

	// Nil safety.
	var nilState *employeev1.Employee
	if got := nilState.View(es.AccessLevelOperator); got != nil {
		t.Errorf("View on nil should return nil, got %v", got)
	}

	// LogValue on State.
	logBuf := &bytes.Buffer{}
	logger := slog.New(slog.NewTextHandler(logBuf, nil))
	logger.Info("state", "employee", s)
	if strings.Contains(logBuf.String(), "Bob") {
		t.Errorf("state log leaked legal_name: %s", logBuf.String())
	}
}

func TestPartyClone_DeepCopiesOneofMessageVariant(t *testing.T) {
	// PendingChange has a oneof "proposed" with both message
	// (Name) and scalar (string) variants. Verifies that:
	//   - The message variant is deep-cloned, not aliased.
	//   - The scalar variant is copied by value.
	//   - The repeated []PendingChange on Party is fully independent.
	orig := &partyv1.Party{
		PartyId: "p-123",
		Name:    &partyv1.Name{First: "Alice", Last: "Smith"},
		Email:   "alice@example.com",
		PendingChanges: []*partyv1.PendingChange{
			{
				ChangeId:   "ch-1",
				ProposedBy: "u-1",
				Reason:     "rename",
				Proposed:   &partyv1.PendingChange_Name{Name: &partyv1.Name{First: "Alex", Last: "Smith"}},
			},
			{
				ChangeId:   "ch-2",
				ProposedBy: "u-2",
				Reason:     "email change",
				Proposed:   &partyv1.PendingChange_Email{Email: "alex@example.com"},
			},
		},
	}

	clone := orig.Clone()

	// Slice equality by value, independence by reference.
	if len(clone.PendingChanges) != 2 {
		t.Fatalf("clone.PendingChanges len: got %d want 2", len(clone.PendingChanges))
	}
	if clone.PendingChanges[0] == orig.PendingChanges[0] {
		t.Errorf("PendingChange[0] aliased — slice not deep-cloned")
	}

	// Message-variant oneof: must be a new pointer, same value.
	originalProposedName := orig.PendingChanges[0].Proposed.(*partyv1.PendingChange_Name)
	clonedProposedName := clone.PendingChanges[0].Proposed.(*partyv1.PendingChange_Name)
	if clonedProposedName == originalProposedName {
		t.Errorf("oneof wrapper aliased — should be new struct")
	}
	if clonedProposedName.Name == originalProposedName.Name {
		t.Errorf("oneof inner message aliased — deep clone failed for oneof")
	}
	if clonedProposedName.Name.First != "Alex" {
		t.Errorf("inner Name lost value: %q", clonedProposedName.Name.First)
	}

	// Scalar-variant oneof: value preserved.
	clonedProposedEmail := clone.PendingChanges[1].Proposed.(*partyv1.PendingChange_Email)
	if clonedProposedEmail.Email != "alex@example.com" {
		t.Errorf("scalar oneof variant lost value: %q", clonedProposedEmail.Email)
	}

	// Singular nested message (Name): deep cloned.
	if clone.Name == orig.Name {
		t.Errorf("singular nested Name aliased")
	}

	// Mutate clone, source untouched.
	clonedProposedName.Name.First = "Different"
	if originalProposedName.Name.First != "Alex" {
		t.Errorf("source oneof inner mutated by clone: %q", originalProposedName.Name.First)
	}
}

func TestEmployee_CloneIsDeepCopy(t *testing.T) {
	orig := &employeev1.Hired{
		EmployeeId:  "emp-42",
		LegalName:   "Alice Smith",
		Email:       "alice@example.com",
		DateOfBirth: "1990-04-12",
		Department:  "engineering",
		InitialRole: "swe-2",
	}

	clone := orig.Clone()

	// Equal in value.
	if clone.LegalName != orig.LegalName {
		t.Errorf("clone.LegalName: got %q want %q", clone.LegalName, orig.LegalName)
	}
	if clone.EmployeeId != orig.EmployeeId {
		t.Errorf("clone.EmployeeId mismatch")
	}

	// Independent: mutate clone, source unchanged.
	clone.LegalName = "Bob Different"
	clone.Department = "marketing"
	if orig.LegalName != "Alice Smith" {
		t.Errorf("source mutated by clone change: %q", orig.LegalName)
	}
	if orig.Department != "engineering" {
		t.Errorf("source department mutated: %q", orig.Department)
	}

	// Nil safety.
	var nilHired *employeev1.Hired
	if got := nilHired.Clone(); got != nil {
		t.Errorf("Clone on nil should return nil, got %v", got)
	}
}

func TestEmployee_AccessLevelOrdering(t *testing.T) {
	if es.AccessLevelPublic >= es.AccessLevelInternal {
		t.Errorf("Public should be below Internal")
	}
	if es.AccessLevelInternal >= es.AccessLevelCustomer {
		t.Errorf("Internal should be below Customer")
	}
	if es.AccessLevelCustomer >= es.AccessLevelCompliance {
		t.Errorf("Customer should be below Compliance")
	}
	if es.AccessLevelCompliance >= es.AccessLevelOperator {
		t.Errorf("Compliance should be below Operator")
	}
}
