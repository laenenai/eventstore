package employee_test

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"testing"

	_ "modernc.org/sqlite"

	sqliteadapter "github.com/laenenai/eventstore/adapters/storage/sqlite"
	"github.com/laenenai/eventstore/aggregate"
	"github.com/laenenai/eventstore/es"
	"github.com/laenenai/eventstore/examples/employee"
	employeev1 "github.com/laenenai/eventstore/gen/myapp/employee/v1"
	"github.com/laenenai/eventstore/adapters/kms/inproc"
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

	plainName := []byte("Alice Smith")
	plainEmail := []byte("alice@example.com")

	if _, err := rt.Handle(ctx, sid, &employeev1.Hire{
		EmployeeId:  "emp-42",
		LegalName:   plainName,
		Email:       plainEmail,
		DateOfBirth: []byte("1990-04-12"),
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
	if bytes.Contains(envs[0].Payload, plainName) {
		t.Errorf("raw payload leaks plaintext legal_name")
	}
	if bytes.Contains(envs[0].Payload, plainEmail) {
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
	if string(state.LegalName) != string(plainName) {
		t.Errorf("legal_name after decrypt: got %q want %q",
			state.LegalName, plainName)
	}
	if string(state.Email) != string(plainEmail) {
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
		LegalName:   []byte("Bob Jones"),
		Email:       []byte("bob@example.com"),
		DateOfBirth: []byte("1985-09-20"),
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

	if len(state.LegalName) != 0 {
		t.Errorf("legal_name should be empty after shred, got %q", state.LegalName)
	}
	if len(state.Email) != 0 {
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
		EmployeeId: "emp-7",
		LegalName:  []byte("Carol"),
		Email:      []byte("c@example.com"),
		DateOfBirth: []byte("1995-01-01"),
		Department: "ops",
		InitialRole: "sre-1",
	}); err != nil {
		t.Fatalf("Hire: %v", err)
	}
	if _, err := rt.Handle(ctx, sid, &employeev1.Terminate{
		Reason: []byte("voluntary departure"),
	}); err != nil {
		t.Fatalf("Terminate: %v", err)
	}

	_, err := rt.Handle(ctx, sid, &employeev1.Promote{NewRole: "sre-2"})
	if !errors.Is(err, es.ErrTerminal) {
		t.Errorf("post-terminate Promote: got %v want ErrTerminal", err)
	}
}
