package sqlite_test

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"os"
	"strings"
	"testing"

	_ "modernc.org/sqlite"

	sqliteadapter "github.com/laenenai/eventstore/adapters/storage/sqlite"
	"github.com/laenenai/eventstore/aggregate"
	"github.com/laenenai/eventstore/es"
	"github.com/laenenai/eventstore/estest"
	shredv1 "github.com/laenenai/eventstore/gen/test/shred/v1"
	"github.com/laenenai/eventstore/adapters/kms/inproc"
	"github.com/laenenai/eventstore/shred"
)

// End-to-end crypto-shredding: events flow through Handle (encrypt on
// disk) → Load (decrypt back to plaintext) → ForgetSubject (subsequent
// Load returns redacted fields).

// personDecider is a minimal Decider with the Person state proto.
var personDecider = es.Decider[*shredv1.Person, shredv1.Command, shredv1.Event]{
	Initial: func() *shredv1.Person { return &shredv1.Person{} },
	Decide: func(s *shredv1.Person, c shredv1.Command) ([]shredv1.Event, []es.ConstraintOp, error) {
		switch cmd := c.(type) {
		case *shredv1.Register:
			return []shredv1.Event{
				&shredv1.Registered{
					PersonId:    cmd.PersonId,
					DisplayName: cmd.DisplayName,
					Email:       cmd.Email,
				},
			}, nil, nil
		case *shredv1.UpdateName:
			return []shredv1.Event{
				&shredv1.NameChanged{
					PersonId:       s.PersonId,
					NewDisplayName: cmd.NewDisplayName,
				},
			}, nil, nil
		}
		return nil, nil, errors.New("person: unknown command")
	},
	Evolve: func(s *shredv1.Person, e shredv1.Event) *shredv1.Person {
		out := &shredv1.Person{PersonId: s.PersonId, DisplayName: s.DisplayName}
		switch evt := e.(type) {
		case *shredv1.Registered:
			out.PersonId = evt.PersonId
			out.DisplayName = evt.DisplayName
		case *shredv1.NameChanged:
			out.DisplayName = evt.NewDisplayName
		}
		return out
	},
}

func newShredRuntime(t *testing.T) (*sqliteadapter.Adapter, *aggregate.Runtime[*shredv1.Person, shredv1.Command, shredv1.Event]) {
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
	rt := &aggregate.Runtime[*shredv1.Person, shredv1.Command, shredv1.Event]{
		Store:    a,
		Decider:  personDecider,
		Codec:    shredv1.EventCodec{},
		Shredder: s,
	}
	return a, rt
}

// TestShred_E2E_EncryptOnDiskDecryptOnLoad confirms the round trip:
// Handle persists ciphertext, Load returns plaintext.
func TestShred_E2E_EncryptOnDiskDecryptOnLoad(t *testing.T) {
	a, rt := newShredRuntime(t)
	ctx := es.WithTenant(context.Background(), "t-pii")
	sid := estest.MustStream(t, "t-pii", "person", "alice")

	plain := []byte("Alice Smith")
	if _, err := rt.Handle(ctx, sid, &shredv1.Register{
		PersonId:    "alice",
		DisplayName: plain,
		Email:       []byte("alice@example.com"),
	}); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	// Inspect raw event payload on disk — should NOT contain plaintext.
	envs, err := a.ReadStream(context.Background(), sid, 0)
	if err != nil {
		t.Fatalf("ReadStream: %v", err)
	}
	if len(envs) != 1 {
		t.Fatalf("envs: got %d want 1", len(envs))
	}
	if bytes.Contains(envs[0].Payload, plain) {
		t.Errorf("raw payload contains plaintext display name — encryption did not happen")
	}

	// Load through the runtime — should decrypt transparently.
	state, _, err := rt.Load(context.Background(), sid)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if string(state.DisplayName) != string(plain) {
		t.Errorf("decrypted display name: got %q want %q", state.DisplayName, plain)
	}
}

// TestShred_E2E_ForgetSubjectRedactsOnLoad verifies that after
// ForgetSubject, the next Load reports the field as redacted.
func TestShred_E2E_ForgetSubjectRedactsOnLoad(t *testing.T) {
	_, rt := newShredRuntime(t)
	ctx := es.WithTenant(context.Background(), "t-forget")
	sid := estest.MustStream(t, "t-forget", "person", "bob")

	if _, err := rt.Handle(ctx, sid, &shredv1.Register{
		PersonId:    "bob",
		DisplayName: []byte("Bob Jones"),
		Email:       []byte("bob@example.com"),
	}); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	if err := rt.Shredder.ForgetSubject(context.Background(), "t-forget", "bob"); err != nil {
		t.Fatalf("ForgetSubject: %v", err)
	}

	var redacted shred.RedactedFields
	rt.OnRedacted = func(r shred.RedactedFields) { redacted = r }

	state, _, err := rt.Load(context.Background(), sid)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	// Display name and email fields are zeroed.
	if len(state.DisplayName) != 0 {
		t.Errorf("display name should be empty after shred, got %q", state.DisplayName)
	}
	// PersonId is non-PII (subject_field), should remain.
	if state.PersonId != "bob" {
		t.Errorf("subject field stayed plaintext: got %q want bob", state.PersonId)
	}
	// OnRedacted was called for both encrypted fields.
	if len(redacted) != 2 {
		t.Errorf("redacted entries: got %d want 2 (display_name + email)", len(redacted))
	}
	for _, r := range redacted {
		if r.Reason != "shredded" {
			t.Errorf("reason: got %q want shredded", r.Reason)
		}
		if r.Subject != "bob" {
			t.Errorf("subject: got %q want bob", r.Subject)
		}
	}
}

// TestShred_E2E_RebuildAfterShredYieldsRedacted verifies that the
// "shredded then re-run through Handle" path doesn't accidentally
// re-encrypt under a new DEK that masks the shred.
func TestShred_E2E_ShredPreventsFurtherWrites(t *testing.T) {
	_, rt := newShredRuntime(t)
	ctx := es.WithTenant(context.Background(), "t-prevent")
	sid := estest.MustStream(t, "t-prevent", "person", "carol")

	if _, err := rt.Handle(ctx, sid, &shredv1.Register{
		PersonId: "carol", DisplayName: []byte("Carol"), Email: []byte("c@example.com"),
	}); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if err := rt.Shredder.ForgetSubject(context.Background(), "t-prevent", "carol"); err != nil {
		t.Fatalf("ForgetSubject: %v", err)
	}

	// Subsequent Handle should fail because EnsureSubjectKey refuses
	// shredded subjects.
	_, err := rt.Handle(ctx, sid, &shredv1.UpdateName{NewDisplayName: []byte("Carol Renamed")})
	if err == nil {
		t.Errorf("expected Handle to fail after subject shredded")
	}
}

// TestShred_E2E_NonPIIFieldsStayPlaintext confirms that fields whose
// (es.v1.data_classification) is below PERSONAL (INTERNAL, PUBLIC,
// UNSPECIFIED) are not encrypted.
func TestShred_E2E_NonPIIFieldsStayPlaintext(t *testing.T) {
	a, rt := newShredRuntime(t)
	ctx := es.WithTenant(context.Background(), "t-nonpii")
	sid := estest.MustStream(t, "t-nonpii", "person", "dave")

	if _, err := rt.Handle(ctx, sid, &shredv1.Register{
		PersonId:    "dave",
		DisplayName: []byte("Dave"),
		Email:       []byte("dave@example.com"),
	}); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	envs, _ := a.ReadStream(context.Background(), sid, 0)
	// referrer_id is non_pii but we didn't set it. person_id (subject)
	// should appear plaintext in the wire payload.
	if !bytes.Contains(envs[0].Payload, []byte("dave")) {
		t.Errorf("subject_id (non-PII) should appear plaintext in payload")
	}
}

// TestShred_SADRejectedOnEncrypt verifies the codegen-emitted
// EncryptPII rejects any event variant carrying a
// DATA_CLASSIFICATION_SAD field (ADR 0027 + cookbook 11). The proto
// AuthorizedWithSAD has cvv = SAD; the generated method must return a
// typed error naming every SAD field, AND must not mutate the value
// (no partial encrypt).
//
// Bug demonstrated: before this fix, codegen mapped SAD to
// piiKindRejected in the manifest but emitPIIMethods only emitted
// piiKindBytes / piiKindString branches, leaving SAD fields to flow
// through as plaintext. Run this test against the un-fixed plugin and
// the generated EncryptPII returns nil with the cvv still set.
func TestShred_SADRejectedOnEncrypt(t *testing.T) {
	_, s := newShredder(t)
	ctx := context.Background()

	const plaintextCVV = "999"
	const plaintextAuth = "auth-12345"
	e := &shredv1.AuthorizedWithSAD{
		PersonId: "frank",
		AuthCode: plaintextAuth,
		Cvv:      plaintextCVV,
	}

	err := e.EncryptPII(ctx, s, "t-sad", "frank")
	if err == nil {
		t.Fatalf("EncryptPII on SAD-bearing event: want error, got nil (cvv would persist as plaintext)")
	}
	msg := err.Error()
	if !strings.Contains(msg, "SAD MUST NOT be persisted") {
		t.Errorf("error should announce SAD reject, got: %q", msg)
	}
	if !strings.Contains(msg, "cvv") {
		t.Errorf("error should name the SAD field, got: %q", msg)
	}
	// No partial-encrypt: auth_code (PERSONAL) must stay plaintext
	// because the reject fires before any field is touched. If the
	// fix regresses and SAD detection runs AFTER the per-field loop,
	// auth_code would already be base64-ciphertext at this point.
	if e.AuthCode != plaintextAuth {
		t.Errorf("auth_code mutated despite SAD reject: %q", e.AuthCode)
	}
	if e.Cvv != plaintextCVV {
		t.Errorf("cvv mutated despite SAD reject: %q", e.Cvv)
	}
}

// TestShred_SADRejectedOnDecrypt mirrors the encrypt path. If a SAD
// payload ever reached storage (despite the encrypt guard), reading
// it back is itself corruption (regulator-reportable). DecryptPII
// must return the same SAD reject error rather than silently decoding.
func TestShred_SADRejectedOnDecrypt(t *testing.T) {
	_, s := newShredder(t)
	ctx := context.Background()

	e := &shredv1.AuthorizedWithSAD{PersonId: "grace", AuthCode: "auth-xyz", Cvv: "777"}
	red, err := e.DecryptPII(ctx, s, "t-sad", "grace")
	if err == nil {
		t.Fatalf("DecryptPII on SAD-bearing event: want error, got nil")
	}
	if red != nil {
		t.Errorf("redacted fields should be nil on SAD reject, got %v", red)
	}
	if !strings.Contains(err.Error(), "SAD MUST NOT be persisted") {
		t.Errorf("error wording: got %q", err)
	}
	if !strings.Contains(err.Error(), "cvv") {
		t.Errorf("error should name SAD field cvv, got %q", err)
	}
}

// TestShred_SADManifestEntry confirms the per-aggregate
// pii_manifest.json continues to surface SAD fields as
// "encryption": "rejected_sad" so PCI auditors can grep for them.
// (The manifest is the contract that ADR 0027 promises.)
func TestShred_SADManifestEntry(t *testing.T) {
	// Hand-rolled manifest read so the test stays portable across
	// adapter modules (no encoding/json import gymnastics needed).
	b, err := os.ReadFile("../../../gen/test/shred/v1/shred_pii_manifest.json")
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	manifest := string(b)
	if !strings.Contains(manifest, `"name": "cvv"`) {
		t.Fatalf("manifest missing cvv entry:\n%s", manifest)
	}
	if !strings.Contains(manifest, `"classification": "DATA_CLASSIFICATION_SAD"`) {
		t.Fatalf("manifest missing SAD classification:\n%s", manifest)
	}
	if !strings.Contains(manifest, `"encryption": "rejected_sad"`) {
		t.Fatalf("manifest missing rejected_sad encryption marker:\n%s", manifest)
	}
}
