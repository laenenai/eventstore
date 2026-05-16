package piimigrationv1_test

import (
	"context"
	"testing"

	"google.golang.org/protobuf/proto"

	kmsinproc "github.com/laenenai/eventstore/adapters/kms/inproc"
	"github.com/laenenai/eventstore/estest"
	piimigrationv1 "github.com/laenenai/eventstore/gen/test/piimigration/v1"
	legacyv1 "github.com/laenenai/eventstore/gen/test/piimigration/v1/legacy"
	"github.com/laenenai/eventstore/shred"
)

// ---- Setup -----------------------------------------------------------
//
// Same wiring shape as gen/test/encspec/v1/encrypt_test.go: in-process
// KMS plus the shared estest.MemSubjectStore. See ADR 0010 and
// cookbook recipe 11 — the SubjectStore is a tenant+subject-keyed
// map; the KMS lazily creates one AES-256 KEK per tenant.

func newShredder() *shred.Shredder {
	return shred.New(kmsinproc.New(), estest.NewMemSubjectStore())
}

// TestMigratingCodec_UpcastsLegacyHiredAndRoundTripsPlaintext walks
// the full fixture from cookbook recipe 11 § "PII shape migration":
//
//  1. Encrypt two plaintext fields under the LEGACY shape via the
//     same Shredder the runtime would use — the v1 events on disk
//     held raw ciphertext bytes in `legal_name` and `email`.
//  2. Marshal the legacy proto to bytes — this simulates an event
//     written before the bytes→string migration.
//  3. Pipe the bytes through MigratingCodec.Decode(typeURL, 1, ...) —
//     dispatches to the upcaster, which base64-encodes the
//     ciphertext into the current-shape string fields.
//  4. DecryptPII against the same shredder — the codegen-emitted
//     method base64-decodes, hits the per-subject DEK, and writes
//     plaintext back into the fields.
//  5. Assert plaintext matches.
//
// If any step fails the recipe pattern doesn't compile, the upcaster
// is broken, or the DEK identity between v1 EncryptField and the
// codegen-emitted DecryptPII has drifted.
func TestMigratingCodec_UpcastsLegacyHiredAndRoundTripsPlaintext(t *testing.T) {
	ctx := context.Background()
	s := newShredder()
	const (
		tenant     = "tenant-acme"
		subject    = "emp-42"
		legalName  = "Alice Example"
		email      = "alice@example.com"
	)

	// Step 1: encrypt plaintext under the LEGACY (v1) shape.
	// EncryptField returns the framework's per-field wire format:
	//   version(1B) | iv(12B) | ciphertext | tag(16B)
	// — exactly the bytes the pre-migration runtime stuffed into the
	// `bytes` field on HiredV1.
	legalCT, err := s.EncryptField(ctx, tenant, subject, []byte(legalName))
	if err != nil {
		t.Fatalf("EncryptField legal_name: %v", err)
	}
	emailCT, err := s.EncryptField(ctx, tenant, subject, []byte(email))
	if err != nil {
		t.Fatalf("EncryptField email: %v", err)
	}

	// Step 2: marshal HiredV1 → bytes (the on-disk payload for a v1
	// event).
	v1Event := &legacyv1.HiredV1{
		EmployeeId: subject,
		LegalName:  legalCT,
		Email:      emailCT,
	}
	payload, err := proto.Marshal(v1Event)
	if err != nil {
		t.Fatalf("proto.Marshal HiredV1: %v", err)
	}

	// Step 3: decode through the migrating codec at schema_version=1.
	codec := piimigrationv1.MigratingCodec{Inner: piimigrationv1.EventCodec{}}
	decoded, err := codec.Decode("test.piimigration.v1.Hired", 1, payload)
	if err != nil {
		t.Fatalf("MigratingCodec.Decode v1: %v", err)
	}
	hired, ok := decoded.(*piimigrationv1.Hired)
	if !ok {
		t.Fatalf("upcaster returned wrong type: got %T want *Hired", decoded)
	}

	// Sanity: the upcast yielded base64 strings, not raw bytes (the
	// recipe says "doesn't decrypt — bytes are still ciphertext, just
	// re-encoded"). A trivial way to spot a bug is to check we no
	// longer see the original plaintext in the field.
	if hired.LegalName == legalName || hired.Email == email {
		t.Fatalf("upcaster leaked plaintext into the current shape: %+v", hired)
	}
	if hired.LegalName == "" || hired.Email == "" {
		t.Fatalf("upcaster zeroed a non-empty field: %+v", hired)
	}
	if hired.EmployeeId != subject {
		t.Errorf("employee_id mismatch: got %q want %q", hired.EmployeeId, subject)
	}

	// Step 4: DecryptPII — same shredder, same per-subject DEK.
	redacted, err := hired.DecryptPII(ctx, s, tenant, subject)
	if err != nil {
		t.Fatalf("Hired.DecryptPII after upcast: %v", err)
	}
	if len(redacted) != 0 {
		t.Errorf("upcast round-trip should produce no redactions, got %v", redacted)
	}

	// Step 5: plaintext matches.
	if hired.LegalName != legalName {
		t.Errorf("legal_name after decrypt: got %q want %q", hired.LegalName, legalName)
	}
	if hired.Email != email {
		t.Errorf("email after decrypt: got %q want %q", hired.Email, email)
	}
}

// TestMigratingCodec_PassesThroughCurrentSchemaUnchanged verifies the
// upcaster only kicks in for legacy versions. At schema_version=2 the
// payload is the current shape and Decode must delegate to the
// codegen-emitted EventCodec verbatim.
func TestMigratingCodec_PassesThroughCurrentSchemaUnchanged(t *testing.T) {
	codec := piimigrationv1.MigratingCodec{Inner: piimigrationv1.EventCodec{}}

	// Build a current-shape Hired. Note: we encode the proto raw
	// here — no encryption — so the test stays focused on the codec
	// dispatch contract. Crypto round-trip is covered separately.
	orig := &piimigrationv1.Hired{
		EmployeeId: "emp-1",
		LegalName:  "base64-shaped-string",
		Email:      "another-base64-shaped-string",
	}
	payload, err := proto.Marshal(orig)
	if err != nil {
		t.Fatalf("proto.Marshal Hired: %v", err)
	}

	decoded, err := codec.Decode("test.piimigration.v1.Hired", 2, payload)
	if err != nil {
		t.Fatalf("MigratingCodec.Decode v2: %v", err)
	}
	got, ok := decoded.(*piimigrationv1.Hired)
	if !ok {
		t.Fatalf("decode returned wrong type: got %T want *Hired", decoded)
	}
	if got.EmployeeId != orig.EmployeeId || got.LegalName != orig.LegalName || got.Email != orig.Email {
		t.Errorf("v2 passthrough mutated event: got %+v want %+v", got, orig)
	}
}

// TestMigratingCodec_HandlesEmptyFields proves the upcaster preserves
// "unset" semantics. v1 events with empty bytes fields must produce
// v2 events with empty string fields — no spurious base64-of-zero-
// bytes encoding, which would defeat the codegen-emitted DecryptPII
// (it skips empty strings).
func TestMigratingCodec_HandlesEmptyFields(t *testing.T) {
	v1 := &legacyv1.HiredV1{
		EmployeeId: "emp-7",
		// LegalName + Email left as nil/empty bytes
	}
	payload, err := proto.Marshal(v1)
	if err != nil {
		t.Fatalf("proto.Marshal HiredV1: %v", err)
	}
	codec := piimigrationv1.MigratingCodec{Inner: piimigrationv1.EventCodec{}}
	decoded, err := codec.Decode("test.piimigration.v1.Hired", 1, payload)
	if err != nil {
		t.Fatalf("MigratingCodec.Decode v1 empty: %v", err)
	}
	hired := decoded.(*piimigrationv1.Hired)
	if hired.EmployeeId != "emp-7" {
		t.Errorf("employee_id mismatch: got %q want %q", hired.EmployeeId, "emp-7")
	}
	if hired.LegalName != "" {
		t.Errorf("legal_name should be empty after upcast of empty bytes, got %q", hired.LegalName)
	}
	if hired.Email != "" {
		t.Errorf("email should be empty after upcast of empty bytes, got %q", hired.Email)
	}
}

// TestMigratingCodec_UnknownTypeURL ensures unrecognized type URLs
// fall through to the inner codec, which produces the canonical
// "unknown type_url" error. The wrapper shouldn't swallow that path
// — adopters want a single, predictable error surface.
func TestMigratingCodec_UnknownTypeURL(t *testing.T) {
	codec := piimigrationv1.MigratingCodec{Inner: piimigrationv1.EventCodec{}}

	// At schema_version=1 the wrapper's switch doesn't match the
	// unknown URL and falls through to the inner codec.
	_, err := codec.Decode("test.piimigration.v1.Unknown", 1, []byte("ignored"))
	if err == nil {
		t.Fatal("expected error for unknown type_url at v1, got nil")
	}

	// At schema_version=2 the wrapper short-circuits to the inner
	// codec; same error contract.
	_, err = codec.Decode("test.piimigration.v1.Unknown", 2, []byte("ignored"))
	if err == nil {
		t.Fatal("expected error for unknown type_url at v2, got nil")
	}
}
