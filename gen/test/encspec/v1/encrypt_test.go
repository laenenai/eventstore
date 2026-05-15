package encspecv1_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	kmsinproc "github.com/laenenai/eventstore/adapters/kms/inproc"
	pb "github.com/laenenai/eventstore/gen/test/encspec/v1"
	"github.com/laenenai/eventstore/shred"
)

// ---- In-memory SubjectStore ------------------------------------------
//
// shred.Shredder needs a SubjectStore implementation. Storage adapters
// (sqlite, postgres) provide one in production; tests against the
// codegen-emitted EncryptPII/DecryptPII methods only need correctness
// at this layer, not durability. Keep it minimal: tenant+subject keyed
// map, no concurrency theatrics, supports ForgetSubject by storing
// shredded_at.

type memSubjectStore struct {
	mu   sync.Mutex
	rows map[string]shred.SubjectKey // key: tenant|subject
}

func newMemSubjectStore() *memSubjectStore {
	return &memSubjectStore{rows: map[string]shred.SubjectKey{}}
}

func sskey(tenant, subject string) string { return tenant + "|" + subject }

func (s *memSubjectStore) GetSubjectKey(_ context.Context, tenantID, subject string) (shred.SubjectKey, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	row, ok := s.rows[sskey(tenantID, subject)]
	if !ok {
		return shred.SubjectKey{}, shred.ErrSubjectKeyNotFound
	}
	return row, nil
}

func (s *memSubjectStore) UpsertSubjectKey(_ context.Context, tenantID, subject string, dekWrapped []byte, kekVersion uint32) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rows[sskey(tenantID, subject)] = shred.SubjectKey{
		TenantID:   tenantID,
		Subject:    subject,
		DEKWrapped: append([]byte(nil), dekWrapped...),
		KEKVersion: kekVersion,
		CreatedAt:  time.Now().UTC(),
	}
	return nil
}

func (s *memSubjectStore) ForgetSubject(_ context.Context, tenantID, subject string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	row, ok := s.rows[sskey(tenantID, subject)]
	if !ok {
		return shred.ErrSubjectKeyNotFound
	}
	now := time.Now().UTC()
	row.DEKWrapped = nil
	row.ShreddedAt = &now
	s.rows[sskey(tenantID, subject)] = row
	return nil
}

func (s *memSubjectStore) ListStaleSubjectKeys(_ context.Context, _ string, _ uint32, _ int) ([]shred.SubjectKey, error) {
	return nil, nil // unused in these tests
}

// ---- Helpers ----------------------------------------------------------

func newShredder() *shred.Shredder {
	return shred.New(kmsinproc.New(), newMemSubjectStore())
}

func cleanPIISample() *pb.CleanPII {
	return &pb.CleanPII{
		SubjectId:        "subj-1",
		SPublic:          "pub",
		SPublicExplicit:  "pub-x",
		SInternal:        "int",
		IPublic:          1,
		IInternal:        2,
		SPersonal:        "alice@example.com",
		SQuasi:           "1990-04-12",
		SSensitive:       "blood-type:A+",
		SFinancial:       "1000",
		SCardholder:      "4111111111111111",
		SCredential:      "bcrypt$xxxx",
		SUnstructured:    "free-form note with maybe PII",
		BPersonal:        []byte("biometric-template-bytes"),
		BCredential:      []byte("token-bytes"),
	}
}

// ---- Round-trip: every classification path ---------------------------

func TestEncryptDecrypt_RoundTrip(t *testing.T) {
	s := newShredder()
	ctx := context.Background()
	const tenant, subject = "t-1", "subj-1"

	plain := cleanPIISample()
	encrypted := cleanPIISample() // identical sample; we'll encrypt this one

	// Encrypt.
	if err := encrypted.EncryptPII(ctx, s, tenant, subject); err != nil {
		t.Fatalf("EncryptPII: %v", err)
	}

	// PII fields differ from plaintext.
	if encrypted.SPersonal == plain.SPersonal {
		t.Errorf("s_personal not encrypted: still %q", encrypted.SPersonal)
	}
	if string(encrypted.BPersonal) == string(plain.BPersonal) {
		t.Errorf("b_personal not encrypted")
	}
	if string(encrypted.BCredential) == string(plain.BCredential) {
		t.Errorf("b_credential not encrypted")
	}

	// Non-PII fields untouched.
	if encrypted.SubjectId != plain.SubjectId {
		t.Errorf("subject_id mutated: %q vs %q", encrypted.SubjectId, plain.SubjectId)
	}
	if encrypted.SPublic != plain.SPublic {
		t.Errorf("PUBLIC mutated: %q vs %q", encrypted.SPublic, plain.SPublic)
	}
	if encrypted.SPublicExplicit != plain.SPublicExplicit {
		t.Errorf("explicit-PUBLIC mutated")
	}
	if encrypted.SInternal != plain.SInternal {
		t.Errorf("INTERNAL mutated: %q vs %q", encrypted.SInternal, plain.SInternal)
	}
	if encrypted.IPublic != plain.IPublic || encrypted.IInternal != plain.IInternal {
		t.Errorf("scalar int fields mutated")
	}

	// Decrypt.
	redacted, err := encrypted.DecryptPII(ctx, s, tenant, subject)
	if err != nil {
		t.Fatalf("DecryptPII: %v", err)
	}
	if len(redacted) != 0 {
		t.Errorf("clean roundtrip should have no redactions, got %d", len(redacted))
	}

	// Every classified field should now match plaintext.
	checks := map[string][2]string{
		"s_personal":     {encrypted.SPersonal, plain.SPersonal},
		"s_quasi":        {encrypted.SQuasi, plain.SQuasi},
		"s_sensitive":    {encrypted.SSensitive, plain.SSensitive},
		"s_financial":    {encrypted.SFinancial, plain.SFinancial},
		"s_cardholder":   {encrypted.SCardholder, plain.SCardholder},
		"s_credential":   {encrypted.SCredential, plain.SCredential},
		"s_unstructured": {encrypted.SUnstructured, plain.SUnstructured},
	}
	for field, pair := range checks {
		if pair[0] != pair[1] {
			t.Errorf("%s after decrypt: got %q want %q", field, pair[0], pair[1])
		}
	}
	if string(encrypted.BPersonal) != string(plain.BPersonal) {
		t.Errorf("b_personal after decrypt: got %q want %q", encrypted.BPersonal, plain.BPersonal)
	}
	if string(encrypted.BCredential) != string(plain.BCredential) {
		t.Errorf("b_credential after decrypt: got %q want %q", encrypted.BCredential, plain.BCredential)
	}
}

// ---- Encrypt is deterministic-shaped: strings base64; bytes raw -----

func TestEncryptPII_StringFieldsBase64Encoded(t *testing.T) {
	s := newShredder()
	e := cleanPIISample()
	if err := e.EncryptPII(context.Background(), s, "t-1", "subj-1"); err != nil {
		t.Fatalf("EncryptPII: %v", err)
	}
	// base64 alphabet: A-Z a-z 0-9 + / (no padding for RawStdEncoding)
	// Plus '_' / '-' for URL variant. Strict: characters MUST be in
	// [A-Za-z0-9+/]. No literal commas/spaces from the plaintext.
	for _, field := range []string{
		e.SPersonal, e.SQuasi, e.SSensitive,
		e.SFinancial, e.SCardholder, e.SCredential, e.SUnstructured,
	} {
		if !isBase64(field) {
			t.Errorf("string PII field not base64-encoded: %q", field)
		}
	}
}

func TestEncryptPII_BytesFieldsAreRawCiphertext(t *testing.T) {
	s := newShredder()
	e := cleanPIISample()
	origLen := len(e.BPersonal)
	if err := e.EncryptPII(context.Background(), s, "t-1", "subj-1"); err != nil {
		t.Fatalf("EncryptPII: %v", err)
	}
	// AEAD wraps add a fixed overhead (nonce + tag) — ciphertext is
	// >= plaintext + 16 bytes (AES-GCM 12-byte nonce + 16-byte tag,
	// implementations vary). At minimum it must be different and
	// non-empty.
	if len(e.BPersonal) == 0 {
		t.Errorf("b_personal ciphertext empty")
	}
	if len(e.BPersonal) == origLen && string(e.BPersonal[:origLen]) == "biometric-template-bytes" {
		t.Errorf("b_personal ciphertext equals plaintext")
	}
}

// ---- SAD reject ------------------------------------------------------

func TestEncryptPII_SADBlocked(t *testing.T) {
	s := newShredder()
	e := &pb.SADBlocked{
		SubjectId: "subj-1",
		SPersonal: "should-not-be-touched",
		SSad:      "CVV-1234",
	}
	originalPersonal := e.SPersonal
	originalSAD := e.SSad

	err := e.EncryptPII(context.Background(), s, "t-1", "subj-1")
	if err == nil {
		t.Fatal("EncryptPII on SAD-tainted message must return error")
	}
	if !strings.Contains(err.Error(), "SAD MUST NOT be persisted") {
		t.Errorf("error should mention SAD: %v", err)
	}
	if !strings.Contains(err.Error(), "s_sad") {
		t.Errorf("error should name the offending field: %v", err)
	}
	// Neither field touched — the reject happens BEFORE any encryption.
	if e.SPersonal != originalPersonal {
		t.Errorf("s_personal was mutated despite SAD reject: %q", e.SPersonal)
	}
	if e.SSad != originalSAD {
		t.Errorf("s_sad was mutated: %q", e.SSad)
	}
}

func TestDecryptPII_SADBlockedAlsoRejects(t *testing.T) {
	// A SAD-tainted message that somehow reached storage should fail
	// loud on read too — same reject path on DecryptPII.
	s := newShredder()
	e := &pb.SADBlocked{
		SubjectId: "subj-1",
		SPersonal: "garbage",
		SSad:      "garbage",
	}
	_, err := e.DecryptPII(context.Background(), s, "t-1", "subj-1")
	if err == nil {
		t.Fatal("DecryptPII on SAD must return error")
	}
	if !strings.Contains(err.Error(), "SAD") {
		t.Errorf("error should mention SAD: %v", err)
	}
}

// ---- Multi-subject: per-field subject override -----------------------

func TestEncryptDecrypt_MultiSubject(t *testing.T) {
	s := newShredder()
	ctx := context.Background()
	const tenant = "t-1"
	const fromUser, toUser = "alice", "bob"

	e := &pb.MultiSubject{
		FromUserId:  fromUser,
		ToUserId:    toUser,
		AmountCents: 12_500,
		FromNote:    "sending rent",
		ToNote:      "received rent payment",
	}
	clean := e.Clone() // use codegen-emitted Clone (proto messages contain a mutex)

	if err := e.EncryptPII(ctx, s, tenant, fromUser); err != nil {
		t.Fatalf("EncryptPII: %v", err)
	}

	// Each note is encrypted under a different subject's DEK; both
	// look like base64 now.
	if e.FromNote == clean.FromNote {
		t.Errorf("from_note not encrypted")
	}
	if e.ToNote == clean.ToNote {
		t.Errorf("to_note not encrypted")
	}
	if e.FromNote == e.ToNote {
		t.Errorf("from_note and to_note ciphertexts identical — same DEK used?")
	}

	// FINANCIAL int64 — never encrypted (not string/bytes).
	if e.AmountCents != clean.AmountCents {
		t.Errorf("amount_cents int mutated: %d vs %d", e.AmountCents, clean.AmountCents)
	}

	// Decrypt — restores both fields. The subject param to DecryptPII
	// is the fallback subject, but per-field overrides win.
	redacted, err := e.DecryptPII(ctx, s, tenant, fromUser)
	if err != nil {
		t.Fatalf("DecryptPII: %v", err)
	}
	if len(redacted) != 0 {
		t.Errorf("clean roundtrip should have no redactions: %v", redacted)
	}
	if e.FromNote != clean.FromNote {
		t.Errorf("from_note after decrypt: %q vs %q", e.FromNote, clean.FromNote)
	}
	if e.ToNote != clean.ToNote {
		t.Errorf("to_note after decrypt: %q vs %q", e.ToNote, clean.ToNote)
	}
}

// ---- ForgetSubject: post-shred decrypt → RedactedField, not error ----

func TestDecryptPII_AfterForgetSubject(t *testing.T) {
	s := newShredder()
	ctx := context.Background()
	const tenant, subject = "t-1", "subj-1"

	e := cleanPIISample()
	if err := e.EncryptPII(ctx, s, tenant, subject); err != nil {
		t.Fatalf("EncryptPII: %v", err)
	}

	// Operator action: destroy the DEK.
	if err := s.ForgetSubject(ctx, tenant, subject); err != nil {
		t.Fatalf("ForgetSubject: %v", err)
	}
	// Clear the in-process cache so subsequent reads hit the now-shredded
	// row.
	s.ClearCache()

	redacted, err := e.DecryptPII(ctx, s, tenant, subject)
	if err != nil {
		t.Fatalf("DecryptPII after shred must not fail; got %v", err)
	}
	if len(redacted) == 0 {
		t.Fatal("DecryptPII after shred should return RedactedField entries")
	}
	// One redaction entry per PII field. CleanPII has 9 classified
	// fields (7 strings + 2 bytes). All should be redacted.
	if len(redacted) != 9 {
		t.Errorf("expected 9 redactions, got %d: %v", len(redacted), redacted)
	}
	for _, r := range redacted {
		if r.Reason != "shredded" {
			t.Errorf("redaction reason: got %q want shredded", r.Reason)
		}
		if r.Subject != subject {
			t.Errorf("redaction subject: got %q want %q", r.Subject, subject)
		}
	}

	// Every PII field is now zero-valued.
	if e.SPersonal != "" || e.SCredential != "" || e.SCardholder != "" {
		t.Errorf("string PII fields not zeroed after shred")
	}
	if len(e.BPersonal) != 0 || len(e.BCredential) != 0 {
		t.Errorf("bytes PII fields not zeroed after shred")
	}

	// Non-PII fields untouched.
	if e.SubjectId != "subj-1" {
		t.Errorf("subject_id zeroed after shred: %q", e.SubjectId)
	}
	if e.SPublic == "" || e.SInternal == "" {
		t.Errorf("non-PII fields zeroed after shred")
	}
}

// ---- Cross-tenant isolation -----------------------------------------

func TestDecryptPII_WrongTenantCannotDecrypt(t *testing.T) {
	// Encrypt under tenant A; try to decrypt under tenant B.
	// The wrong-tenant decrypt either:
	//   - Returns ErrSubjectKeyNotFound (no DEK exists for that tenant), OR
	//   - Fails AEAD tag verification if a DEK for the same subject
	//     happens to exist under a different KEK.
	// Either way: the bytes don't come back as plaintext.
	s := newShredder()
	ctx := context.Background()

	e := cleanPIISample()
	if err := e.EncryptPII(ctx, s, "tenant-a", "subj-1"); err != nil {
		t.Fatalf("encrypt A: %v", err)
	}
	encryptedPersonal := e.SPersonal // record before attempting wrong-tenant decrypt

	_, err := e.DecryptPII(ctx, s, "tenant-b", "subj-1")
	if err == nil {
		t.Fatal("DecryptPII under wrong tenant should fail")
	}

	// Field still in its sealed form (decryption didn't accidentally
	// commit half a result).
	if e.SPersonal != encryptedPersonal {
		t.Errorf("encrypted field clobbered by failed cross-tenant decrypt")
	}
}

// ---- Empty fields skipped --------------------------------------------

func TestEncryptPII_EmptyFieldsSkipped(t *testing.T) {
	s := newShredder()
	e := &pb.CleanPII{SubjectId: "subj-1"} // every PII field empty

	if err := e.EncryptPII(context.Background(), s, "t-1", "subj-1"); err != nil {
		t.Fatalf("EncryptPII on empty: %v", err)
	}
	// All PII fields still empty post-encrypt — no spurious "encrypt empty
	// string" calls that would produce a meaningless ciphertext.
	if e.SPersonal != "" || len(e.BPersonal) != 0 {
		t.Errorf("empty PII fields should stay empty after EncryptPII")
	}
}

// ---- Tamper detection (AEAD) ----------------------------------------

func TestDecryptPII_TamperedCiphertextDetected(t *testing.T) {
	s := newShredder()
	ctx := context.Background()
	const tenant, subject = "t-1", "subj-1"

	e := cleanPIISample()
	if err := e.EncryptPII(ctx, s, tenant, subject); err != nil {
		t.Fatalf("EncryptPII: %v", err)
	}
	// Tamper: flip a byte in the encrypted bytes field. AEAD tag
	// verification should reject.
	if len(e.BPersonal) > 0 {
		e.BPersonal[0] ^= 0xFF
	}

	_, err := e.DecryptPII(ctx, s, tenant, subject)
	if err == nil {
		t.Error("DecryptPII should detect tampered ciphertext")
	}
	if errors.Is(err, shred.ErrShredded) {
		t.Errorf("tamper should not be reported as 'shredded': %v", err)
	}
}

// ---- isBase64 helper -------------------------------------------------

func isBase64(s string) bool {
	if s == "" {
		return false
	}
	for _, c := range s {
		switch {
		case c >= 'A' && c <= 'Z',
			c >= 'a' && c <= 'z',
			c >= '0' && c <= '9',
			c == '+', c == '/', c == '=':
			continue
		default:
			return false
		}
	}
	return true
}
