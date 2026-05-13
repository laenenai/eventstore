package sqlite_test

import (
	"context"
	"database/sql"
	"testing"

	_ "modernc.org/sqlite"

	sqliteadapter "github.com/laenenai/eventstore/adapters/storage/sqlite"
	"github.com/laenenai/eventstore/kms/inproc"
	"github.com/laenenai/eventstore/shred"
)

// TestShred_RewrapDEKs_AfterRotation verifies the operator workflow:
// generate some DEKs under KEK v1, rotate to v2, run RewrapDEKs,
// confirm all rows now reference v2 and still decrypt correctly.
func TestShred_RewrapDEKs_AfterRotation(t *testing.T) {
	db, _ := sql.Open("sqlite", ":memory:")
	t.Cleanup(func() { _ = db.Close() })
	a := sqliteadapter.New(db)
	if err := a.Migrate(context.Background()); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	ks := inproc.New()
	s := shred.New(ks, a)
	ctx := context.Background()

	// Encrypt under three subjects — DEKs auto-generated under KEK v1.
	subjects := []string{"alice", "bob", "carol"}
	plain := map[string][]byte{
		"alice": []byte("alice@example.com"),
		"bob":   []byte("bob@example.com"),
		"carol": []byte("carol@example.com"),
	}
	sealed := map[string][]byte{}
	for _, sub := range subjects {
		out, err := s.EncryptField(ctx, "t-rot", sub, plain[sub])
		if err != nil {
			t.Fatalf("encrypt %s: %v", sub, err)
		}
		sealed[sub] = out
	}

	// Rotate KEK → v2. Existing DEKs still reference v1.
	newVer, err := ks.RotateKEK(ctx, "t-rot")
	if err != nil {
		t.Fatalf("RotateKEK: %v", err)
	}
	if newVer != 2 {
		t.Fatalf("new KEK version: got %d want 2", newVer)
	}

	// Sanity: before rewrap, alice's subject_key is still kek_version=1.
	row, _ := a.GetSubjectKey(ctx, "t-rot", "alice")
	if row.KEKVersion != 1 {
		t.Fatalf("pre-rewrap KEK version: got %d want 1", row.KEKVersion)
	}

	// Clear the DEK cache so the next decrypt round-trips through KMS.
	s.ClearCache()

	// Run rotation.
	n, err := s.RewrapDEKs(ctx, "t-rot", 0)
	if err != nil {
		t.Fatalf("RewrapDEKs: %v", err)
	}
	if n != 3 {
		t.Errorf("rewrapped count: got %d want 3", n)
	}

	// All subject_keys rows now reference v2.
	for _, sub := range subjects {
		row, err := a.GetSubjectKey(ctx, "t-rot", sub)
		if err != nil {
			t.Fatalf("GetSubjectKey %s: %v", sub, err)
		}
		if row.KEKVersion != 2 {
			t.Errorf("%s KEK version after rewrap: got %d want 2", sub, row.KEKVersion)
		}
	}

	// And decryption still works for ciphertext created under v1.
	s.ClearCache()
	for _, sub := range subjects {
		pt, err := s.DecryptField(ctx, "t-rot", sub, sealed[sub])
		if err != nil {
			t.Fatalf("DecryptField %s after rewrap: %v", sub, err)
		}
		if string(pt) != string(plain[sub]) {
			t.Errorf("%s decrypted: got %q want %q", sub, pt, plain[sub])
		}
	}

	// Second rewrap is a no-op — everything's at v2.
	n2, err := s.RewrapDEKs(ctx, "t-rot", 0)
	if err != nil {
		t.Fatalf("RewrapDEKs second pass: %v", err)
	}
	if n2 != 0 {
		t.Errorf("second rewrap rewrote %d (expected 0 — already current)", n2)
	}
}

// TestShred_RewrapDEKs_SkipsShredded confirms forgotten subjects are
// left alone by the rotation job — their DEKs are gone and must stay
// gone.
func TestShred_RewrapDEKs_SkipsShredded(t *testing.T) {
	db, _ := sql.Open("sqlite", ":memory:")
	t.Cleanup(func() { _ = db.Close() })
	a := sqliteadapter.New(db)
	if err := a.Migrate(context.Background()); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	ks := inproc.New()
	s := shred.New(ks, a)
	ctx := context.Background()

	if _, err := s.EncryptField(ctx, "t-rot-skip", "alice", []byte("data")); err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	if _, err := s.EncryptField(ctx, "t-rot-skip", "bob", []byte("data")); err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	if err := s.ForgetSubject(ctx, "t-rot-skip", "alice"); err != nil {
		t.Fatalf("ForgetSubject: %v", err)
	}

	if _, err := ks.RotateKEK(ctx, "t-rot-skip"); err != nil {
		t.Fatalf("RotateKEK: %v", err)
	}

	n, err := s.RewrapDEKs(ctx, "t-rot-skip", 0)
	if err != nil {
		t.Fatalf("RewrapDEKs: %v", err)
	}
	// Only bob rewrapped; alice's shredded row was skipped.
	if n != 1 {
		t.Errorf("rewrapped: got %d want 1 (alice should be skipped — shredded)", n)
	}
}
