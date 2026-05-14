package sqlite_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	_ "modernc.org/sqlite"

	sqliteadapter "github.com/laenenai/eventstore/adapters/storage/sqlite"
	"github.com/laenenai/eventstore/es"
	"github.com/laenenai/eventstore/adapters/kms/inproc"
	"github.com/laenenai/eventstore/shred"
)

// Crypto-shredding end-to-end on the SQLite adapter: encrypt under a
// subject's DEK, round-trip, shred, decrypt fails with ErrShredded.

func newShredder(t *testing.T) (*sqliteadapter.Adapter, *shred.Shredder) {
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
	return a, shred.New(inproc.New(), a)
}

func TestShred_RoundTrip(t *testing.T) {
	_, s := newShredder(t)
	ctx := context.Background()

	plaintext := []byte("alice@example.com")
	sealed, err := s.EncryptField(ctx, "t-shred", "alice", plaintext)
	if err != nil {
		t.Fatalf("EncryptField: %v", err)
	}
	if len(sealed) <= len(plaintext) {
		t.Errorf("sealed (%d) should be larger than plaintext (%d) — overhead missing",
			len(sealed), len(plaintext))
	}

	out, err := s.DecryptField(ctx, "t-shred", "alice", sealed)
	if err != nil {
		t.Fatalf("DecryptField: %v", err)
	}
	if string(out) != string(plaintext) {
		t.Errorf("round trip: got %q want %q", out, plaintext)
	}
}

func TestShred_DifferentSubjectsHaveDifferentDEKs(t *testing.T) {
	_, s := newShredder(t)
	ctx := context.Background()

	sealedAlice, _ := s.EncryptField(ctx, "t-shred", "alice", []byte("secret"))
	// Decrypting alice's ciphertext under bob's DEK should fail
	// (different DEKs → tag mismatch).
	if _, err := s.DecryptField(ctx, "t-shred", "bob", sealedAlice); err == nil {
		t.Errorf("cross-subject decryption should fail")
	}
}

func TestShred_ForgetSubject(t *testing.T) {
	_, s := newShredder(t)
	ctx := context.Background()

	sealed, err := s.EncryptField(ctx, "t-shred", "carol", []byte("secret"))
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}

	if err := s.ForgetSubject(ctx, "t-shred", "carol"); err != nil {
		t.Fatalf("ForgetSubject: %v", err)
	}

	_, err = s.DecryptField(ctx, "t-shred", "carol", sealed)
	if !errors.Is(err, shred.ErrShredded) {
		t.Errorf("expected ErrShredded after Forget, got %v", err)
	}
}

func TestShred_IntegrityFailure(t *testing.T) {
	_, s := newShredder(t)
	ctx := context.Background()

	sealed, _ := s.EncryptField(ctx, "t-shred", "dave", []byte("plaintext"))
	// Flip a byte in the ciphertext body (skip past version + IV).
	sealed[len(sealed)-1] ^= 0xff

	if _, err := s.DecryptField(ctx, "t-shred", "dave", sealed); !errors.Is(err, es.ErrCryptoIntegrity) {
		t.Errorf("expected ErrCryptoIntegrity on tampered ciphertext, got %v", err)
	}
}

func TestShred_UnsupportedWireVersionFails(t *testing.T) {
	_, s := newShredder(t)
	ctx := context.Background()
	sealed, _ := s.EncryptField(ctx, "t-shred", "eve", []byte("plaintext"))
	// Corrupt the version byte.
	sealed[0] = 0x99

	if _, err := s.DecryptField(ctx, "t-shred", "eve", sealed); !errors.Is(err, es.ErrCryptoIntegrity) {
		t.Errorf("expected ErrCryptoIntegrity on bad version byte, got %v", err)
	}
}
