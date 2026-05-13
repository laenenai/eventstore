package shred

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"fmt"
	"time"

	"github.com/laenenai/eventstore/es"
	"github.com/laenenai/eventstore/kms"
)

// SubjectStore is implemented by adapters that persist per-subject
// DEKs (ADR 0010). Encrypted DEKs are stored alongside the events;
// the framework's KeyStore unwraps them on demand.
type SubjectStore interface {
	// GetSubjectKey returns the wrapped DEK (and KEK version it was
	// wrapped under) for one (tenant, subject). Returns ErrSubjectKeyNotFound
	// when the subject has no DEK yet.
	GetSubjectKey(ctx context.Context, tenantID, subject string) (SubjectKey, error)

	// UpsertSubjectKey writes or replaces the wrapped DEK. Called on
	// first use (initial DEK generation) and during KEK rotation
	// (re-wrap of an existing DEK).
	UpsertSubjectKey(ctx context.Context, tenantID, subject string, dekWrapped []byte, kekVersion uint32) error

	// ForgetSubject crypto-shreds the subject by zeroing dek_wrapped
	// and setting shredded_at. The row is retained for compliance
	// audit.
	ForgetSubject(ctx context.Context, tenantID, subject string) error
}

// SubjectKey is one row of the subject_keys table — the wrapped DEK
// for a (tenant, subject). KEKVersion identifies the KEK used to wrap
// it (so rotation across many subjects can happen incrementally).
type SubjectKey struct {
	TenantID    string
	Subject     string
	DEKWrapped  []byte
	KEKVersion  uint32
	CreatedAt   time.Time
	ShreddedAt  *time.Time // nil unless the subject has been shredded
}

// ErrSubjectKeyNotFound signals no DEK exists yet for the subject.
// Callers should call Shredder.EnsureSubjectKey to lazily create one.
var ErrSubjectKeyNotFound = errors.New("shred: subject key not found")

// Shredder orchestrates encrypt/decrypt of PII fields and the
// shredding operator action. Combines a KeyStore (KEK custody) with a
// SubjectStore (DEK persistence). One Shredder per app; safe for
// concurrent use.
type Shredder struct {
	KMS      kms.KeyStore
	Store    SubjectStore

	// dekCache memoizes unwrapped DEKs by (tenant, subject) for the
	// process lifetime. Cleared via ClearCache(); operators typically
	// rebuild on KEK rotation. ADR 0010: "Hot read path: DEK fetched
	// from subject_keys, unwrapped via KMS once, cached".
	cache *dekCache
}

// New returns a ready-to-use Shredder.
func New(keyStore kms.KeyStore, subjectStore SubjectStore) *Shredder {
	return &Shredder{
		KMS:   keyStore,
		Store: subjectStore,
		cache: newDEKCache(),
	}
}

// EnsureSubjectKey returns the DEK for (tenant, subject), generating
// and persisting one if absent. The hot path uses the cache; only
// first-ever encryption for a subject touches the KMS and the
// subject_keys table.
func (s *Shredder) EnsureSubjectKey(ctx context.Context, tenantID, subject string) ([]byte, error) {
	if dek, ok := s.cache.get(tenantID, subject); ok {
		return dek, nil
	}

	row, err := s.Store.GetSubjectKey(ctx, tenantID, subject)
	if err == nil {
		if row.ShreddedAt != nil {
			return nil, fmt.Errorf("shred: subject %q has been shredded", subject)
		}
		dek, err := s.KMS.UnwrapDEK(ctx, tenantID, row.DEKWrapped, row.KEKVersion)
		if err != nil {
			return nil, fmt.Errorf("%w: unwrap DEK for %s: %v", es.ErrKMSUnavailable, subject, err)
		}
		s.cache.set(tenantID, subject, dek)
		return dek, nil
	}
	if !errors.Is(err, ErrSubjectKeyNotFound) {
		return nil, err
	}

	// Generate a fresh DEK, wrap, persist.
	dek := make([]byte, 32)
	if _, err := rand.Read(dek); err != nil {
		return nil, err
	}
	wrapped, kekVersion, err := s.KMS.WrapDEK(ctx, tenantID, dek)
	if err != nil {
		return nil, fmt.Errorf("%w: wrap DEK for %s: %v", es.ErrKMSUnavailable, subject, err)
	}
	if err := s.Store.UpsertSubjectKey(ctx, tenantID, subject, wrapped, kekVersion); err != nil {
		return nil, err
	}
	s.cache.set(tenantID, subject, dek)
	return dek, nil
}

// EncryptField encrypts plaintext under the subject's DEK. Output is
// the framework's per-field wire format:
//
//	version(1B) | iv(12B) | ciphertext | tag(16B)
//
// version=0x01 indicates AES-256-GCM. Future algorithms bump the
// version byte.
func (s *Shredder) EncryptField(ctx context.Context, tenantID, subject string, plaintext []byte) ([]byte, error) {
	dek, err := s.EnsureSubjectKey(ctx, tenantID, subject)
	if err != nil {
		return nil, err
	}
	return aesGCMSealField(dek, plaintext)
}

// DecryptField reverses EncryptField. Returns:
//   - plaintext, nil — success.
//   - nil, ErrShredded — the subject's DEK has been crypto-shredded.
//     Callers should substitute a RedactedValue at the call site.
//   - nil, es.ErrCryptoIntegrity — tag mismatch (corrupt or tampered).
//   - nil, other error — KMS unavailable or decode failure.
//
// The framework never returns ciphertext or a silent default.
func (s *Shredder) DecryptField(ctx context.Context, tenantID, subject string, sealed []byte) ([]byte, error) {
	if dek, ok := s.cache.get(tenantID, subject); ok {
		return aesGCMOpenField(dek, sealed)
	}

	row, err := s.Store.GetSubjectKey(ctx, tenantID, subject)
	if err != nil {
		if errors.Is(err, ErrSubjectKeyNotFound) {
			return nil, fmt.Errorf("shred: no DEK for subject %q", subject)
		}
		return nil, err
	}
	if row.ShreddedAt != nil {
		return nil, ErrShredded
	}
	dek, err := s.KMS.UnwrapDEK(ctx, tenantID, row.DEKWrapped, row.KEKVersion)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", es.ErrKMSUnavailable, err)
	}
	s.cache.set(tenantID, subject, dek)
	return aesGCMOpenField(dek, sealed)
}

// ForgetSubject executes the operator action — destroys the DEK,
// tombstones the subject_keys row, clears the local cache. All
// existing ciphertext for that subject becomes computationally
// inaccessible.
func (s *Shredder) ForgetSubject(ctx context.Context, tenantID, subject string) error {
	if err := s.Store.ForgetSubject(ctx, tenantID, subject); err != nil {
		return err
	}
	s.cache.forget(tenantID, subject)
	return nil
}

// ClearCache empties the in-memory DEK cache. Called after KEK rotation
// (so new wrappings stick) or for tests.
func (s *Shredder) ClearCache() { s.cache = newDEKCache() }

// ErrShredded signals that the subject has been crypto-shredded; the
// ciphertext is computationally inaccessible. Returned by
// DecryptField. Callers substitute a typed RedactedValue at the call
// site per ADR 0010.
var ErrShredded = errors.New("shred: subject has been shredded")

// Wire format version byte values.
const (
	wireV1AESGCM = 0x01
)

func aesGCMSealField(dek, plaintext []byte) ([]byte, error) {
	block, err := aes.NewCipher(dek)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	iv := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(iv); err != nil {
		return nil, err
	}
	out := make([]byte, 1, 1+len(iv)+len(plaintext)+gcm.Overhead())
	out[0] = wireV1AESGCM
	out = append(out, iv...)
	out = gcm.Seal(out, iv, plaintext, nil)
	return out, nil
}

func aesGCMOpenField(dek, sealed []byte) ([]byte, error) {
	if len(sealed) < 1 {
		return nil, fmt.Errorf("%w: empty ciphertext", es.ErrCryptoIntegrity)
	}
	if sealed[0] != wireV1AESGCM {
		return nil, fmt.Errorf("%w: unsupported wire version 0x%02x", es.ErrCryptoIntegrity, sealed[0])
	}
	block, err := aes.NewCipher(dek)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	body := sealed[1:]
	if len(body) < gcm.NonceSize()+gcm.Overhead() {
		return nil, fmt.Errorf("%w: ciphertext shorter than nonce+tag", es.ErrCryptoIntegrity)
	}
	iv := body[:gcm.NonceSize()]
	ct := body[gcm.NonceSize():]
	pt, err := gcm.Open(nil, iv, ct, nil)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", es.ErrCryptoIntegrity, err)
	}
	return pt, nil
}
