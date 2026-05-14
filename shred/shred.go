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

	// ListStaleSubjectKeys returns up to limit non-shredded
	// subject_keys rows for tenantID that were wrapped under an
	// older KEK version. Used by Shredder.RewrapDEKs to migrate
	// historical DEKs after a KEK rotation. Rows are ordered by
	// subject for stable pagination.
	ListStaleSubjectKeys(ctx context.Context, tenantID string, currentKEKVersion uint32, limit int) ([]SubjectKey, error)
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

// RewrapDEKs migrates every non-shredded subject_keys row for tenantID
// from its current kek_version to the KeyStore's current version. ADR
// 0010: KEK rotation produces a new key version; existing DEKs keep
// working under their stored version, but it's an operator's job to
// re-wrap them so an old KEK can eventually be retired.
//
// The function paginates through stale rows in batches of pageSize,
// unwraps each DEK under its existing version, re-wraps under the new
// current version, and upserts. Returns the total number of rows
// rewrapped. Safe to interrupt and resume — partial progress survives.
//
// pageSize ≤ 0 defaults to 100.
func (s *Shredder) RewrapDEKs(ctx context.Context, tenantID string, pageSize int) (int, error) {
	if pageSize <= 0 {
		pageSize = 100
	}
	current, err := s.KMS.CurrentKEKVersion(ctx, tenantID)
	if err != nil {
		return 0, fmt.Errorf("shred: read current KEK version: %w", err)
	}
	if current == 0 {
		return 0, nil // no KEK yet — nothing to rotate
	}

	total := 0
	for {
		stale, err := s.Store.ListStaleSubjectKeys(ctx, tenantID, current, pageSize)
		if err != nil {
			return total, fmt.Errorf("shred: list stale subject keys: %w", err)
		}
		if len(stale) == 0 {
			return total, nil
		}
		for _, row := range stale {
			dek, err := s.KMS.UnwrapDEK(ctx, tenantID, row.DEKWrapped, row.KEKVersion)
			if err != nil {
				return total, fmt.Errorf("shred: unwrap DEK for %s: %w", row.Subject, err)
			}
			wrapped, version, err := s.KMS.WrapDEK(ctx, tenantID, dek)
			if err != nil {
				return total, fmt.Errorf("shred: re-wrap DEK for %s: %w", row.Subject, err)
			}
			if err := s.Store.UpsertSubjectKey(ctx, tenantID, row.Subject, wrapped, version); err != nil {
				return total, fmt.Errorf("shred: upsert re-wrapped DEK for %s: %w", row.Subject, err)
			}
			total++
		}
		// If we read fewer rows than pageSize the next iteration
		// will see no stale rows and exit. We could break here to
		// save one query; we don't, to keep the termination
		// condition unambiguous.
	}
}

// ErrShredded signals that the subject has been crypto-shredded; the
// ciphertext is computationally inaccessible. Returned by
// DecryptField. Codegen-emitted DecryptPII methods convert this into
// a RedactedField entry rather than failing the whole decode.
var ErrShredded = errors.New("shred: subject has been shredded")

// PIIEncoder is implemented by codegen-emitted event types that carry
// at least one encrypted field (a non-subject field whose
// (es.v1.data_classification) is PERSONAL or stricter — see ADR 0027
// for the full classification matrix). aggregate.Runtime auto-detects
// this interface and calls EncryptPII/DecryptPII when Runtime.Shredder
// is configured. See ADR 0010.
type PIIEncoder interface {
	// PIIFields returns the field names this event carries under
	// encryption — the ones EncryptPII/DecryptPII walk. Stable
	// across versions of the codegen output; used for audit
	// reporting and tests.
	PIIFields() []string

	// Subject returns the default subject id used to key the DEK
	// for this event's fields. Sourced from the field marked
	// (es.v1.subject_field) on the variant; empty when not set —
	// the framework falls back to the StreamID's identifier.
	Subject() string

	// EncryptPII encrypts every PII field in place using the
	// supplied Shredder. Already-empty fields are skipped. Called
	// by the framework before Codec.Encode.
	EncryptPII(ctx context.Context, s *Shredder, tenantID, subject string) error

	// DecryptPII reverses EncryptPII. When the subject has been
	// shredded, the affected field is zeroed and a RedactedField
	// entry is added rather than failing the whole decode; other
	// errors (KMS unavailable, tag mismatch) abort.
	DecryptPII(ctx context.Context, s *Shredder, tenantID, subject string) (RedactedFields, error)
}

// RedactedField records one encrypted field that could not be
// decrypted for an event read. The field's bytes have been zeroed
// by DecryptPII; consumers branch on Reason to decide UX (typically
// "shredded" means GDPR-deletion has run and the value is gone).
type RedactedField struct {
	Name    string
	Subject string
	Reason  string // "shredded", "missing_key", "kms_unavailable"
}

// RedactedFields is the per-event list of redactions. nil/empty means
// every PII field decrypted successfully (the common path).
type RedactedFields []RedactedField

// Has reports whether any field in the slice matches name.
func (rs RedactedFields) Has(name string) bool {
	for _, r := range rs {
		if r.Name == name {
			return true
		}
	}
	return false
}

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
