package estest

import (
	"context"
	"sync"
	"time"

	"github.com/laenenai/eventstore/shred"
)

// MemSubjectStore is a tenant+subject-keyed in-memory implementation of
// shred.SubjectStore for tests. Production adapters (postgres, sqlite)
// provide a durable equivalent — see adapters/storage/*/shred.go. Tests
// that exercise codegen-emitted EncryptPII/DecryptPII or upcasters that
// re-encode ciphertext only need correctness at this layer, not
// durability, so the implementation is deliberately small.
//
// Safe for concurrent use. ForgetSubject zeroes DEKWrapped and stamps
// ShreddedAt — DecryptField on a forgotten subject returns
// shred.ErrShredded; the row is retained for compliance audit.
type MemSubjectStore struct {
	mu   sync.Mutex
	rows map[string]shred.SubjectKey // key: tenant|subject
}

// NewMemSubjectStore returns an empty MemSubjectStore ready to plug
// into shred.New.
func NewMemSubjectStore() *MemSubjectStore {
	return &MemSubjectStore{rows: map[string]shred.SubjectKey{}}
}

func memSubjectStoreKey(tenant, subject string) string {
	return tenant + "|" + subject
}

// GetSubjectKey implements shred.SubjectStore.
func (s *MemSubjectStore) GetSubjectKey(_ context.Context, tenantID, subject string) (shred.SubjectKey, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	row, ok := s.rows[memSubjectStoreKey(tenantID, subject)]
	if !ok {
		return shred.SubjectKey{}, shred.ErrSubjectKeyNotFound
	}
	return row, nil
}

// UpsertSubjectKey implements shred.SubjectStore.
func (s *MemSubjectStore) UpsertSubjectKey(_ context.Context, tenantID, subject string, dekWrapped []byte, kekVersion uint32) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rows[memSubjectStoreKey(tenantID, subject)] = shred.SubjectKey{
		TenantID:   tenantID,
		Subject:    subject,
		DEKWrapped: append([]byte(nil), dekWrapped...),
		KEKVersion: kekVersion,
		CreatedAt:  time.Now().UTC(),
	}
	return nil
}

// ForgetSubject implements shred.SubjectStore.
func (s *MemSubjectStore) ForgetSubject(_ context.Context, tenantID, subject string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	row, ok := s.rows[memSubjectStoreKey(tenantID, subject)]
	if !ok {
		return shred.ErrSubjectKeyNotFound
	}
	now := time.Now().UTC()
	row.DEKWrapped = nil
	row.ShreddedAt = &now
	s.rows[memSubjectStoreKey(tenantID, subject)] = row
	return nil
}

// ListStaleSubjectKeys implements shred.SubjectStore. The in-memory
// store doesn't track KEK rotation, so RewrapDEKs-style tests must
// supply their own list (or extend this type if real rotation
// coverage is needed). Returns (nil, nil) by default — adequate for
// the encryption-round-trip tests that motivated this helper.
func (s *MemSubjectStore) ListStaleSubjectKeys(_ context.Context, _ string, _ uint32, _ int) ([]shred.SubjectKey, error) {
	return nil, nil
}

// Compile-time check.
var _ shred.SubjectStore = (*MemSubjectStore)(nil)
