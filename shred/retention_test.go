package shred

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"
)

// fakeSubjectStore is an in-memory SubjectStore that also implements
// RetentionScanner. Kept minimal: no concurrency, no real crypto —
// just enough to exercise the worker's loop end-to-end against a
// deterministic dataset.
type fakeSubjectStore struct {
	mu   sync.Mutex
	rows map[string]map[string]*SubjectKey // tenant → subject → row
}

func newFakeSubjectStore() *fakeSubjectStore {
	return &fakeSubjectStore{rows: make(map[string]map[string]*SubjectKey)}
}

func (s *fakeSubjectStore) put(tenant, subject string, createdAt time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.rows[tenant]
	if !ok {
		t = make(map[string]*SubjectKey)
		s.rows[tenant] = t
	}
	t[subject] = &SubjectKey{
		TenantID:   tenant,
		Subject:    subject,
		DEKWrapped: []byte("fake"),
		KEKVersion: 1,
		CreatedAt:  createdAt,
	}
}

func (s *fakeSubjectStore) GetSubjectKey(_ context.Context, tenantID, subject string) (SubjectKey, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.rows[tenantID]
	if !ok {
		return SubjectKey{}, ErrSubjectKeyNotFound
	}
	row, ok := t[subject]
	if !ok {
		return SubjectKey{}, ErrSubjectKeyNotFound
	}
	return *row, nil
}

func (s *fakeSubjectStore) UpsertSubjectKey(_ context.Context, tenantID, subject string, dekWrapped []byte, kekVersion uint32) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.rows[tenantID]
	if !ok {
		t = make(map[string]*SubjectKey)
		s.rows[tenantID] = t
	}
	t[subject] = &SubjectKey{
		TenantID:   tenantID,
		Subject:    subject,
		DEKWrapped: dekWrapped,
		KEKVersion: kekVersion,
		CreatedAt:  time.Now(),
	}
	return nil
}

func (s *fakeSubjectStore) ForgetSubject(_ context.Context, tenantID, subject string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.rows[tenantID]
	if !ok {
		return nil
	}
	row, ok := t[subject]
	if !ok {
		return nil
	}
	row.DEKWrapped = nil
	now := time.Now()
	row.ShreddedAt = &now
	return nil
}

func (s *fakeSubjectStore) ListStaleSubjectKeys(_ context.Context, tenantID string, currentKEK uint32, limit int) ([]SubjectKey, error) {
	// Not exercised by retention tests.
	return nil, nil
}

func (s *fakeSubjectStore) ListSubjectsCreatedBefore(_ context.Context, tenantID string, cutoff time.Time, limit int) ([]SubjectKey, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.rows[tenantID]
	if !ok {
		return nil, nil
	}
	var out []SubjectKey
	for _, row := range t {
		if row.ShreddedAt != nil {
			continue
		}
		if row.CreatedAt.Before(cutoff) {
			out = append(out, *row)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Subject < out[j].Subject })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

// stubKMS is the minimum-viable kms.KeyStore that Shredder needs for
// ForgetSubject; the worker only calls Store.ForgetSubject via the
// Shredder, which doesn't touch KMS on the forget path.
type stubKMS struct{}

func (stubKMS) WrapDEK(_ context.Context, _ string, dek []byte) ([]byte, uint32, error) {
	return dek, 1, nil
}
func (stubKMS) UnwrapDEK(_ context.Context, _ string, w []byte, _ uint32) ([]byte, error) {
	return w, nil
}
func (stubKMS) CurrentKEKVersion(_ context.Context, _ string) (uint32, error) { return 1, nil }

func TestRetentionWorker_ShredsExpiredSubjects(t *testing.T) {
	store := newFakeSubjectStore()
	now := time.Date(2026, 6, 18, 12, 0, 0, 0, time.UTC)
	// Two tenants. Each has one expired and one fresh subject.
	store.put("tenant-a", "user-1-old", now.AddDate(-2, 0, 0))
	store.put("tenant-a", "user-2-fresh", now.AddDate(0, -1, 0))
	store.put("tenant-b", "user-3-old", now.AddDate(-3, 0, 0))
	store.put("tenant-b", "user-4-fresh", now.AddDate(0, 0, -3))

	shredder := New(stubKMS{}, store)

	var auditLog []string
	w := &RetentionWorker{
		Shredder: shredder,
		Tenants:  StaticTenants("tenant-a", "tenant-b"),
		MaxAge:   365 * 24 * time.Hour, // 1 year
		Clock:    func() time.Time { return now },
		OnShredded: func(_ context.Context, tenant, subject string) error {
			auditLog = append(auditLog, fmt.Sprintf("%s/%s", tenant, subject))
			return nil
		},
	}

	got, err := w.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if got != 2 {
		t.Fatalf("shredded count: got %d want 2", got)
	}

	// Fresh subjects must survive; expired subjects must be tombstoned.
	cases := []struct {
		tenant, subject string
		wantShredded    bool
	}{
		{"tenant-a", "user-1-old", true},
		{"tenant-a", "user-2-fresh", false},
		{"tenant-b", "user-3-old", true},
		{"tenant-b", "user-4-fresh", false},
	}
	for _, c := range cases {
		row := store.rows[c.tenant][c.subject]
		got := row.ShreddedAt != nil
		if got != c.wantShredded {
			t.Errorf("%s/%s: shredded=%v want %v", c.tenant, c.subject, got, c.wantShredded)
		}
	}

	wantAudit := []string{"tenant-a/user-1-old", "tenant-b/user-3-old"}
	if len(auditLog) != len(wantAudit) {
		t.Fatalf("audit log: got %d entries want %d", len(auditLog), len(wantAudit))
	}
	for i := range auditLog {
		if auditLog[i] != wantAudit[i] {
			t.Errorf("audit[%d]: got %q want %q", i, auditLog[i], wantAudit[i])
		}
	}
}

func TestRetentionWorker_RejectsMissingScanner(t *testing.T) {
	// A SubjectStore that doesn't implement RetentionScanner.
	store := &noScanStore{}
	shredder := New(stubKMS{}, store)
	w := &RetentionWorker{
		Shredder: shredder,
		Tenants:  StaticTenants("t"),
		MaxAge:   time.Hour,
	}
	_, err := w.RunOnce(context.Background())
	if !errors.Is(err, ErrRetentionScannerNotSupported) {
		t.Fatalf("RunOnce: got %v want ErrRetentionScannerNotSupported", err)
	}
}

type noScanStore struct{}

func (noScanStore) GetSubjectKey(context.Context, string, string) (SubjectKey, error) {
	return SubjectKey{}, ErrSubjectKeyNotFound
}
func (noScanStore) UpsertSubjectKey(context.Context, string, string, []byte, uint32) error {
	return nil
}
func (noScanStore) ForgetSubject(context.Context, string, string) error { return nil }
func (noScanStore) ListStaleSubjectKeys(context.Context, string, uint32, int) ([]SubjectKey, error) {
	return nil, nil
}

func TestRetentionWorker_Validation(t *testing.T) {
	shredder := New(stubKMS{}, newFakeSubjectStore())
	cases := []struct {
		name string
		w    RetentionWorker
	}{
		{"missing shredder", RetentionWorker{Tenants: StaticTenants("t"), MaxAge: time.Hour}},
		{"missing tenants", RetentionWorker{Shredder: shredder, MaxAge: time.Hour}},
		{"zero max age", RetentionWorker{Shredder: shredder, Tenants: StaticTenants("t")}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := c.w.RunOnce(context.Background())
			if err == nil {
				t.Fatalf("expected validation error")
			}
		})
	}
}

func TestRetentionWorker_TenantSourceFailureSurfaces(t *testing.T) {
	store := newFakeSubjectStore()
	shredder := New(stubKMS{}, store)
	sentinel := errors.New("upstream down")
	w := &RetentionWorker{
		Shredder: shredder,
		Tenants:  func(context.Context) ([]string, error) { return nil, sentinel },
		MaxAge:   time.Hour,
	}
	_, err := w.RunOnce(context.Background())
	if !errors.Is(err, sentinel) {
		t.Fatalf("RunOnce: got %v want chain containing sentinel", err)
	}
}
