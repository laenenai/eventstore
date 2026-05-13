package sqlite

import (
	"context"
	"database/sql"
	"errors"

	"github.com/laenenai/eventstore/adapters/storage/sqlite/internal/db"
	"github.com/laenenai/eventstore/shred"
)

// GetSubjectKey implements shred.SubjectStore.
func (a *Adapter) GetSubjectKey(ctx context.Context, tenantID, subject string) (shred.SubjectKey, error) {
	row, err := a.queries.GetSubjectKey(ctx, db.GetSubjectKeyParams{
		TenantID: tenantID,
		Subject:  subject,
	})
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return shred.SubjectKey{}, shred.ErrSubjectKeyNotFound
		}
		return shred.SubjectKey{}, err
	}
	created, err := parseTime(row.CreatedAt)
	if err != nil {
		return shred.SubjectKey{}, err
	}
	out := shred.SubjectKey{
		TenantID:   row.TenantID,
		Subject:    row.Subject,
		DEKWrapped: row.DekWrapped,
		KEKVersion: uint32(row.KekVersion),
		CreatedAt:  created,
	}
	if row.ShreddedAt != nil && *row.ShreddedAt != "" {
		t, err := parseTime(*row.ShreddedAt)
		if err != nil {
			return shred.SubjectKey{}, err
		}
		out.ShreddedAt = &t
	}
	return out, nil
}

// UpsertSubjectKey implements shred.SubjectStore.
func (a *Adapter) UpsertSubjectKey(ctx context.Context, tenantID, subject string, dekWrapped []byte, kekVersion uint32) error {
	return a.queries.UpsertSubjectKey(ctx, db.UpsertSubjectKeyParams{
		TenantID:   tenantID,
		Subject:    subject,
		DekWrapped: dekWrapped,
		KekVersion: int64(kekVersion),
	})
}

// ForgetSubject implements shred.SubjectStore.
func (a *Adapter) ForgetSubject(ctx context.Context, tenantID, subject string) error {
	return a.queries.ForgetSubject(ctx, db.ForgetSubjectParams{
		TenantID: tenantID,
		Subject:  subject,
	})
}

var _ shred.SubjectStore = (*Adapter)(nil)
