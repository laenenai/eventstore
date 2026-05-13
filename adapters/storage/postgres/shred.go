package postgres

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"

	"github.com/laenenai/eventstore/adapters/storage/postgres/internal/db"
	"github.com/laenenai/eventstore/shred"
)

// GetSubjectKey implements shred.SubjectStore.
func (a *Adapter) GetSubjectKey(ctx context.Context, tenantID, subject string) (shred.SubjectKey, error) {
	row, err := a.queries.GetSubjectKey(ctx, db.GetSubjectKeyParams{
		TenantID: tenantID,
		Subject:  subject,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return shred.SubjectKey{}, shred.ErrSubjectKeyNotFound
		}
		return shred.SubjectKey{}, err
	}
	return shred.SubjectKey{
		TenantID:   row.TenantID,
		Subject:    row.Subject,
		DEKWrapped: row.DekWrapped,
		KEKVersion: uint32(row.KekVersion),
		CreatedAt:  row.CreatedAt,
		ShreddedAt: row.ShreddedAt,
	}, nil
}

// UpsertSubjectKey implements shred.SubjectStore.
func (a *Adapter) UpsertSubjectKey(ctx context.Context, tenantID, subject string, dekWrapped []byte, kekVersion uint32) error {
	return a.queries.UpsertSubjectKey(ctx, db.UpsertSubjectKeyParams{
		TenantID:   tenantID,
		Subject:    subject,
		DekWrapped: dekWrapped,
		KekVersion: int32(kekVersion),
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
