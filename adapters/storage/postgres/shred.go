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
	var row db.SubjectKey
	err := a.withTenantTx(ctx, tenantID, func(q *db.Queries) error {
		var inner error
		row, inner = q.GetSubjectKey(ctx, db.GetSubjectKeyParams{
			TenantID: tenantID,
			Subject:  subject,
		})
		return inner
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
	return a.withTenantTx(ctx, tenantID, func(q *db.Queries) error {
		return q.UpsertSubjectKey(ctx, db.UpsertSubjectKeyParams{
			TenantID:   tenantID,
			Subject:    subject,
			DekWrapped: dekWrapped,
			KekVersion: int32(kekVersion),
		})
	})
}

// ForgetSubject implements shred.SubjectStore.
func (a *Adapter) ForgetSubject(ctx context.Context, tenantID, subject string) error {
	return a.withTenantTx(ctx, tenantID, func(q *db.Queries) error {
		return q.ForgetSubject(ctx, db.ForgetSubjectParams{
			TenantID: tenantID,
			Subject:  subject,
		})
	})
}

// ListStaleSubjectKeys implements shred.SubjectStore.
func (a *Adapter) ListStaleSubjectKeys(ctx context.Context, tenantID string, currentKEKVersion uint32, limit int) ([]shred.SubjectKey, error) {
	if limit <= 0 {
		limit = 100
	}
	var rows []db.SubjectKey
	err := a.withTenantTx(ctx, tenantID, func(q *db.Queries) error {
		var inner error
		rows, inner = q.ListStaleSubjectKeys(ctx, db.ListStaleSubjectKeysParams{
			TenantID:          tenantID,
			CurrentKekVersion: int32(currentKEKVersion),
			MaxRows:           int32(limit),
		})
		return inner
	})
	if err != nil {
		return nil, err
	}
	out := make([]shred.SubjectKey, 0, len(rows))
	for _, r := range rows {
		out = append(out, shred.SubjectKey{
			TenantID:   r.TenantID,
			Subject:    r.Subject,
			DEKWrapped: r.DekWrapped,
			KEKVersion: uint32(r.KekVersion),
			CreatedAt:  r.CreatedAt,
			ShreddedAt: r.ShreddedAt,
		})
	}
	return out, nil
}

var _ shred.SubjectStore = (*Adapter)(nil)
