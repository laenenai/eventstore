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

// ListStaleSubjectKeys implements shred.SubjectStore.
func (a *Adapter) ListStaleSubjectKeys(ctx context.Context, tenantID string, currentKEKVersion uint32, limit int) ([]shred.SubjectKey, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := a.queries.ListStaleSubjectKeys(ctx, db.ListStaleSubjectKeysParams{
		TenantID:          tenantID,
		CurrentKekVersion: int64(currentKEKVersion),
		MaxRows:           int64(limit),
	})
	if err != nil {
		return nil, err
	}
	out := make([]shred.SubjectKey, 0, len(rows))
	for _, r := range rows {
		created, err := parseTime(r.CreatedAt)
		if err != nil {
			return nil, err
		}
		sk := shred.SubjectKey{
			TenantID:   r.TenantID,
			Subject:    r.Subject,
			DEKWrapped: r.DekWrapped,
			KEKVersion: uint32(r.KekVersion),
			CreatedAt:  created,
		}
		if r.ShreddedAt != nil && *r.ShreddedAt != "" {
			t, err := parseTime(*r.ShreddedAt)
			if err != nil {
				return nil, err
			}
			sk.ShreddedAt = &t
		}
		out = append(out, sk)
	}
	return out, nil
}

var _ shred.SubjectStore = (*Adapter)(nil)
