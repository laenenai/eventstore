package postgres

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/laenenai/eventstore/adapters/storage/postgres/internal/db"
	"github.com/laenenai/eventstore/es"
)

// ReadStream returns events for a stream with version > fromVersion,
// ordered ascending. fromVersion=0 returns all events.
func (a *Adapter) ReadStream(ctx context.Context, sid es.StreamID, fromVersion uint64) ([]es.Envelope, error) {
	if err := sid.Validate(); err != nil {
		return nil, err
	}
	rows, err := a.queries.ReadStreamFromVersion(ctx, db.ReadStreamFromVersionParams{
		TenantID:     sid.Tenant,
		StreamID:     sid.Canonical(),
		AfterVersion: int64(fromVersion),
	})
	if err != nil {
		return nil, err
	}
	out := make([]es.Envelope, 0, len(rows))
	for _, r := range rows {
		env, err := rowToEnvelope(r)
		if err != nil {
			return nil, err
		}
		out = append(out, env)
	}
	return out, nil
}

// ReadAll returns events store-wide with global_position > fromPosition,
// limited to `limit` rows.
func (a *Adapter) ReadAll(ctx context.Context, fromPosition uint64, limit int) ([]es.Envelope, error) {
	rows, err := a.queries.ReadAllFromPosition(ctx, db.ReadAllFromPositionParams{
		AfterPosition: int64(fromPosition),
		MaxRows:       int32(limit),
	})
	if err != nil {
		return nil, err
	}
	out := make([]es.Envelope, 0, len(rows))
	for _, r := range rows {
		env, err := rowToEnvelope(r)
		if err != nil {
			return nil, err
		}
		out = append(out, env)
	}
	return out, nil
}

// ReadAllForTenant is ReadAll scoped to a single tenant.
func (a *Adapter) ReadAllForTenant(ctx context.Context, tenantID string, fromPosition uint64, limit int) ([]es.Envelope, error) {
	rows, err := a.queries.ReadAllFromPositionTenant(ctx, db.ReadAllFromPositionTenantParams{
		TenantID:      tenantID,
		AfterPosition: int64(fromPosition),
		MaxRows:       int32(limit),
	})
	if err != nil {
		return nil, err
	}
	out := make([]es.Envelope, 0, len(rows))
	for _, r := range rows {
		env, err := rowToEnvelope(r)
		if err != nil {
			return nil, err
		}
		out = append(out, env)
	}
	return out, nil
}

// CurrentStreamVersion returns the highest committed version for a
// stream, or 0 if empty.
func (a *Adapter) CurrentStreamVersion(ctx context.Context, sid es.StreamID) (uint64, error) {
	if err := sid.Validate(); err != nil {
		return 0, err
	}
	v, err := a.queries.CurrentStreamVersion(ctx, db.CurrentStreamVersionParams{
		TenantID: sid.Tenant,
		StreamID: sid.Canonical(),
	})
	if err != nil {
		return 0, err
	}
	return uint64(v), nil
}

// GetEventByID returns one event by id. Returns ErrEventNotFound when
// the row does not exist for the tenant.
func (a *Adapter) GetEventByID(ctx context.Context, tenantID string, eventID uuid.UUID) (es.Envelope, error) {
	row, err := a.queries.GetEventByID(ctx, db.GetEventByIDParams{
		TenantID: tenantID,
		EventID:  eventID,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return es.Envelope{}, es.ErrEventNotFound
	}
	if err != nil {
		return es.Envelope{}, err
	}
	return rowToEnvelope(row)
}
