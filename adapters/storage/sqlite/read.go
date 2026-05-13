package sqlite

import (
	"context"
	"database/sql"
	"errors"

	"github.com/google/uuid"

	"github.com/laenenai/eventstore/adapters/storage/sqlite/internal/db"
	"github.com/laenenai/eventstore/es"
)

// ReadStream returns events for a stream with version > fromVersion,
// ordered ascending.
func (a *Adapter) ReadStream(ctx context.Context, sid es.StreamID, fromVersion uint64) ([]es.Envelope, error) {
	if err := sid.Validate(); err != nil {
		return nil, err
	}
	rows, err := a.queries.ReadStreamFromVersion(ctx, db.ReadStreamFromVersionParams{
		TenantID: sid.Tenant,
		StreamID: sid.Canonical(),
		Version:  int64(fromVersion),
	})
	if err != nil {
		return nil, err
	}
	return collect(rows)
}

// ReadAll returns events store-wide with global_position > fromPosition.
func (a *Adapter) ReadAll(ctx context.Context, fromPosition uint64, limit int) ([]es.Envelope, error) {
	rows, err := a.queries.ReadAllFromPosition(ctx, db.ReadAllFromPositionParams{
		GlobalPosition: int64(fromPosition),
		Limit:          int64(limit),
	})
	if err != nil {
		return nil, err
	}
	return collect(rows)
}

// ReadAllForTenant is ReadAll scoped to a single tenant.
func (a *Adapter) ReadAllForTenant(ctx context.Context, tenantID string, fromPosition uint64, limit int) ([]es.Envelope, error) {
	rows, err := a.queries.ReadAllFromPositionTenant(ctx, db.ReadAllFromPositionTenantParams{
		TenantID:       tenantID,
		GlobalPosition: int64(fromPosition),
		Limit:          int64(limit),
	})
	if err != nil {
		return nil, err
	}
	return collect(rows)
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
	if i, ok := v.(int64); ok {
		return uint64(i), nil
	}
	return 0, nil
}

// GetEventByID returns one event by id, or ErrEventNotFound.
func (a *Adapter) GetEventByID(ctx context.Context, tenantID string, eventID uuid.UUID) (es.Envelope, error) {
	row, err := a.queries.GetEventByID(ctx, db.GetEventByIDParams{
		TenantID: tenantID,
		EventID:  eventID,
	})
	if errors.Is(err, sql.ErrNoRows) {
		return es.Envelope{}, es.ErrEventNotFound
	}
	if err != nil {
		return es.Envelope{}, err
	}
	return rowToEnvelope(row)
}

// collect converts a slice of sqlc rows into es.Envelope.
func collect(rows []db.Event) ([]es.Envelope, error) {
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
