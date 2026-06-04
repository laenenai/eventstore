package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel/metric"

	"github.com/laenenai/eventstore/adapters/storage/postgres/internal/db"
	"github.com/laenenai/eventstore/es"
	"github.com/laenenai/eventstore/es/obs"
)

// ReadStream returns events for a stream with version > fromVersion,
// ordered ascending. fromVersion=0 returns all events.
func (a *Adapter) ReadStream(ctx context.Context, sid es.StreamID, fromVersion uint64) ([]es.Envelope, error) {
	if err := sid.Validate(); err != nil {
		return nil, err
	}
	ctx, span := obs.Start(ctx, "store.read_stream",
		obs.Tenant(sid.Tenant),
		obs.StreamID(sid.String()),
		obs.DBSystem(dbSystemPostgreSQL),
	)
	defer span.End()
	start := time.Now()

	var rows []db.Event
	err := a.withTenantTx(ctx, sid.Tenant, func(q *db.Queries) error {
		var inner error
		rows, inner = q.ReadStreamFromVersion(ctx, db.ReadStreamFromVersionParams{
			TenantID:     sid.Tenant,
			StreamID:     sid.Canonical(),
			AfterVersion: int64(fromVersion),
		})
		return inner
	})

	obs.StoreReadStreamDuration.Record(ctx, time.Since(start).Seconds(),
		metric.WithAttributes(
			obs.Tenant(sid.Tenant),
			obs.DBSystem(dbSystemPostgreSQL),
		),
	)
	if err != nil {
		obs.EndWithErr(span, err)
		return nil, err
	}
	out := make([]es.Envelope, 0, len(rows))
	for _, r := range rows {
		env, err := rowToEnvelope(r)
		if err != nil {
			obs.EndWithErr(span, err)
			return nil, err
		}
		out = append(out, env)
	}
	span.SetAttributes(obs.EventCount(len(out)))
	return out, nil
}

// ReadAll returns events store-wide with global_position > fromPosition,
// limited to `limit` rows. Cross-tenant by design (ADR 0009) — uses the
// admin pool so RLS does not filter out other tenants' rows.
func (a *Adapter) ReadAll(ctx context.Context, fromPosition uint64, limit int) ([]es.Envelope, error) {
	q, err := a.admin()
	if err != nil {
		return nil, err
	}
	rows, err := q.ReadAllFromPosition(ctx, db.ReadAllFromPositionParams{
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
	var rows []db.Event
	err := a.withTenantTx(ctx, tenantID, func(q *db.Queries) error {
		var inner error
		rows, inner = q.ReadAllFromPositionTenant(ctx, db.ReadAllFromPositionTenantParams{
			TenantID:      tenantID,
			AfterPosition: int64(fromPosition),
			MaxRows:       int32(limit),
		})
		return inner
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
	var v int64
	err := a.withTenantTx(ctx, sid.Tenant, func(q *db.Queries) error {
		var inner error
		v, inner = q.CurrentStreamVersion(ctx, db.CurrentStreamVersionParams{
			TenantID: sid.Tenant,
			StreamID: sid.Canonical(),
		})
		return inner
	})
	if err != nil {
		return 0, err
	}
	return uint64(v), nil
}

// GetEventByID returns one event by id. Returns ErrEventNotFound when
// the row does not exist for the tenant.
func (a *Adapter) GetEventByID(ctx context.Context, tenantID string, eventID uuid.UUID) (es.Envelope, error) {
	var row db.Event
	err := a.withTenantTx(ctx, tenantID, func(q *db.Queries) error {
		var inner error
		row, inner = q.GetEventByID(ctx, db.GetEventByIDParams{
			TenantID: tenantID,
			EventID:  eventID,
		})
		return inner
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return es.Envelope{}, es.ErrEventNotFound
	}
	if err != nil {
		return es.Envelope{}, err
	}
	return rowToEnvelope(row)
}
