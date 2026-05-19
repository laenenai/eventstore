package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"go.opentelemetry.io/otel/metric"

	"github.com/laenenai/eventstore/adapters/storage/postgres/internal/db"
	"github.com/laenenai/eventstore/es"
	"github.com/laenenai/eventstore/es/obs"
)

// pgUniqueViolation is the SQLSTATE for unique-constraint conflicts.
const pgUniqueViolation = "23505"

// dbSystemPostgreSQL is the value reported for the OTel semconv
// db.system attribute on every span/metric this adapter emits.
const dbSystemPostgreSQL = "postgresql"

// Append commits one batch of events plus any uniqueness constraint
// operations in a single transaction. See ADR 0009 (advisory-lock +
// sequence) and ADR 0010 (uniqueness as a first-class store capability).
//
// Sequence per transaction:
//  1. BEGIN
//  2. pg_advisory_xact_lock(<store-wide constant>)  — serializes all writers
//  3. ClaimUnique / ReleaseUnique for each constraint op
//  4. InsertEvent (allocates global_position from sequence)
//  5. InsertOutbox (durable publish backstop)
//  6. COMMIT
//
// On a unique-violation against unique_claims (or its partitions), the
// error surfaces as es.ErrConstraintViolated. On a unique-violation
// against the events primary key (tenant_id, stream_id, version), the
// error surfaces as es.ErrConflict — the caller's expected version is
// stale and they must reload + retry.
func (a *Adapter) Append(ctx context.Context, p es.AppendParams) (es.AppendResult, error) {
	if err := p.StreamID.Validate(); err != nil {
		return es.AppendResult{}, err
	}
	if len(p.Events) == 0 {
		return es.AppendResult{}, errors.New("postgres: append requires at least one event")
	}

	ctx, span := obs.Start(ctx, "store.append",
		obs.Tenant(p.StreamID.Tenant),
		obs.StreamID(p.StreamID.String()),
		obs.EventCount(len(p.Events)),
		obs.DBSystem(dbSystemPostgreSQL),
	)
	defer span.End()
	start := time.Now()

	var result es.AppendResult
	err := pgx.BeginFunc(ctx, a.pool, func(tx pgx.Tx) error {
		q := a.queries.WithTx(tx)

		// 1. Acquire store-wide advisory lock. Auto-released on
		//    commit/rollback.
		if err := q.AdvisoryLock(ctx, a.lockKey); err != nil {
			return fmt.Errorf("acquire advisory lock: %w", err)
		}

		// 2. Apply constraint ops; a claim conflict fails the whole
		//    transaction with ErrConstraintViolated.
		canonical := p.StreamID.Canonical()
		for _, op := range p.Constraints {
			switch op.Op {
			case es.ClaimConstraint:
				if err := q.ClaimUnique(ctx, db.ClaimUniqueParams{
					TenantID: p.StreamID.Tenant,
					Scope:    op.Scope,
					Value:    op.Value,
					StreamID: canonical,
				}); err != nil {
					return mapErr(err)
				}
			case es.ReleaseConstraint:
				if err := q.ReleaseUnique(ctx, db.ReleaseUniqueParams{
					TenantID: p.StreamID.Tenant,
					Scope:    op.Scope,
					Value:    op.Value,
				}); err != nil {
					return fmt.Errorf("release constraint: %w", err)
				}
			default:
				return fmt.Errorf("postgres: unknown constraint op %d", op.Op)
			}
		}

		// 3a. Tamper-evident chain (ADR 0028). Read predecessor hash in
		//     the same transaction so the store-wide advisory lock plus
		//     PK uniqueness keep the chain serializable on this stream.
		prevHash, err := q.LastStreamHash(ctx, db.LastStreamHashParams{
			TenantID: p.StreamID.Tenant,
			StreamID: canonical,
		})
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("read last hash: %w", err)
		}
		if len(prevHash) == 0 {
			prevHash = es.ZeroHash
		}

		// 3b. Insert each event with caller-supplied version. PK
		//     violation on (tenant, stream, version) → ErrConflict.
		var (
			startPos int64
			endPos   int64
			recAt    = result.RecordedAt
		)
		for i, ev := range p.Events {
			actorBytes, err := encodeActor(ev.Actor)
			if err != nil {
				return err
			}

			version := uint64(int64(p.ExpectedVersion) + int64(i) + 1)
			chainEnv := es.Envelope{
				EventID:       ev.EventID,
				TenantID:      p.StreamID.Tenant,
				StreamID:      p.StreamID,
				Version:       version,
				TypeURL:       ev.TypeURL,
				SchemaVersion: ev.SchemaVersion,
				OccurredAt:    ev.OccurredAt,
				CorrelationID: ev.CorrelationID,
				CausationID:   ev.CausationID,
				CommandID:     ev.CommandID,
				Actor:         ev.Actor,
				Payload:       ev.Payload,
				KeyRefs:       ev.KeyRefs,
			}
			hash, err := es.ComputeChainHash(prevHash, &chainEnv)
			if err != nil {
				return fmt.Errorf("compute chain hash: %w", err)
			}

			row, err := q.InsertEvent(ctx, db.InsertEventParams{
				EventID:           ev.EventID,
				TenantID:          p.StreamID.Tenant,
				StreamID:          canonical,
				Version:           int64(version),
				TypeUrl:           ev.TypeURL,
				SchemaVersion:     int32(ev.SchemaVersion),
				OccurredAt:        ev.OccurredAt,
				CorrelationID:     ev.CorrelationID,
				CausationID:       ev.CausationID,
				CommandID:         ev.CommandID,
				Actor:             actorBytes,
				ActorPrincipal:    ev.Actor.Principal,
				Payload:           ev.Payload,
				PayloadJson:       ev.PayloadJSON,
				EncryptionKeyRefs: ev.KeyRefs,
				Hash:              hash,
				PrevHash:          prevHash,
			})
			if err != nil {
				return mapErr(err)
			}
			if i == 0 {
				startPos = row.GlobalPosition
			}
			endPos = row.GlobalPosition
			recAt = row.RecordedAt
			prevHash = hash

			// 4. Outbox row pointing at the just-inserted event.
			if err := q.InsertOutbox(ctx, db.InsertOutboxParams{
				TenantID:       p.StreamID.Tenant,
				GlobalPosition: row.GlobalPosition,
				EventID:        ev.EventID,
			}); err != nil {
				return fmt.Errorf("insert outbox: %w", err)
			}
		}

		// 5. Tier-1 state_cache row (optional). Written in the same
		//    transaction so reads after Append are guaranteed to see
		//    the post-decide state. See ADR 0020 + ADR 0023.
		if p.NewStateBytes != nil {
			schema := p.StateSchemaVersion
			if schema == 0 {
				schema = 1
			}
			if err := q.UpsertStateCache(ctx, db.UpsertStateCacheParams{
				TenantID:           p.StreamID.Tenant,
				StreamID:           canonical,
				TypeUrl:            p.StateTypeURL,
				State:              p.NewStateBytes,
				Version:            int64(p.ExpectedVersion) + int64(len(p.Events)),
				Terminal:           p.Terminal,
				StateSchemaVersion: int32(schema),
			}); err != nil {
				return fmt.Errorf("upsert state_cache: %w", err)
			}
		}

		result = es.AppendResult{
			StartVersion:        p.ExpectedVersion + 1,
			EndVersion:          p.ExpectedVersion + uint64(len(p.Events)),
			StartGlobalPosition: uint64(startPos),
			EndGlobalPosition:   uint64(endPos),
			RecordedAt:          recAt,
		}
		return nil
	})

	obs.StoreAppendDuration.Record(ctx, time.Since(start).Seconds(),
		metric.WithAttributes(
			obs.Tenant(p.StreamID.Tenant),
			obs.DBSystem(dbSystemPostgreSQL),
		),
	)
	if err != nil {
		obs.EndWithErr(span, err)
		return result, err
	}
	obs.EventsAppendedTotal.Add(ctx, int64(len(p.Events)),
		metric.WithAttributes(
			obs.Tenant(p.StreamID.Tenant),
			obs.DBSystem(dbSystemPostgreSQL),
		),
	)
	span.SetAttributes(obs.Version(result.EndVersion))
	return result, nil
}

// mapErr translates Postgres unique-violations into framework sentinel
// errors. Other errors pass through unchanged.
func mapErr(err error) error {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return err
	}
	if pgErr.Code != pgUniqueViolation {
		return err
	}
	// PG reports the partition's table name (e.g., "events_p03",
	// "unique_claims_p07") on conflict. Match by prefix.
	switch {
	case strings.HasPrefix(pgErr.TableName, "unique_claims"):
		return es.ErrConstraintViolated
	case strings.HasPrefix(pgErr.TableName, "events"):
		return es.ErrConflict
	default:
		return err
	}
}
