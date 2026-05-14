package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/laenenai/eventstore/adapters/storage/sqlite/internal/db"
	"github.com/laenenai/eventstore/es"
)

// Append commits one batch of events plus any uniqueness constraint
// operations in a single transaction. See ADR 0009 (gap-free
// global_position semantics — SQLite is single-writer so the advisory-
// lock from the Postgres adapter is not needed here) and ADR 0010
// (uniqueness as a first-class store capability).
//
// Sequence per transaction:
//  1. BEGIN
//  2. ClaimUnique / ReleaseUnique for each constraint op
//  3. InsertEvent (LastInsertId() yields global_position)
//  4. InsertOutbox
//  5. COMMIT
//
// Errors:
//   - Unique violation against unique_claims -> es.ErrConstraintViolated.
//   - Unique violation against events.(tenant,stream,version) -> es.ErrConflict.
//   - Any other DB error: returned unchanged.
func (a *Adapter) Append(ctx context.Context, p es.AppendParams) (es.AppendResult, error) {
	if err := p.StreamID.Validate(); err != nil {
		return es.AppendResult{}, err
	}
	if len(p.Events) == 0 {
		return es.AppendResult{}, errors.New("sqlite: append requires at least one event")
	}

	var result es.AppendResult
	err := withTx(ctx, a.db, func(tx *sql.Tx) error {
		q := a.queries.WithTx(tx)

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
				return fmt.Errorf("sqlite: unknown constraint op %d", op.Op)
			}
		}

		now := time.Now().UTC()
		recordedAt := formatTime(now)

		var startPos, endPos int64
		for i, ev := range p.Events {
			actorStr, err := encodeActor(ev.Actor)
			if err != nil {
				return err
			}

			var payloadJSON, keyRefs *string
			if len(ev.PayloadJSON) > 0 {
				s := string(ev.PayloadJSON)
				payloadJSON = &s
			}
			if len(ev.KeyRefs) > 0 {
				s := string(ev.KeyRefs)
				keyRefs = &s
			}

			gp, err := q.InsertEvent(ctx, db.InsertEventParams{
				EventID:           ev.EventID,
				TenantID:          p.StreamID.Tenant,
				StreamID:          canonical,
				Version:           int64(p.ExpectedVersion) + int64(i) + 1,
				TypeUrl:           ev.TypeURL,
				SchemaVersion:     int64(ev.SchemaVersion),
				OccurredAt:        formatTime(ev.OccurredAt),
				RecordedAt:        recordedAt,
				CorrelationID:     ev.CorrelationID,
				CausationID:       ev.CausationID,
				CommandID:         ev.CommandID,
				Actor:             actorStr,
				ActorPrincipal:    ev.Actor.Principal,
				Payload:           ev.Payload,
				PayloadJson:       payloadJSON,
				EncryptionKeyRefs: keyRefs,
			})
			if err != nil {
				return mapErr(err)
			}
			if i == 0 {
				startPos = gp
			}
			endPos = gp

			if err := q.InsertOutbox(ctx, db.InsertOutboxParams{
				TenantID:       p.StreamID.Tenant,
				GlobalPosition: gp,
				EventID:        ev.EventID,
			}); err != nil {
				return fmt.Errorf("insert outbox: %w", err)
			}
		}

		// Tier-1 state_cache row (optional). Written in the same tx so
		// reads after Append see the post-decide state. See ADR 0020.
		if p.NewStateBytes != nil {
			terminal := int64(0)
			if p.Terminal {
				terminal = 1
			}
			schema := p.StateSchemaVersion
			if schema == 0 {
				schema = 1
			}
			if err := q.UpsertStateCache(ctx, db.UpsertStateCacheParams{
				TenantID:           p.StreamID.Tenant,
				StreamID:           canonical,
				TypeUrl:            p.StateTypeURL,
				State:              string(p.NewStateBytes),
				Version:            int64(p.ExpectedVersion) + int64(len(p.Events)),
				Terminal:           terminal,
				StateSchemaVersion: int64(schema),
				UpdatedAt:          recordedAt,
			}); err != nil {
				return fmt.Errorf("upsert state_cache: %w", err)
			}
		}

		result = es.AppendResult{
			StartVersion:        p.ExpectedVersion + 1,
			EndVersion:          p.ExpectedVersion + uint64(len(p.Events)),
			StartGlobalPosition: uint64(startPos),
			EndGlobalPosition:   uint64(endPos),
			RecordedAt:          now,
		}
		return nil
	})
	return result, err
}

// withTx runs fn inside a transaction, committing on nil return and
// rolling back otherwise. Mirrors pgx.BeginFunc.
func withTx(ctx context.Context, sqlDB *sql.DB, fn func(*sql.Tx) error) (err error) {
	tx, beginErr := sqlDB.BeginTx(ctx, nil)
	if beginErr != nil {
		return fmt.Errorf("begin tx: %w", beginErr)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	if err = fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}

// mapErr translates SQLite unique-constraint failures into framework
// sentinel errors. Driver-agnostic: matches on the error message text
// since modernc, libsql-client-go, and go-libsql surface their errors
// differently at the Go-error level.
//
// SQLite's unique-violation message has the form
//   "UNIQUE constraint failed: <table>.<col>, <table>.<col>, ..."
// and we route based on the table prefix.
func mapErr(err error) error {
	if err == nil {
		return nil
	}
	msg := err.Error()
	if !strings.Contains(msg, "UNIQUE constraint failed") {
		return err
	}
	switch {
	case strings.Contains(msg, "unique_claims."):
		return es.ErrConstraintViolated
	case strings.Contains(msg, "events."):
		return es.ErrConflict
	default:
		return err
	}
}
