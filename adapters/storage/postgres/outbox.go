package postgres

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/laenenai/eventstore/adapters/storage/postgres/internal/db"
	"github.com/laenenai/eventstore/es"
)

// PendingOutbox implements es.OutboxStore.
func (a *Adapter) PendingOutbox(ctx context.Context, tenantID string, limit int, maxAttempts int32) ([]es.OutboxRow, error) {
	now := time.Now().UTC()
	rows, err := a.queries.PendingOutboxWithEnvelope(ctx, db.PendingOutboxWithEnvelopeParams{
		Now:         &now,
		MaxAttempts: safeMaxAttempts(maxAttempts),
		MaxRows:     int32(limit),
	})
	if err != nil {
		return nil, err
	}
	out := make([]es.OutboxRow, 0, len(rows))
	for _, r := range rows {
		// PendingOutboxWithEnvelope is cross-tenant; filter here for
		// the tenant-scoped variant.
		if tenantID != "" && r.TenantID != tenantID {
			continue
		}
		env, err := rowToEnvelopeOutbox(r)
		if err != nil {
			return nil, err
		}
		out = append(out, es.OutboxRow{Envelope: env, Attempts: r.Attempts})
	}
	return out, nil
}

// QuarantinedStreams implements es.OutboxStore.
func (a *Adapter) QuarantinedStreams(ctx context.Context, tenantID string, maxAttempts int32) ([]es.StreamRef, error) {
	maxA := safeMaxAttempts(maxAttempts)
	if tenantID == "" {
		rows, err := a.queries.QuarantinedStreamsAllTenants(ctx, maxA)
		if err != nil {
			return nil, err
		}
		out := make([]es.StreamRef, 0, len(rows))
		for _, r := range rows {
			out = append(out, es.StreamRef{TenantID: r.TenantID, StreamID: r.StreamID})
		}
		return out, nil
	}
	rows, err := a.queries.QuarantinedStreams(ctx, db.QuarantinedStreamsParams{
		MaxAttempts: maxA,
		TenantID:    tenantID,
	})
	if err != nil {
		return nil, err
	}
	out := make([]es.StreamRef, 0, len(rows))
	for _, r := range rows {
		out = append(out, es.StreamRef{TenantID: r.TenantID, StreamID: r.StreamID})
	}
	return out, nil
}

// MarkOutboxPublished implements es.OutboxStore.
func (a *Adapter) MarkOutboxPublished(ctx context.Context, tenantID string, globalPosition uint64) error {
	return a.queries.MarkOutboxPublished(ctx, db.MarkOutboxPublishedParams{
		TenantID:       tenantID,
		GlobalPosition: int64(globalPosition),
	})
}

// MarkOutboxFailed implements es.OutboxStore.
func (a *Adapter) MarkOutboxFailed(ctx context.Context, tenantID string, globalPosition uint64, errMsg string, nextAttemptAt time.Time) error {
	truncated := errMsg
	if len(truncated) > 2048 {
		truncated = truncated[:2048]
	}
	nextAt := nextAttemptAt.UTC()
	return a.queries.MarkOutboxFailed(ctx, db.MarkOutboxFailedParams{
		LastError:      &truncated,
		NextAttemptAt:  &nextAt,
		TenantID:       tenantID,
		GlobalPosition: int64(globalPosition),
	})
}

// CleanupPublishedOutbox implements es.OutboxStore.
func (a *Adapter) CleanupPublishedOutbox(ctx context.Context, tenantID string, olderThan time.Time) (int, error) {
	cutoff := olderThan.UTC()
	if err := a.queries.CleanupPublished(ctx, db.CleanupPublishedParams{
		TenantID:  tenantID,
		OlderThan: &cutoff,
	}); err != nil {
		return 0, err
	}
	return -1, nil
}

// safeMaxAttempts converts a user-supplied maxAttempts (0 = unbounded)
// into a SQL-friendly value.
func safeMaxAttempts(n int32) int32 {
	if n <= 0 {
		return math.MaxInt32
	}
	return n
}

// rowToEnvelopeOutbox converts a PendingOutboxWithEnvelopeRow into
// an es.Envelope.
func rowToEnvelopeOutbox(r db.PendingOutboxWithEnvelopeRow) (es.Envelope, error) {
	sid, err := es.ParseCanonical(r.TenantID, r.StreamID)
	if err != nil {
		return es.Envelope{}, fmt.Errorf("parse stream id %q: %w", r.StreamID, err)
	}
	actor, err := decodeActor(r.Actor)
	if err != nil {
		return es.Envelope{}, err
	}
	return es.Envelope{
		EventID:        r.EventID,
		TenantID:       r.TenantID,
		StreamID:       sid,
		Version:        uint64(r.Version),
		GlobalPosition: uint64(r.GlobalPosition),
		TypeURL:        r.TypeUrl,
		SchemaVersion:  uint32(r.SchemaVersion),
		OccurredAt:     r.OccurredAt,
		RecordedAt:     r.RecordedAt,
		CorrelationID:  r.CorrelationID,
		CausationID:    r.CausationID,
		CommandID:      r.CommandID,
		Actor:          actor,
		Payload:        r.Payload,
		KeyRefs:        r.EncryptionKeyRefs,
		Hash:           r.Hash,
		PrevHash:       r.PrevHash,
	}, nil
}

// Compile-time checks.
var (
	_ es.OutboxStore = (*Adapter)(nil)
	_ es.OutboxAdmin = (*Adapter)(nil)
)
