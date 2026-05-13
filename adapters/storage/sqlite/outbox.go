package sqlite

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/laenenai/eventstore/adapters/storage/sqlite/internal/db"
	"github.com/laenenai/eventstore/es"
)

// PendingOutbox implements es.OutboxStore. Filters by retry-eligibility
// and DLQ threshold before joining with events.
func (a *Adapter) PendingOutbox(ctx context.Context, tenantID string, limit int, maxAttempts int32) ([]es.OutboxRow, error) {
	now := formatTime(time.Now().UTC())
	rows, err := a.queries.PendingOutboxWithEnvelope(ctx, db.PendingOutboxWithEnvelopeParams{
		NextAttemptAt: &now,
		Attempts:      int64(safeMaxAttempts(maxAttempts)),
		Limit:         int64(limit),
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
		out = append(out, es.OutboxRow{Envelope: env, Attempts: int32(r.Attempts)})
	}
	return out, nil
}

// QuarantinedStreams implements es.OutboxStore.
func (a *Adapter) QuarantinedStreams(ctx context.Context, tenantID string, maxAttempts int32) ([]es.StreamRef, error) {
	maxA := int64(safeMaxAttempts(maxAttempts))
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
		Attempts: maxA,
		TenantID: tenantID,
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
	nextStr := formatTime(nextAttemptAt)
	return a.queries.MarkOutboxFailed(ctx, db.MarkOutboxFailedParams{
		LastError:      &truncated,
		NextAttemptAt:  &nextStr,
		TenantID:       tenantID,
		GlobalPosition: int64(globalPosition),
	})
}

// CleanupPublishedOutbox implements es.OutboxStore.
func (a *Adapter) CleanupPublishedOutbox(ctx context.Context, tenantID string, olderThan time.Time) (int, error) {
	cutoff := formatTime(olderThan)
	if err := a.queries.CleanupPublished(ctx, db.CleanupPublishedParams{
		TenantID:    tenantID,
		PublishedAt: &cutoff,
	}); err != nil {
		return 0, err
	}
	return -1, nil
}

// safeMaxAttempts converts a user-supplied maxAttempts (0 = unbounded)
// into a SQL-friendly value. We pass int64 max for "no limit" to keep
// the SQL filter simple.
func safeMaxAttempts(n int32) int64 {
	if n <= 0 {
		return math.MaxInt32
	}
	return int64(n)
}

// rowToEnvelopeOutbox converts a PendingOutboxWithEnvelopeRow into
// an es.Envelope. The shape differs from db.Event (it's a JOIN
// result), so we can't reuse rowToEnvelope.
func rowToEnvelopeOutbox(r db.PendingOutboxWithEnvelopeRow) (es.Envelope, error) {
	sid, err := es.ParseCanonical(r.TenantID, r.StreamID)
	if err != nil {
		return es.Envelope{}, fmt.Errorf("parse stream id %q: %w", r.StreamID, err)
	}
	actor, err := decodeActor(r.Actor)
	if err != nil {
		return es.Envelope{}, err
	}
	occurred, err := parseTime(r.OccurredAt)
	if err != nil {
		return es.Envelope{}, fmt.Errorf("occurred_at: %w", err)
	}
	recorded, err := parseTime(r.RecordedAt)
	if err != nil {
		return es.Envelope{}, fmt.Errorf("recorded_at: %w", err)
	}
	env := es.Envelope{
		EventID:        r.EventID,
		TenantID:       r.TenantID,
		StreamID:       sid,
		Version:        uint64(r.Version),
		GlobalPosition: uint64(r.GlobalPosition),
		TypeURL:        r.TypeUrl,
		SchemaVersion:  uint32(r.SchemaVersion),
		OccurredAt:     occurred,
		RecordedAt:     recorded,
		CorrelationID:  r.CorrelationID,
		CausationID:    r.CausationID,
		CommandID:      r.CommandID,
		Actor:          actor,
		Payload:        r.Payload,
	}
	if r.EncryptionKeyRefs != nil {
		env.KeyRefs = []byte(*r.EncryptionKeyRefs)
	}
	return env, nil
}

// Compile-time check that Adapter implements both interfaces.
var (
	_ es.OutboxStore = (*Adapter)(nil)
	_ es.OutboxAdmin = (*Adapter)(nil)
)
