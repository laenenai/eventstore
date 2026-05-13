package sqlite

import (
	"context"
	"fmt"
	"time"

	"github.com/laenenai/eventstore/adapters/storage/sqlite/internal/db"
	"github.com/laenenai/eventstore/es"
)

// PendingOutbox implements es.OutboxStore.
func (a *Adapter) PendingOutbox(ctx context.Context, tenantID string, limit int) ([]es.OutboxRow, error) {
	rows, err := a.queries.PendingOutboxWithEnvelope(ctx, int64(limit))
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

// MarkOutboxPublished implements es.OutboxStore.
func (a *Adapter) MarkOutboxPublished(ctx context.Context, tenantID string, globalPosition uint64) error {
	return a.queries.MarkOutboxPublished(ctx, db.MarkOutboxPublishedParams{
		TenantID:       tenantID,
		GlobalPosition: int64(globalPosition),
	})
}

// MarkOutboxFailed implements es.OutboxStore.
func (a *Adapter) MarkOutboxFailed(ctx context.Context, tenantID string, globalPosition uint64, errMsg string) error {
	truncated := errMsg
	if len(truncated) > 2048 {
		truncated = truncated[:2048]
	}
	return a.queries.MarkOutboxFailed(ctx, db.MarkOutboxFailedParams{
		LastError:      &truncated,
		TenantID:       tenantID,
		GlobalPosition: int64(globalPosition),
	})
}

// CleanupPublishedOutbox implements es.OutboxStore.
func (a *Adapter) CleanupPublishedOutbox(ctx context.Context, tenantID string, olderThan time.Time) (int, error) {
	// SQLite stores TIMESTAMPTZ as TEXT; the generated CleanupPublished
	// expects the cutoff as a *string in the canonical timeLayout.
	cutoff := formatTime(olderThan)
	if err := a.queries.CleanupPublished(ctx, db.CleanupPublishedParams{
		TenantID:    tenantID,
		PublishedAt: &cutoff,
	}); err != nil {
		return 0, err
	}
	// CleanupPublished is :exec so we don't get the affected-row count.
	// Return -1 to signal "deleted, count unknown".
	return -1, nil
}

// rowToEnvelopeOutbox converts a PendingOutboxWithEnvelopeRow into an
// es.Envelope. The PendingOutboxWithEnvelopeRow shape isn't the same
// as db.Event (it's a JOIN result), so we can't reuse rowToEnvelope.
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

// Compile-time check that Adapter implements OutboxStore.
var _ es.OutboxStore = (*Adapter)(nil)
