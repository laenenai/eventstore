package es

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// OutboxStore is the operational interface for the outbox table.
// Implemented by storage adapters alongside Store; consumed by the
// outbox.Drain helper. Separate from Store because outbox operations
// are infrastructure plumbing — application code (aggregates,
// projectors) doesn't touch them.
//
// See ADR 0014 for the outbox semantics and cookbook recipe 06 for
// production deployment patterns.
type OutboxStore interface {
	// PendingOutbox returns up to limit pending rows in
	// global_position order, joined with the originating event so the
	// drain can hand a complete envelope to the publisher.
	//
	// Filters applied:
	//   - published_at IS NULL                 (row is pending)
	//   - next_attempt_at IS NULL OR <= now    (retry-eligible)
	//   - attempts < maxAttempts               (not in DLQ state)
	//
	// tenantID="" means "across all tenants". maxAttempts=0 means
	// "no DLQ cap" (rows retry forever).
	PendingOutbox(ctx context.Context, tenantID string, limit int, maxAttempts int32) ([]OutboxRow, error)

	// QuarantinedStreams returns the (tenant_id, stream_id) pairs
	// that have at least one DLQ'd row. Used by the drain to
	// preserve per-stream order under quarantine mode: any row of
	// a quarantined stream is skipped until operator action releases
	// the stream.
	//
	// tenantID="" returns all tenants' quarantined streams.
	QuarantinedStreams(ctx context.Context, tenantID string, maxAttempts int32) ([]StreamRef, error)

	// MarkOutboxPublished records a successful publish; the row is
	// excluded from future drain queries.
	MarkOutboxPublished(ctx context.Context, tenantID string, globalPosition uint64) error

	// MarkOutboxFailed increments the attempts counter, stores the
	// truncated error message, and sets next_attempt_at. The Drain
	// computes next_attempt_at via its backoff function.
	MarkOutboxFailed(ctx context.Context, tenantID string, globalPosition uint64, errMsg string, nextAttemptAt time.Time) error

	// CleanupPublishedOutbox deletes rows that have been published
	// longer than olderThan. Returns the count deleted. Tenant-scoped.
	CleanupPublishedOutbox(ctx context.Context, tenantID string, olderThan time.Time) (int, error)
}

// OutboxAdmin is the inspection + operator-action interface for the
// outbox. Used to build dashboards (counts, lists) and operator tools
// (replay, abandon). Separate from OutboxStore so adapters can
// implement them independently — though in practice both live on the
// same adapter.
type OutboxAdmin interface {
	// CountPending returns the total number of unpublished rows.
	// Gauge metric.
	CountPending(ctx context.Context, tenantID string) (int64, error)

	// CountFailing returns rows that have failed at least once but
	// have not yet hit DLQ (attempts > 0 AND attempts < maxAttempts).
	// Worth watching; not yet alarming.
	CountFailing(ctx context.Context, tenantID string, maxAttempts int32) (int64, error)

	// CountDLQ returns rows in DLQ state
	// (attempts >= maxAttempts AND published_at IS NULL).
	// Alarm threshold.
	CountDLQ(ctx context.Context, tenantID string, maxAttempts int32) (int64, error)

	// ListDLQ returns DLQ'd rows paginated by global_position.
	// Pass afterPosition=0 for the first page; for subsequent pages,
	// pass the GlobalPosition of the last row from the previous page.
	ListDLQ(ctx context.Context, tenantID string, maxAttempts int32, afterPosition uint64, limit int) ([]DLQRow, error)

	// ReplayDLQ resets a single DLQ'd row so the next drain run
	// picks it up. Use after fixing the root cause (subscriber
	// deploy, schema migration, etc.).
	ReplayDLQ(ctx context.Context, tenantID string, globalPosition uint64) error

	// AbandonDLQ closes out a DLQ'd row without publishing. The
	// event itself stays in the events table (ADR 0005) — this
	// just marks the outbox row as "we give up trying to publish it".
	// Use for events that are genuinely garbage and never should
	// be delivered.
	AbandonDLQ(ctx context.Context, tenantID string, globalPosition uint64) error

	// ReplayAllDLQ resets every DLQ'd row for a tenant. Returns the
	// number of rows reset. Useful after a publisher outage recovery.
	ReplayAllDLQ(ctx context.Context, tenantID string, maxAttempts int32) (int64, error)
}

// OutboxRow is one outbox entry joined with its originating event
// envelope. Returned by PendingOutbox in global_position order.
type OutboxRow struct {
	Envelope Envelope
	Attempts int32
}

// DLQRow is a row in DLQ state with the operator-facing fields. The
// full envelope is not included by default — dashboards typically
// show summary info and let users drill into the event via separate
// GetEventByID calls.
type DLQRow struct {
	TenantID       string
	GlobalPosition uint64
	EventID        uuid.UUID
	StreamID       string // canonical "type-id" form
	TypeURL        string
	CorrelationID  uuid.UUID
	CausationID    uuid.UUID
	CommandID      uuid.UUID
	ActorPrincipal string
	EnqueuedAt     time.Time
	Attempts       int32
	LastError      string
	NextAttemptAt  *time.Time // nil if no retry has been scheduled
}

// StreamRef identifies one stream across tenants. Used by
// QuarantinedStreams.
type StreamRef struct {
	TenantID string
	StreamID string // canonical "type-id" form
}
