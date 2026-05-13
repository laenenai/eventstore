package es

import (
	"context"
	"time"
)

// OutboxStore is the operational interface for the outbox table.
// Implemented by storage adapters alongside Store; consumed by the
// outbox.Drain helper. Separate from Store because outbox operations
// are infrastructure plumbing — application code (aggregates,
// projectors) doesn't touch them.
//
// See ADR 0014 for the outbox semantics.
type OutboxStore interface {
	// PendingOutbox returns up to limit pending rows in
	// global_position order, joined with the originating event so the
	// drain can hand a complete envelope to the publisher.
	//
	// tenantID="" means "across all tenants" (used by shared
	// scheduled drains). Tenant-scoped drains pass the tenant id.
	PendingOutbox(ctx context.Context, tenantID string, limit int) ([]OutboxRow, error)

	// MarkOutboxPublished records a successful publish; the row is
	// excluded from future drain queries.
	MarkOutboxPublished(ctx context.Context, tenantID string, globalPosition uint64) error

	// MarkOutboxFailed increments the attempts counter and stores
	// the truncated error message. The row remains pending; the next
	// drain run retries.
	MarkOutboxFailed(ctx context.Context, tenantID string, globalPosition uint64, errMsg string) error

	// CleanupPublishedOutbox deletes rows that have been published
	// longer than olderThan. Returns the count deleted. Tenant-scoped.
	CleanupPublishedOutbox(ctx context.Context, tenantID string, olderThan time.Time) (int, error)
}

// OutboxRow is one outbox entry joined with its originating event
// envelope. Returned by PendingOutbox in global_position order.
type OutboxRow struct {
	Envelope Envelope
	Attempts int32
}
