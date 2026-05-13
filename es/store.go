package es

import (
	"context"

	"github.com/google/uuid"
)

// Store is the storage contract every adapter implements. Adapter
// implementations live under adapters/storage/{postgres,sqlite}; see
// ADRs 0005, 0009, 0010, 0014, 0017.
//
// All operations require a tenant in context (RequireTenant) where
// applicable, plus a validated StreamID with a non-empty Tenant.
type Store interface {
	// Append commits one batch of events plus any constraint operations
	// in a single transaction. Returns ErrConflict on optimistic-
	// concurrency failure, ErrConstraintViolated on a uniqueness clash.
	Append(ctx context.Context, p AppendParams) (AppendResult, error)

	// ReadStream returns events for a stream with version > fromVersion,
	// ordered ascending. fromVersion=0 returns the full stream.
	ReadStream(ctx context.Context, sid StreamID, fromVersion uint64) ([]Envelope, error)

	// ReadAll returns up to limit events with global_position > fromPosition,
	// across all tenants. Used by admin-scope subscribers (compliance,
	// billing). Most callers should use ReadAllForTenant.
	ReadAll(ctx context.Context, fromPosition uint64, limit int) ([]Envelope, error)

	// ReadAllForTenant is ReadAll scoped to a single tenant. Used by
	// per-tenant projection rebuilds and gap-fill in subscribers.
	ReadAllForTenant(ctx context.Context, tenantID string, fromPosition uint64, limit int) ([]Envelope, error)

	// CurrentStreamVersion returns the highest committed version for a
	// stream, or 0 if the stream is empty.
	CurrentStreamVersion(ctx context.Context, sid StreamID) (uint64, error)

	// GetEventByID returns one event by its UUIDv7 id. Returns
	// ErrEventNotFound if missing.
	GetEventByID(ctx context.Context, tenantID string, eventID uuid.UUID) (Envelope, error)
}
