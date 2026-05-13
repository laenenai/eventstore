package es

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// ProjectionStatus is the row returned by ProjectionAdmin.Status and
// each entry of ProjectionAdmin.List. See ADR 0020 decision 3g.
type ProjectionStatus struct {
	Name      string
	TenantID  string
	Cursor    uint64
	UpdatedAt time.Time
}

// ProjectionDLQRow is one entry in projection_dlq. Populated by the
// projection runtime when DLQOnFailure is set and a handler returned
// an error.
type ProjectionDLQRow struct {
	ProjectionName string
	TenantID       string
	GlobalPosition uint64
	EventID        uuid.UUID
	TypeURL        string
	LastError      string
	EnqueuedAt     time.Time
}

// ProjectionDLQWriter is implemented by adapters that support the
// DLQ-skip failure mode. projection.Runtime.DLQOnFailure requires the
// Store to satisfy this interface.
type ProjectionDLQWriter interface {
	InsertProjectionDLQ(ctx context.Context, projectionName, tenantID string,
		globalPosition uint64, eventID uuid.UUID, typeURL, lastError string) error
}

// ProjectionDLQAdmin is the inspection-and-management surface for the
// projection DLQ. Admins implement it alongside ProjectionAdmin.
type ProjectionDLQAdmin interface {
	ListProjectionDLQ(ctx context.Context, projectionName, tenantID string,
		afterPosition uint64, limit int) ([]ProjectionDLQRow, error)

	CountProjectionDLQ(ctx context.Context, projectionName, tenantID string) (int64, error)

	// AbandonProjectionDLQ removes one DLQ entry by global_position.
	// Operator says "this event is permanently skipped". The event
	// stays in the events table — only the DLQ marker goes.
	AbandonProjectionDLQ(ctx context.Context, projectionName, tenantID string,
		globalPosition uint64) error

	// AbandonAllProjectionDLQ wipes every DLQ row for one projection.
	// Returns the number of rows removed.
	AbandonAllProjectionDLQ(ctx context.Context, projectionName, tenantID string) (int64, error)
}

// ProjectionAdmin is the inspection-and-control surface for Tier-3
// projections — analogous to OutboxAdmin for the drain. Adapters
// implement it against the framework-managed projection_checkpoint
// table.
//
// The standard rebuild workflow (cookbook recipe 08):
//
//  1. Optionally stop the runner.
//  2. App-specific: TRUNCATE my_read_model (or equivalent).
//  3. admin.Reset(ctx, name, tenant) — sets cursor to 0.
//  4. Runner picks up: re-reads from gp=0 and rebuilds.
//
// The framework does not own the read-model storage, so step 2 is
// always app-specific. Use ResetTo for partial replay from a known
// good cursor.
type ProjectionAdmin interface {
	// Status returns the current cursor and last-updated time for one
	// projector. Returns ErrStateNotFound when the projector has
	// never been recorded.
	Status(ctx context.Context, name, tenantID string) (ProjectionStatus, error)

	// Reset sets the projector's cursor to 0. Combined with an
	// app-side TRUNCATE this is the standard rebuild trigger.
	Reset(ctx context.Context, name, tenantID string) error

	// ResetTo sets the projector's cursor to a specific
	// global_position. Use for partial replay from a known-good
	// point.
	ResetTo(ctx context.Context, name, tenantID string, position uint64) error

	// List enumerates every projector known to the adapter. For
	// dashboards and lag-monitoring queries.
	List(ctx context.Context) ([]ProjectionStatus, error)
}
