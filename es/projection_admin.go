package es

import (
	"context"
	"time"
)

// ProjectionStatus is the row returned by ProjectionAdmin.Status and
// each entry of ProjectionAdmin.List. See ADR 0020 decision 3g.
type ProjectionStatus struct {
	Name      string
	TenantID  string
	Cursor    uint64
	UpdatedAt time.Time
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
