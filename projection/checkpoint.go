package projection

import (
	"context"
	"sync"
)

// Checkpoint stores per-projector cursor positions over the
// global_position. Implementations must be safe for concurrent
// access by a single runtime (positions written and re-read in the
// same process). Per ADR 0020 decision 3e, cursors are keyed by
// (name, tenantID); pass tenantID == "" for cross-tenant projectors.
//
// Adapters ship a SQL-backed default (projection_checkpoint table)
// per ADR 0020 — that's what Runtime uses when its Checkpoint field
// is left nil. Override via Runtime.Checkpoint for Redis, Dynamo,
// in-memory tests, or anything else.
type Checkpoint interface {
	// Load returns the last-saved position for the projector, or 0
	// if the projector has never run.
	Load(ctx context.Context, name, tenantID string) (uint64, error)

	// Save persists the projector's current cursor.
	Save(ctx context.Context, name, tenantID string, position uint64) error
}

// MemoryCheckpoint is a process-local Checkpoint. Concurrency-safe
// via a single mutex. Useful for tests; production projectors should
// use the adapter-backed default.
type MemoryCheckpoint struct {
	mu        sync.Mutex
	positions map[string]uint64
}

// NewMemoryCheckpoint returns an empty in-memory checkpoint store.
func NewMemoryCheckpoint() *MemoryCheckpoint {
	return &MemoryCheckpoint{positions: map[string]uint64{}}
}

func (c *MemoryCheckpoint) Load(_ context.Context, name, tenantID string) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.positions[memKey(name, tenantID)], nil
}

func (c *MemoryCheckpoint) Save(_ context.Context, name, tenantID string, position uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.positions[memKey(name, tenantID)] = position
	return nil
}

func memKey(name, tenantID string) string { return name + "|" + tenantID }

// Compile-time check.
var _ Checkpoint = (*MemoryCheckpoint)(nil)
