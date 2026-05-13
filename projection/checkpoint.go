package projection

import (
	"context"
	"sync"
)

// Checkpoint stores per-projector cursor positions over the
// global_position. Implementations must be safe for concurrent
// access by a single runtime (positions written and re-read in the
// same process).
//
// Production projectors typically use a SQL-backed checkpoint in the
// read-model database so the checkpoint advance and the read-model
// mutation commit atomically (exactly-once at the read-model layer).
// The MemoryCheckpoint here is for tests and for projections whose
// read models can tolerate at-least-once writes idempotently.
type Checkpoint interface {
	// Load returns the last-saved position for the projector, or 0
	// if the projector has never run.
	Load(ctx context.Context, projector string) (uint64, error)

	// Save persists the projector's current cursor.
	Save(ctx context.Context, projector string, position uint64) error
}

// MemoryCheckpoint is a process-local Checkpoint. Concurrency-safe
// via a single mutex.
type MemoryCheckpoint struct {
	mu        sync.Mutex
	positions map[string]uint64
}

// NewMemoryCheckpoint returns an empty in-memory checkpoint store.
func NewMemoryCheckpoint() *MemoryCheckpoint {
	return &MemoryCheckpoint{positions: map[string]uint64{}}
}

func (c *MemoryCheckpoint) Load(ctx context.Context, projector string) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.positions[projector], nil
}

func (c *MemoryCheckpoint) Save(ctx context.Context, projector string, position uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.positions[projector] = position
	return nil
}

// Compile-time check.
var _ Checkpoint = (*MemoryCheckpoint)(nil)
