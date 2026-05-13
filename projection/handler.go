package projection

import (
	"context"

	"github.com/laenenai/eventstore/es"
)

// Handler processes one event for a projection.
//
// The runtime guarantees at-least-once delivery; handlers must be
// idempotent. Common patterns: dedup by event_id, use ON CONFLICT
// upserts, or check a "processed events" table before mutating the
// read model.
//
// Handlers SHOULD return quickly. The runtime advances the checkpoint
// after each batch, not after each event, so long-running handlers
// extend the lag for the whole projection. For per-event durable
// state, use a saga (cookbook recipe 02) instead.
//
// Returning an error halts the projection. The runtime does not
// retry — recovery happens on next Run().
type Handler func(ctx context.Context, env es.Envelope) error
