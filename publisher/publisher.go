// Package publisher hosts the EventPublisher contract. Implementations
// live under adapters/publisher/{restate,nats,sns,pubsub,cfqueues};
// the inproc subpackage provides a single-process publisher for tests
// and examples.
//
// See ADR 0012 for the delivery model: in the serverless profile
// (Neon, Turso), the publisher is the runtime that delivers events
// from the writer to projection / saga subscribers without keeping the
// database awake. Durability is provided by the outbox; the publisher
// is responsible for at-least-once delivery to its subscribers.
package publisher

import (
	"context"

	"github.com/laenenai/eventstore/es"
)

// Publisher publishes one event to all its subscribers.
//
// Implementations vary:
//
//   - inproc:   single-process synchronous dispatch
//   - restate:  durable handler invocations (recommended for prod)
//   - nats:     NATS JetStream
//   - sns:      AWS SNS + SQS
//   - pubsub:   GCP Pub/Sub
//   - cfqueues: Cloudflare Queues
//
// Publish is logically fire-and-forget from the writer's perspective.
// A returned error tells the caller (typically the outbox drain) that
// the publish did not succeed and the row should remain unmarked, so
// the next drain run will retry.
//
// Delivery semantics are at-least-once: subscribers must be
// idempotent.
type Publisher interface {
	Publish(ctx context.Context, env es.Envelope) error
}
