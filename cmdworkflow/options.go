package cmdworkflow

import (
	"github.com/google/uuid"

	"github.com/laenenai/eventstore/aggregate"
)

// HandleCmdOption configures one HandleCmd invocation. Used for bus-
// specific concerns (idempotency key, future tracing/correlation
// helpers) and pass-through to aggregate.HandleOption.
type HandleCmdOption func(*handleCmdConfig)

type handleCmdConfig struct {
	idempotencyKey string
	aggOpts        []aggregate.HandleOption
}

// WithIdempotencyKey derives a deterministic command_id from the
// supplied key. Two distinct effects:
//
//   - Workflow runtime adapter (Restate, DBOS): the adapter exposes
//     the key separately as the invocation id; a second call with
//     the same key joins the in-progress execution or returns the
//     cached result. That is the actual cross-call dedup mechanism
//     and is the runtime's responsibility, not the framework's.
//
//   - command_id determinism: every event produced by this HandleCmd
//     carries a command_id derived from key. Subscribers that dedup
//     on (command_id, output_index) — the ADR 0015 pattern — observe
//     stable ids across retries. This works regardless of runtime.
//
// What this option does NOT do on its own (Phase 1):
//
//   - It does not prevent two Append calls from succeeding with the
//     same command_id. The events table indexes command_id but does
//     not enforce uniqueness — duplicates would still write twice.
//     For real cross-call dedup, use a durable runtime (Restate /
//     DBOS); the inproc adapter is happy-path only.
//
// Pass the caller's request id (HTTP X-Request-Id, gRPC metadata,
// etc.) so upstream retries map cleanly to a single workflow
// invocation in the runtime.
func WithIdempotencyKey(key string) HandleCmdOption {
	return func(c *handleCmdConfig) {
		c.idempotencyKey = key
	}
}

// WithAggregateOption passes an aggregate.HandleOption through to the
// underlying Runtime.Handle call (correlation, causation, occurred-at,
// actor, etc.). Multiple may be supplied.
func WithAggregateOption(opt aggregate.HandleOption) HandleCmdOption {
	return func(c *handleCmdConfig) {
		c.aggOpts = append(c.aggOpts, opt)
	}
}

// idempKeyNamespace is the UUIDv5 namespace for deriving deterministic
// command ids from string idempotency keys. Stable forever — changing
// it would re-key every existing client.
var idempKeyNamespace = uuid.MustParse("c0ffee00-0b05-4be7-9000-000000000000")

// deriveCommandIDFromKey produces a deterministic UUIDv5 from the
// caller's idempotency key. Same key always yields the same id.
func deriveCommandIDFromKey(key string) uuid.UUID {
	return uuid.NewSHA1(idempKeyNamespace, []byte(key))
}
