package cmdworkflow

import (
	"context"
	"path"
	"slices"
	"time"

	"github.com/laenenai/eventstore/es"
)

// DeliveryMode controls whether the bus blocks the caller on the
// subscriber's completion.
type DeliveryMode int

const (
	// Sync: HandleCmd does not return until the subscriber settles
	// (success, exhaustion → DLQ / Compensate / Drop).
	Sync DeliveryMode = iota
	// Async: the subscriber runs as a spawned child workflow.
	// HandleCmd returns as soon as it has been scheduled.
	Async
)

func (m DeliveryMode) String() string {
	switch m {
	case Sync:
		return "Sync"
	case Async:
		return "Async"
	default:
		return "Unknown"
	}
}

// ExhaustedPolicy controls what happens after MaxRetries is reached.
type ExhaustedPolicy int

const (
	// DLQ: write the failed command-batch into subscriber_dlq for
	// operator action. The most common policy for both Sync and
	// Async subscribers.
	DLQ ExhaustedPolicy = iota
	// Compensate: invoke Subscriber.Compensate to produce a
	// compensating command, append it through the bus (nested
	// HandleCmd call), and consider the original batch settled.
	// Saga semantics. Requires Compensate to be set.
	Compensate
	// Drop: silently abandon. No DLQ, no compensation.
	// Best-effort delivery only.
	Drop
)

func (p ExhaustedPolicy) String() string {
	switch p {
	case DLQ:
		return "DLQ"
	case Compensate:
		return "Compensate"
	case Drop:
		return "Drop"
	default:
		return "Unknown"
	}
}

// EventFilter declaratively narrows which envelopes a subscriber
// receives. Evaluated by the framework *before* any WorkflowRuntime
// step is created — unmatched subscribers cost zero journal entries.
//
// All non-zero fields must match (AND). Within a slice, any element
// matches (OR). A nil Custom is ignored. An entirely-empty filter
// matches every envelope.
type EventFilter struct {
	// TypeURLs is the set of envelope.TypeURL values to accept.
	// Empty = accept any type.
	TypeURLs []string

	// StreamGlob is a shell-style glob matched against the
	// envelope's canonical stream id (e.g., "invoice:*", "*:vip-*").
	// Empty = accept any stream.
	StreamGlob string

	// Tenants narrows to specific tenant ids. Empty = accept any.
	Tenants []string

	// Custom is an escape-hatch predicate evaluated last. Nil = no
	// additional gate.
	Custom func(env es.Envelope) bool
}

// Matches reports whether env passes the filter.
func (f EventFilter) Matches(env es.Envelope) bool {
	if len(f.TypeURLs) > 0 && !slices.Contains(f.TypeURLs, env.TypeURL) {
		return false
	}
	if len(f.Tenants) > 0 && !slices.Contains(f.Tenants, env.TenantID) {
		return false
	}
	if f.StreamGlob != "" {
		// path.Match implements shell-style globs; the canonical
		// stream id is "<type>:<id>" so it's a single segment.
		// path.Match treats "/" specially, which our canonical form
		// avoids by construction.
		ok, _ := path.Match(f.StreamGlob, env.StreamID.Canonical())
		if !ok {
			return false
		}
	}
	if f.Custom != nil && !f.Custom(env) {
		return false
	}
	return true
}

// Subscriber describes one registered consumer of a command's effect
// on the bus. The framework calls Handle ONCE per command, passing
// the envelope batch (after Filter narrowing), the typed post-Decide
// state, and the typed events index-aligned with the envelopes.
//
// Type parameters:
//   - S: aggregate state. Matches the Workflow's S.
//   - C: command sum type. Matches the Workflow's C.
//   - E: event sum type. Matches the Workflow's E.
//
// Why per-batch and not per-event: projections that need full
// post-Decide state would otherwise re-derive it from event payloads
// (duplicating Decider logic) or call Load themselves (an extra DB
// roundtrip per event). With per-batch delivery the framework hands
// over the state it already computed for the in-tx state_cache write,
// and Handle becomes one call regardless of how many events the
// command emitted. See ADR 0029.
type Subscriber[S, C, E any] struct {
	// Name is a stable identifier — journal entries use it as a
	// prefix, DLQ rows store it. Must be unique within the bus.
	Name string

	// Filter narrows the set of envelopes this subscriber receives.
	// Filter is applied per-envelope; envs that survive form the
	// batch passed to Handle. If no envelopes survive the filter,
	// Handle is not called at all (no journal entry).
	Filter EventFilter

	// Mode controls whether HandleCmd blocks on the subscriber's
	// completion (Sync) or returns once it's scheduled (Async).
	Mode DeliveryMode

	// MaxRetries caps the failure budget per command-batch. The
	// budget is consumed by the BATCH, not by individual events
	// within it — one Handle call counts as one attempt.
	//   0  = single attempt, no retry
	//   N  = up to N retries after the first attempt
	//  -1  = retry forever (workflow replay re-attempts on every run)
	MaxRetries int

	// OnExhausted controls behavior when MaxRetries is exceeded.
	OnExhausted ExhaustedPolicy

	// AttemptTimeout caps a single Handle invocation. Zero = no cap
	// (inherits the caller's context).
	AttemptTimeout time.Duration

	// Handle is invoked once per command, with:
	//   - envs:   envelopes from this command that matched Filter.
	//             Skipped (subscriber not called) if empty.
	//   - state:  the post-Decide state — the same value the
	//             runtime's StateCodec persisted to state_cache in
	//             the same transaction as the events.
	//   - events: typed events matching Filter. envs[i] ↔ events[i].
	//
	// Handle must be idempotent on (env.EventID, env.EventID, …)
	// for the whole batch — at-least-once delivery is the
	// framework's contract.
	Handle func(ctx context.Context, envs []es.Envelope, state S, events []E) error

	// Compensate produces a compensating command when OnExhausted is
	// Compensate and the retry budget is exhausted. Required for
	// Compensate policy; ignored otherwise. Receives the same batch
	// that Handle did.
	//
	// The returned command is appended through the bus (nested
	// HandleCmd). Compensate runs with a fresh, non-cancellable
	// context (context.WithoutCancel) so a caller timeout cannot
	// leave the aggregate in a half-rolled-back state.
	Compensate func(ctx context.Context, envs []es.Envelope, state S, events []E) (C, error)
}
