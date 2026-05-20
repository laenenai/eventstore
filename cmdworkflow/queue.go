package cmdworkflow

import "context"

// DefaultQueue is the named queue every command runs on unless an
// adopter explicitly routes it elsewhere via WithQueue.
//
// The framework guarantees QueueFromContext never returns "" — empty
// context, empty string, or unset value all resolve to DefaultQueue.
// Adapter authors should match by string against this constant when
// implementing default-queue lookup; never against "". A literal
// constant (rather than the empty-string-means-default convention)
// keeps adapter wiring symmetric: the same name appears in adopter
// config maps and in routing-decision branches, so a queue named
// "default" requires zero ceremony to declare or override.
const DefaultQueue = "default"

// queueKey is the typed context-value key for the routing hint. A
// distinct unexported type so external code cannot collide with
// the framework's slot — context.Value lookups type-discriminate.
type queueKey struct{}

// WithQueue attaches an execution-queue routing hint to ctx. Adapters
// interpret the hint per their native model — see ADR 0031:
//
//   - DBOS maps the name to a declared dbos.WorkflowQueue, so the
//     hint becomes an actual scheduling decision (concurrency caps,
//     rate limits, priority).
//   - Restate has no queue primitive (it serializes per virtual-object
//     key); the adapter logs the requested queue once for traceability
//     and otherwise no-ops.
//   - inproc executes synchronously regardless; the hint is logged.
//
// Empty string resolves to DefaultQueue. This is defensive: adopters
// who derive a queue name from a config lookup that came back blank,
// or who forget to set a field on a request DTO, should not silently
// fall off the queue map and into an "unknown queue" warning. The
// explicit constant keeps the failure mode loud (declare-a-queue or
// don't) and removes the empty-string footgun.
func WithQueue(ctx context.Context, name string) context.Context {
	if name == "" {
		name = DefaultQueue
	}
	return context.WithValue(ctx, queueKey{}, name)
}

// QueueFromContext returns the routing hint attached to ctx, or
// DefaultQueue if none is set. Never returns "" — adapter code can
// rely on the invariant when looking up a Queue value from a map.
func QueueFromContext(ctx context.Context) string {
	if ctx == nil {
		return DefaultQueue
	}
	v, ok := ctx.Value(queueKey{}).(string)
	if !ok || v == "" {
		return DefaultQueue
	}
	return v
}
