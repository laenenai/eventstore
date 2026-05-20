package dbos

import (
	dbossdk "github.com/dbos-inc/dbos-transact-golang/dbos"
)

// Option configures a Runtime at construction. Adapter-specific knobs
// — queue routing, strict-mode policy — live here rather than on
// cmdworkflow.New because they're DBOS concepts the framework
// doesn't model. See ADR 0031 for the cross-adapter contract.
type Option func(*runtimeConfig)

// runtimeConfig collects the values supplied by Option callers before
// the Runtime is constructed. Held internal so the wire shape can
// evolve without breaking adopter code that passes Option values.
type runtimeConfig struct {
	queues map[string]*dbossdk.WorkflowQueue
	strict bool
}

// WithQueues wires named *dbossdk.WorkflowQueue values to the
// queue-name routing hint set via cmdworkflow.WithQueue. The adapter
// looks up the queue name from context via cmdworkflow.QueueFromContext
// on every queue-routed RunWorkflow dispatch (today: the async
// subscriber fan-out; adopter helpers may extend this to the outer
// command boundary).
//
// If "default" is not present in the map, the adapter falls back to a
// no-queue (immediate) execution under that name. Adopters who want
// the "default" name to map to an actual *dbossdk.WorkflowQueue with
// concurrency caps or rate limits should include it explicitly —
// keeping the override loud avoids the "silent default" footgun.
//
// The map is copied defensively; subsequent mutation of the adopter's
// map after New returns has no effect on Runtime behavior.
func WithQueues(qs map[string]*dbossdk.WorkflowQueue) Option {
	return func(c *runtimeConfig) {
		if len(qs) == 0 {
			c.queues = nil
			return
		}
		copied := make(map[string]*dbossdk.WorkflowQueue, len(qs))
		for k, v := range qs {
			copied[k] = v
		}
		c.queues = copied
	}
}

// WithStrictQueues toggles strict-mode queue resolution. Default
// (false) is non-strict: unknown queue names log a one-time WARN per
// unique name and fall back to the "default" queue. Strict mode (true)
// returns an error from ResolveQueue when the resolved name is not
// present in the declared queues — useful in deployments where the
// adopter wants every routing decision to be explicitly typed up-front
// rather than silently degraded.
//
// Strict mode never panics. Returning an error keeps the failure
// recoverable: HandleCmd surfaces it to the caller; the adopter can
// log + retry with a known queue or fall back to default explicitly.
func WithStrictQueues(strict bool) Option {
	return func(c *runtimeConfig) {
		c.strict = strict
	}
}
