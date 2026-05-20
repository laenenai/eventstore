// Package restate implements cmdworkflow.WorkflowRuntime backed by
// the Restate Go SDK (https://github.com/restatedev/sdk-go).
//
// Two-method interface mapped to SDK primitives:
//
//   - Workflow.Run   → restate.Run[[]byte](rc, fn)
//   - Workflow.Spawn → restate.ServiceSend(rc, ...).Send(input)
//
// Bridging stdlib context.Context to restate.Context happens via
// WithContext at the entry point of every Restate handler. See
// ADR 0026 for the full design.
//
// # Queue routing (ADR 0031)
//
// Restate's execution model is virtual-objects: per-key serial
// execution scheduled by the Restate runtime. There is no separate
// "queue" primitive — the key (typically a stream id or aggregate id)
// already provides the isolation and ordering that DBOS uses queues
// for. The framework's cmdworkflow.WithQueue routing hint therefore
// has no behavioral effect on this adapter; the Runtime logs the
// requested queue name at DEBUG once per unique name for
// traceability, then no-ops.
//
// Adopters whose performance / SLA design depends on queue-style
// routing should partition aggregates across virtual-object keys to
// achieve the same effect — different keys execute independently. The
// cross-adapter portability story (ADR 0031) is that command code
// runs unchanged on either adapter, with queues a soft performance
// hint where they're modeled and a no-op where they're not.
package restate
