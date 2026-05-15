// Package cmdworkflow is the workflow-orchestrated command bus
// (ADR 0025). One generic command handler appends events through the
// aggregate runtime, then fans events out to registered subscribers
// — each governed by Mode (Sync/Async), MaxRetries, and OnExhausted
// (DLQ / Compensate / Drop).
//
// Subscribers receive a whole command-batch in one Handle call: the
// envelope slice, the typed post-Decide state, and the typed events.
// See ADR 0029 for the per-batch delivery model.
//
// The package is transport-agnostic and runtime-agnostic. Plug in any
// WorkflowRuntime adapter (inproc for tests, Restate / DBOS for
// production) without changing handler or subscriber code.
package cmdworkflow
