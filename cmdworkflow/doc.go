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
//
// Routing hint: adopters can attach an advisory execution-queue name
// to ctx via WithQueue(ctx, name). Each adapter interprets the hint
// per its native model — DBOS routes to a declared dbos.Queue; Restate
// and inproc log it for traceability and otherwise no-op. See ADR 0031
// for the cross-adapter contract. The hint is advisory: code that
// depends on queue routing for correctness (rather than for
// performance / isolation) violates the contract and will degrade
// silently on adapters that don't model queues.
package cmdworkflow
