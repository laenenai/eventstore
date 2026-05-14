// Package commandbus is the workflow-orchestrated command bus
// (ADR 0025). One generic command handler appends events through the
// aggregate runtime, then fans events out to registered subscribers
// — each governed by Mode (Sync/Async), MaxRetries, and OnExhausted
// (DLQ / Compensate / Drop).
//
// The package is transport-agnostic and runtime-agnostic. Plug in any
// WorkflowRuntime adapter (inproc for tests, Restate / DBOS for
// production) without changing handler or subscriber code.
//
// Status: Phase 1 of ADR 0025 — types, CommandBus, inproc adapter,
// per-subscriber DLQ. Saga primitives (Wait, Awakeable, Cancel) and
// the Restate adapter ship in subsequent ADRs.
package cmdworkflow
