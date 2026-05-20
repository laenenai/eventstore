// Package inproc implements cmdworkflow.WorkflowRuntime as a
// non-durable, in-process runtime. Steps execute synchronously;
// spawns launch goroutines. Failures propagate directly — no journal,
// no replay, no retry beyond what the framework's Workflow already
// performs.
//
// Use this for unit tests, examples, and during local development
// before you've wired Restate / DBOS. Do NOT use in production: a
// crash mid-workflow loses any pending Async work and any
// partially-completed Compensate flows.
//
// # Queue routing (ADR 0031)
//
// inproc has no scheduling model: every step runs synchronously on
// the caller goroutine, and Spawn launches a goroutine. The
// cmdworkflow.WithQueue routing hint therefore has no behavioral
// effect; the Runtime logs the requested queue name at DEBUG once
// per unique name for traceability, then no-ops. Tests can rely on
// this to validate that adopter code propagates the hint correctly
// before swapping in a DBOS adapter that actually applies it.
package inproc
