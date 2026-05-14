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
package inproc
