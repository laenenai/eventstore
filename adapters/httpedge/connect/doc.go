// Package connectedge is a thin runtime helper that adapts a
// cmdworkflow.Workflow[S, C] command bus to a Connect-go (HTTP) RPC.
//
// It is intentionally not a code generator. The framework's proto
// commands evolve with the aggregate (per ADR 0013), but an external
// HTTP contract has stricter rules — generating the edge from the
// internal commands would couple a public API to internal evolution.
//
// Instead, callers wire one Dispatch per RPC, passing a small decode
// callback that:
//
//   - extracts the StreamID (typically from annotated proto fields),
//   - lifts the request message into the aggregate's sealed Command
//     sum type.
//
// This keeps the public API shape (request/response messages) free to
// diverge from the internal command shape, while the helper handles
// the mechanical bits: idempotency-key passthrough, dispatch, and
// framework-error → Connect-code mapping.
//
// See cookbook recipe 15 for the wiring pattern.
package connectedge
