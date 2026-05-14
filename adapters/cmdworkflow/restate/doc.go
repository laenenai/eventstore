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
package restate
