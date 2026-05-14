# Example: cmdworkflow-restate

A worked example of the cmdworkflow framework running against a real
Restate cluster. Mirror of `examples/cmdworkflow` (which uses the
inproc runtime), with the **only changes** being:

1. Workflow runtime swapped from `inproc.New()` to `cwrestate.New()`.
2. Workflow wrapped in the codegen-emitted
   `invoicev1restate.RestateService` and bound to Restate.
3. Commands enter via the Restate ingress, not direct `wf.HandleCmd`.

Everything else — the Decider, the Subscribers, the matrix knobs —
is identical. This is the load-bearing claim of ADR 0026: applications
swap runtime adapters without changing subscriber code.

## Run

```bash
cd examples/cmdworkflow-restate
go test ./...
```

The test spawns a `restatedev/restate:latest` container via
testcontainers, registers an in-process SDK HTTP server with Restate's
admin API, and routes ingress calls through the generated
`RestateService`. ~2-3 seconds end-to-end (after first image pull).

## What the test exercises

- **Sync read-model** (`ReadModel`) — UPSERTs the active-invoices
  view. Sync mode means `HandleCmd` waits for it before returning to
  the API caller; read-your-writes holds.
- **Async audit log** (`AuditLog`) — fires via `Spawn`; the API
  caller gets their response before the audit log catches up.
- **Full lifecycle**: Create → MarkPaid. Each command flows through
  the Restate ingress, journals one `restate.Run` for Append plus one
  `restate.RunAsync` per matched Sync subscriber, and returns the
  post-Decide state.

## OnExhausted policies on Restate

All four policy combinations work on the Restate adapter:

- `Drop` — silently abandon the event.
- `DLQ` — write to `subscriber_dlq` (the table on both PG + SQLite
  adapters). Insert happens from the outer `HandleCmd` context so
  the nested-`Run` restriction doesn't bite.
- `Compensate` — invoke `Subscriber.Compensate` to build a
  rollback command, then nest a HandleCmd call with a unique
  step-name prefix (`compensate:<sub>:<event>:`) to avoid journal
  collisions with the parent's "append" / "read-envelopes".

This example's subscribers use simple `Drop` policies for brevity,
but see `adapters/cmdworkflow/restate/restate_test.go` for end-to-end
tests of all four scenarios including `Sync + Compensate`.

## See also

- ADR 0025 — workflow-orchestrated command bus
- ADR 0026 — workflow adapters (Restate + DBOS)
- `cmdworkflow/README.md` — framework overview
- `examples/cmdworkflow` — sibling example using the inproc runtime
- `adapters/cmdworkflow/restate/testsupport` — test harness reused by
  this example (same code pattern your production app uses for the
  three-step start: HTTP/2 server, container, self-registration)
