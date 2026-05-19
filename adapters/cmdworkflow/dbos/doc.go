// Package dbos implements cmdworkflow.WorkflowRuntime backed by the
// DBOS Go SDK (https://github.com/dbos-inc/dbos-transact-golang).
//
// DBOS is a library, not a separate runtime process — your app
// embeds DBOS in-process and registers workflows + steps at startup.
// The workflow journal lives in the same Postgres as your eventstore
// (in the `dbos` schema by default), so backups, transactions, and
// migrations co-exist with the application data.
//
// Mapping from WorkflowRuntime to DBOS primitives:
//
//   - Run     → dbos.RunAsStep[[]byte]  (durable journaled step)
//   - RunAsync→ dbos.Go[[]byte] + a channel-backed Future
//   - Spawn   → in-process goroutine (best-effort; not journaled by
//               DBOS — same Phase 2a limitation as the Restate adapter,
//               documented in ADR 0026 § 7)
//
// Bridging stdlib context.Context to dbos.DBOSContext happens via
// WithContext at the entry point of every DBOS workflow handler.
// See ADR 0026 § 3.
//
// # Queue routing (ADR 0031)
//
// Adopters declare named *dbossdk.WorkflowQueue values via the
// WithQueues constructor option. The cmdworkflow.WithQueue routing
// hint attached to ctx is resolved against this declaration on every
// queue-aware RunWorkflow dispatch — today, the async-subscriber
// fan-out emitted by the codegen Service's sendAsync.
//
// Unknown queue names degrade gracefully by default: WithStrictQueues
// is false (non-strict), so unrecognized names fall back to "default"
// with a one-time slog WARN per unique name. Adopters who want every
// routing decision to fail loudly set WithStrictQueues(true);
// ResolveQueue then returns ErrUnknownQueue rather than degrading.
//
// The "default" queue is implicit: if WithQueues includes no entry
// for "default", DefaultQueue resolution returns a nil
// *dbossdk.WorkflowQueue, signaling "no queue option — run
// immediately." This keeps the zero-config path identical to the
// pre-queue adapter behavior.
package dbos
