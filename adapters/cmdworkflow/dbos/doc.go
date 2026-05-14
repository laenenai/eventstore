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
package dbos
