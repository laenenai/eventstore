# Restate adapter — community-maintained

**Status as of 2026-06-23 (ADR 0033):** community-maintained.

## What that means

- **It works.** The current implementation compiles against the
  framework's `cmdworkflow` API, the integration tests pass, and
  adopters already running on it can continue without breakage.
- **The framework does not actively develop new features against
  it.** New `cmdworkflow` API decisions optimise for the DBOS
  adapter first (the default per ADR 0033). Restate parity is
  mechanical and follows in a separate change.
- **Integration tests run on a nightly cadence**, not on every PR.
  PRs are not gated on Restate integration green. (The Restate
  unit-level tests still run on every PR; only the
  testcontainer-backed `restate`-tagged suite moves to nightly.)
- **External contributions are welcome and triaged**, but the
  framework does not commit to maintainer-driven feature
  development.

## Why

ADR 0033 explains the strategic context in detail. The short
version: DBOS aligns with the framework's Postgres-native wedge,
supports SQLite for zero-infrastructure local dev (since DBOS Go
SDK v0.16.0), and embeds in your application binary rather than
requiring a separate runtime process. Carrying both adapters as
equal first-class options taxed every cookbook recipe and ADR
addendum with a "and here's the Restate version" section; making
DBOS the default eliminates that tax.

Restate is not removed because deletion is a one-way door and the
framework today has no production adopters to draw deletion data
from. ADR 0033 § 4 documents the gate conditions for eventually
removing the adapter — all three must hold, and the test is
deliberately conservative.

## Using the Restate adapter today

- Wiring is unchanged; see this directory's `doc.go` and the
  `examples/cmdworkflow-restate/` worked example.
- The `cmdworkflow.WorkflowRuntime` interface this adapter
  implements is the same one DBOS implements; framework-level
  guarantees apply identically.
- File issues, PRs, and questions against the Restate adapter as
  usual; they will be triaged like any other contribution.

## See also

- ADR 0033 — DBOS as the Default Command-Bus Adapter (the decision
  this status reflects)
- ADR 0026 — Workflow Adapters — Restate and DBOS (the original
  parity-based positioning, now amended)
- `adapters/cmdworkflow/dbos/` — the default production adapter
