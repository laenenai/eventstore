# ADR 0033: DBOS as the Default Command-Bus Adapter

- **Status:** Accepted
- **Date:** 2026-06-23
- **Amends:** ADR 0025 (Workflow-Orchestrated Command Bus), ADR 0026
  (Workflow Adapters — Restate and DBOS)

## Context

The framework ships two production command-bus adapters today: the
DBOS adapter (Postgres-native, library, embedded in your process)
and the Restate adapter (separate runtime, language-neutral
protocol, polyglot SDK). ADR 0026 originally treated them as equal
options with different strengths. That positioning was right at the
time, but two developments have shifted the cost-benefit:

1. **DBOS v0.16.0 landed `SqliteSystemDB` support.** ADR 0026 § 4
   documented the limitation that DBOS required Postgres for its
   journal, which forced adopters wanting "one binary, one file,
   zero infrastructure" into the inproc adapter for local dev. A
   spike against `dbos-transact-golang` v0.18.0
   (`adapters/cmdworkflow/dbos/sqlite_spike_test.go`) confirms that
   the full framework integration — `DBOSContext.Launch`,
   `RegisterWorkflow`, `RunWorkflow`, `HandleCmd`, the async-
   subscriber queue runner — works end-to-end against a SQLite
   handle. Spike runtime: ~100 ms with no Docker, no testcontainers,
   no Postgres. This collapses DBOS's footprint disadvantage vs.
   Restate (which itself requires a separate runtime process).

2. **The framework's wedge has clarified.** The eventstore framework
   targets regulated multi-tenant SaaS on Postgres-native (or
   SQLite-for-dev) databases. DBOS sits *inside* your database;
   Restate sits *beside* it. Treating both adapters as first-class
   means every cookbook recipe, every ADR addendum, every API change
   pays a "and here's how it looks in Restate" tax. That tax is
   worth paying if the abstraction is the main contribution; it's
   harder to justify when one adapter aligns with the wedge and the
   other does not.

This ADR formalises a position change: DBOS becomes the default,
recommended, actively-maintained command-bus adapter. Restate stays
in the tree but moves to community-maintained status.

The framework deliberately does NOT delete the Restate adapter in
this ADR (see § Decision 3 below). The intent is to retain
optionality without paying the dual-maintenance cost.

## Decision

### 1. DBOS is the default production command-bus story

The framework's README, cookbook recipes, ADR cross-references, and
the `examples/conversations` and `examples/cmdworkflow-dbos` worked
examples all position DBOS as the recommended production wiring. New
cookbook recipes that involve the command bus illustrate DBOS first;
Restate appears (if at all) as a "see also" section.

Concretely:

- `cmdworkflow/README.md` says "production wiring is DBOS"
- ADR 0026's text is amended to mark Restate as community-maintained
- Cookbook recipe 14 (cmdworkflow deployment) leads with the DBOS
  pattern; Restate moves to an "alternative deployments" section
- The framework's `task ci` continues to run the DBOS integration
  suite on every PR (per ADR 0026 amended); the Restate integration
  suite moves to nightly cadence (see § 3 below)

### 2. SQLite + DBOS is officially supported

ADR 0026 § 4 said:

> When SQLite is the eventstore: DBOS requires Postgres for its
> journal, so SQLite eventstore + DBOS workflows is **not a
> supported combination**.

That caveat retracts as of this ADR. The supported configuration is:

- One `*sql.DB` handle pointing at a SQLite file
- The eventstore SQLite adapter (`adapters/storage/sqlite`) uses
  that handle
- The DBOS context uses the same handle via
  `dbossdk.Config.SqliteSystemDB`
- DBOS lays its workflow journal tables alongside the framework's
  event log in the same SQLite file
- One file, one transaction story, one backup

Fixture: `adapters/cmdworkflow/dbos/testsupport/StartSQLite(t)`.
Validation: two passing tests in
`adapters/cmdworkflow/dbos/sqlite_spike_test.go` covering Sync
command flow and Async subscriber delivery through the DBOS queue
runner.

Cookbook recipe 14 gains a new section: "DBOS workflows on SQLite
for local dev" — same architecture as production, no Postgres, no
Docker.

### 3. Restate stays in the tree, marked community-maintained

The Restate adapter (`adapters/cmdworkflow/restate/`) is NOT deleted
by this ADR. Specifically:

- All source files remain in place
- All existing tests remain in place
- The adapter compiles against the current `cmdworkflow` API
- The codegen plugin's `runtime=restate` mode remains supported

What changes:

- A `STATUS.md` lands in `adapters/cmdworkflow/restate/` marking
  the adapter as **community-maintained**: it works today, the
  framework does not actively develop new features against it, and
  framework-side `cmdworkflow` API changes will land in DBOS first
- The Restate integration test job moves from "every PR" to
  "nightly" — it stays green on the current API surface, but PRs
  are not blocked on it
- New cookbook recipes lead with DBOS; existing Restate-specific
  recipes (cookbook 10, etc.) remain but get a banner pointing at
  the new default

### 4. The deletion gate is data-driven, not date-driven

The framework will reconsider deleting the Restate adapter when ALL
THREE conditions hold:

- (a) No external adopter (issue, PR, mailing-list post) has touched
  the Restate adapter for at least six months
- (b) Two consecutive `cmdworkflow` API changes have shipped where
  the Restate adapter only received mechanical adapter changes (no
  design input, no contributor sign-off)
- (c) The DBOS adapter has shipped at least one feature beyond the
  current `cmdworkflow.WorkflowRuntime` interface (sagas, durable
  timers, etc.) that the Restate adapter does not implement

Until all three conditions hold, the adapter stays. This is
deliberately conservative — deletion is a one-way door.

### 5. The codegen plugin keeps three modes

`cmd/protoc-gen-es-go/` keeps its `default`, `runtime=restate`, and
`runtime=dbos` modes. No code is removed by this ADR.

## Consequences

### Positive

- Adopters get one clear recommendation: DBOS. The
  "choose-your-runtime" question disappears for ~90% of use cases
- The "one binary, one file, zero infrastructure" story extends
  across the full command-bus stack — the conversations chat CLI
  can demonstrate DBOS workflows end-to-end on a single SQLite
  file
- `task ci` integration tests get faster (DBOS-only on every PR;
  Restate moves to nightly)
- Cookbook authors stop writing dual-runtime examples; recipe
  prose simplifies
- Framework feature velocity increases: new `cmdworkflow` design
  decisions optimise for one adapter, not two

### Negative

- Adopters with existing Restate deployments get a clear "not the
  primary path" signal. Their code keeps working, but the framework
  is honest about where attention is going
- The framework's "validates the `WorkflowRuntime` abstraction"
  story weakens — proving the interface fits both an embedded
  library and a separate runtime was real evidence of good design,
  and downgrading Restate reduces that signal
- Single-vendor risk concentrates on DBOS. If DBOS pivots, stalls,
  or breaks the API significantly, the framework's primary command
  bus is affected. Mitigation: the `cmdworkflow.WorkflowRuntime`
  interface is the framework's own; reimplementation against
  another runtime would be a few thousand LOC, not a rewrite
- The conservative deletion gate (§ 4) means the framework
  continues to pay maintenance cost on the Restate adapter for some
  time. This is intentional — the cost is bounded; the cost of
  deleting wrongly is unbounded

## Alternatives considered

### Delete the Restate adapter immediately

Maximally simplifies the codebase, ~2,000 LOC removed, ~30%
cookbook trim, single-adapter mental model from day one. Rejected
because deletion is irreversible; the framework has zero production
adopters today, so there is no data on which adapter adopters
actually pick; six months from now we might find Restate
constituencies we don't currently know about. Reconsider per § 4.

### Maintain both as equal first-class adapters

The status quo. Continues to pay dual-maintenance cost on every
`cmdworkflow` API change. Continues to make adopter onboarding
ambiguous ("which one do I pick?"). Continues to validate the
abstraction. Rejected because the maintenance tax exceeds the
abstraction-validation benefit at this stage of the framework's
development.

### Make Restate the default and DBOS the alternative

Considered for symmetry; rejected on the merits. DBOS aligns more
naturally with the framework's Postgres-native wedge, has a smaller
operational footprint (library vs. separate runtime), and (now)
supports SQLite. Restate's polyglot story is not value the
framework consumes.

### Introduce a third command-bus adapter (Temporal, NATS JetStream, etc.)

Out of scope. The framework's command-bus story is solved; the
question is which existing solution gets the primary slot. Adding a
third would re-open every question this ADR closes.

## Reference

- ADR 0025 — workflow-orchestrated command bus (the design Restate
  and DBOS both implement)
- ADR 0026 — workflow adapters (Restate + DBOS), now amended by
  this ADR
- ADR 0031 — execution queues (the queue-routing hint, still
  applies to both adapters)
- Cookbook 14 — `cmdworkflow` deployment patterns (will be updated
  to lead with DBOS)
- Spike: `adapters/cmdworkflow/dbos/sqlite_spike_test.go` and
  `testsupport/sqlite.go` — the validation that retracted ADR 0026
  § 4
