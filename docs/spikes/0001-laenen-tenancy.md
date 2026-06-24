# Spike 0001 — Eventstore tenancy at `idi_*` scale

**Status:** Planning (no measurements taken yet).
**Date opened:** 2026-06-24.
**Owner:** TBD.
**Hard prerequisite for:** ADR 0008 §2 Phase 12a in the laenen.ai
repo. Phase 12a substrate work does not start until this spike
concludes.
**Upstream brief:**
`laenen-ai/docs/spikes/0001_eventstore_tenancy_at_individual_scale.md`
(the requesting side; this document is the eventstore-side
execution plan + report).

## 1. Goal

Decide whether the framework's existing partition strategy (per
ADR 0007, 16 hash-partitions on `tenant_id` for `events` and
`subject_keys`) survives a 100× to 1000× cardinality jump from
"thousands of customer organisations" to "hundreds of thousands to
millions of end-user individuals" with per-tenant SLOs intact.

The decision is binary at the surface but admits three landing
shapes:

- **PASS:** ADR 0008 §2 stands; Phase 12a substrate work proceeds
  unchanged.
- **QUALIFIED PASS:** §2 stands with documented operational
  mitigations (e.g., "shard above 500K per database"). The
  mitigations land as Phase 12a implementation guidance.
- **FAIL:** §2 revisited. The brief enumerates four escape hatches
  (sharded virtual tenants; tier-split tenancy; free-tier asymmetric
  tenancy; Postgres-per-N-tenants horizontal sharding). The spike's
  recommendation ranks them against the observed failure mode.

## 2. Hypothesis

> The eventstore partition strategy handles `idi_*`-cardinality of
> **1M active tenants** with per-tenant write latency, projection
> back-pressure, autovacuum behaviour, and state-cache memory all
> within their existing SLOs.

## 3. Scope

The brief defines six scenarios (A–F). To keep the calendar
bounded and gate execution on findings, this plan splits them into
two phases:

### Phase 1 — mandatory (load-bearing scenarios)

These three test the load-bearing claims; a failure in any one is
sufficient to revisit ADR 0008 §2.

| ID | Scenario              | What it stresses                          |
| -- | --------------------- | ----------------------------------------- |
| A  | Steady-state at scale | Write path, state-cache RSS, pool sizing  |
| C  | Autovacuum behaviour  | Long-term storage health, bloat ratios    |
| E  | Tenant deletion       | GDPR compliance SLA, cascade integrity    |

### Phase 2 — gated on Phase 1 pass

These sharpen the qualified-pass narrative or stress secondary
paths. Phase 2 runs only if Phase 1 doesn't already produce a
clear FAIL.

| ID | Scenario              | What it stresses                          |
| -- | --------------------- | ----------------------------------------- |
| B  | Mass write burst      | Burst-handling, recovery, lock chains     |
| D  | Cold-tenant rehydration | Returning-user UX, state-cache miss path |
| F  | Burst projection rebuild | Schema-change operational feasibility   |

### Out of scope (per the brief)

- Performance of laenen.ai's specific aggregates (Notes, Cards,
  etc.) — downstream concerns once tenancy is validated.
- Network-layer scaling.
- Cost modelling beyond rough infrastructure provisioning for the
  spike itself.
- Multi-region replication.

## 4. Approach

Three sequential phases. Each gates execution of the next.

### Step 1 — Schema audit (2 days, no harness yet)

Catalog every table in `adapters/storage/postgres/migrations/`
against the spike's scenarios. Specifically:

- Which tables are hash-partitioned today, which are not. Currently
  known: `events` ✓, `subject_keys` ✓, `outbox` ✓. Suspected
  unpartitioned: `state_cache`, `projection_checkpoint`,
  `projection_dlq`, `processed_events`, `state_stream_subscribers`.
- What indexes exist and which become problematic at 1M-tenant
  cardinality (tenant_id leading vs trailing; partial vs full;
  composite ordering).
- Current autovacuum settings — defaults, or table-tuned?
- Tables that experience high per-tenant row churn (hot
  projections, state_cache UPSERTs, outbox INSERT/DELETE).

**Output:** a markdown table ranking each table against scenarios
A, C, E. Often the spike's recommendation is foreseeable from this
artifact alone; the audit is the highest-leverage de-risking step
in the whole plan.

### Step 2 — Smoke harness at 10K (1 week)

Build the harness skeleton at `estest/bench/` containing:

- Tenant provisioner with realistic distribution generator
  (90 % cold, 9 % warm, 1 % hot).
- Load-shape composer (steady-state, burst, deletion).
- Metric collector hooked into OTel + a small Prometheus push
  gateway or per-run JSON dump.
- Per-scenario reporter comparing measured values against the
  brief's SLO table.

Run scenario A at 10K only. Two outcomes:

- **Smoke passes cleanly** → invest the remaining 4–6 weeks in
  Phases 1 and 2.
- **Smoke surfaces a bottleneck at 10K** → likely a FAIL signal
  before any large-scale infrastructure spend. The recommendation
  draft starts here.

### Step 3 — Phase 1 (3 weeks calendar)

Run scenarios A, C, E at progressively larger tiers:

- A at 100K, 500K, 1M (managed Postgres required for 500K+).
- C runs continuously for 7 days at 1M-tenant scale (structural
  calendar constraint — cannot compress).
- E at 1M, 10K-tenants-deleted-in-parallel.

Phase 1's output is the load-bearing pass / qualified-pass / fail
signal.

### Step 4 — Phase 2 (1–2 weeks, only if Phase 1 doesn't FAIL)

Scenarios B (burst), D (cold rehydration), F (rebuild) at 1M
tenants. Output sharpens the qualified-pass narrative — which
mitigations apply, which scenarios remain comfortable, which sit
near the edge.

### Step 5 — Recommendation + write-up (3 days)

The final section of this document gets filled in: pass status per
scenario, bottleneck profile, ranked recommendation, harness path.
The harness itself lives in the framework repo as a maintained
benchmark suite — not deleted after the spike concludes.

## 5. Effort, calendar, cost

Honest estimates. The brief's "2 weeks engineer + 3 days harness"
read optimistic to me; this is the more realistic shape.

| Phase | Active engineering | Calendar |
| --- | --- | --- |
| Step 1 — schema audit | 2 days | 2 days |
| Step 2 — smoke harness at 10K | 5–7 days | 1 week |
| Step 3a — Phase 1 scenarios A + E at 100K/500K/1M | 5–6 days | 1–2 weeks |
| Step 3b — Phase 1 scenario C (7-day soak) | 2 days active | 1 week wall |
| Step 4 — Phase 2 (if reached) | 5–7 days | 1–2 weeks |
| Step 5 — write-up + harness polish | 2–3 days | half week |
| Investigation reserve | 5 days | concurrent |
| **Total** | **~25–30 active days** | **5–7 weeks** |

The 7-day autovacuum soak is a structural calendar constraint
that cannot be compressed. Same for generating ~280M synthetic
events (at the brief's distribution) and getting them onto disk —
hours per data-load run, not minutes.

### Infrastructure cost

| Tier | Where it runs | Cost (estimate) |
| --- | --- | --- |
| 10K (smoke) | testcontainers Postgres on dev hardware | $0 |
| 100K | testcontainers or a small managed Postgres | $0–50 |
| 500K | managed Postgres, ~32 GB RAM, 100 GB disk | $200–500 for spike duration |
| 1M + 7-day soak | managed Postgres, ~64 GB RAM, 200 GB disk, provisioned IOPS | $500–1000 |
| **Total** | | **$500–$1500** |

Multiple test runs across tiers, the 7-day soak, and retries on
failed runs all add. Budget $2K to be safe.

## 6. Likely outcome (predicted, with confidence)

Personal calibration — not measurements, just informed prior:

- **40% PASS** — partition strategy survives; mitigations are
  minor or absent. The most "everything works" outcome.
- **45% QUALIFIED PASS** — passes up to ~500K cleanly; above that
  needs sharding (escape hatch 4) or projection-table
  sub-partitioning (a framework-side amendment). ADR 0008 §2
  stands; implementation guidance documents the threshold.
- **15% FAIL** — autovacuum on hot projection tables falls behind,
  OR state-cache RSS (50 KB/tenant × 1M = 50 GB) doesn't fit the
  runtime memory ceiling under burst conditions. The brief's
  escape hatch 1 (sharded virtual tenants) is the most likely
  landing if this happens.

The bottleneck I'd bet hits first under sustained load:
**autovacuum on hot projection tables**. The framework's projection
tables aren't currently hash-partitioned — they grow as single
tables — and at 1M tenants × hot-projection write volume the
dead-tuple churn could outpace autovacuum's default thresholds.
This is testable cheaply at the 100K tier; if observed there, it
de-risks the 1M run dramatically (we know the failure mode and the
mitigation in advance).

## 7. Deliverables

When the spike concludes, this document carries:

1. **Pass / fail / qualified-pass per scenario**, with measured
   numbers vs the brief's SLO targets, in a results table.
2. **Bottleneck profile** — which subsystem each scenario stressed
   first, and at which scale tier it surfaced.
3. **Recommendation** — one of PASS / QUALIFIED PASS / FAIL with
   the ranked escape-hatch options where applicable.
4. **Harness location** — link to the `estest/bench/` packages, the
   load-shape composer, and the reporter. The harness ships as a
   `feat:` PR regardless of the spike's outcome — even a FAIL
   leaves the framework with benchmark infrastructure it doesn't
   have today.
5. **Mirror back to laenen.ai** — copy this document's
   Recommendation section into the laenen.ai repo's spike doc per
   the upstream brief's instructions.

## 8. Open decisions before kick-off

Before any of the engineering starts, three things need to be
nailed:

- **Owner.** "TBD" in the brief; "TBD" here. The brief estimates
  one engineer familiar with eventstore internals for ~2 weeks
  active; this plan estimates ~25–30 days across 5–7 weeks
  calendar. Who. Without an owner, this sits.
- **Cloud provider for the 500K+ tiers.** Neon paid? AWS RDS?
  GCP Cloud SQL? Decision affects the spike's cost basis and how
  closely the results generalize to production deployment shapes.
- **Scope commitment.** Phase 1 only (~3 weeks calendar, decisive
  go/no-go signal) vs. full six scenarios (5–7 weeks, more
  granular qualified-pass shape). The brief implies the full set;
  this plan's recommendation is Phase 1 first, Phase 2 gated on
  the result.

## 9. Risk and what could go sideways

- **The 7-day soak is wall-time gated.** No way to compress; if it
  surfaces an autovacuum issue at day 5, the remediation + re-run
  pushes calendar by another week.
- **Realistic distribution generation is non-trivial.** 280M events
  at three different access patterns (cold / warm / hot) takes
  careful sequencing — generating sequentially with one process is
  hours; parallelising too aggressively can saturate the same
  Postgres you're about to measure.
- **First-run will hit something.** Some surprise — a connection-
  pool setting, an unexpected lock, a partition-pruning bug — will
  consume a chunk of the investigation reserve. Plan for at least
  one re-run per scenario.
- **Single-data-point risk.** All measurements come from one
  Postgres deployment shape. A different deployment (different
  Postgres version, different storage tier, different runner)
  produces different numbers. The recommendation should state the
  exact configuration tested and caveat generalization explicitly.

## 10. Cross-references

- ADR 0007 — eventstore partition strategy (existing strategy
  being stress-tested).
- ADR 0008 §2 — laenen.ai tenancy model (the upstream commitment
  this spike validates; lives in the laenen.ai repo).
- Upstream brief:
  `laenen-ai/docs/spikes/0001_eventstore_tenancy_at_individual_scale.md`.
- Brief's "Options if the spike fails" — four escape hatches; this
  spike's recommendation ranks them against observed failure
  modes.

## 11. Results (to be filled in as we execute)

### 11.1 Schema audit (Step 1)

*Pending. Run by:* TBD. *Output:* will live in §11.1.

### 11.2 Smoke harness at 10K (Step 2)

*Pending. Harness lands at `estest/bench/`. Smoke results:* TBD.

### 11.3 Phase 1 — scenarios A, C, E

*Pending. Phase 1 runs after smoke passes.*

#### Scenario A — steady-state at scale

| Tier | append p50 | append p99 | snapshot p99 | proj lag p99 | state-cache RSS/tenant | pool waits p99 |
| ---- | ---------- | ---------- | ------------ | ------------ | ----------------------- | -------------- |
| 10K  | TBD        | TBD        | TBD          | TBD          | TBD                     | TBD            |
| 100K | TBD        | TBD        | TBD          | TBD          | TBD                     | TBD            |
| 500K | TBD        | TBD        | TBD          | TBD          | TBD                     | TBD            |
| 1M   | TBD        | TBD        | TBD          | TBD          | TBD                     | TBD            |
| Target | < 20 ms  | < 100 ms   | < 50 ms      | < 1 s        | < 50 KB                 | < 10 ms        |

#### Scenario C — autovacuum behaviour (7-day soak)

| Metric | Measured | Target |
| --- | --- | --- |
| Autovacuum cycle on largest table | TBD | < 1 h |
| Bloat ratio on hot projections | TBD | < 1.3× |
| WAL generation rate (sustained) | TBD | within storage budget |
| Tables without vacuum > 24h | TBD | 0 (hard) |

#### Scenario E — tenant deletion (10K parallel)

| Metric | Measured | Target |
| --- | --- | --- |
| Per-tenant delete time | TBD | < 5 s |
| Cascade to projections | TBD | < 30 s |
| Sustained throughput @ 10/s | TBD | maintained |

### 11.4 Phase 2 — scenarios B, D, F

*Pending. Phase 2 runs only if Phase 1 doesn't produce a clear
FAIL.* Tables analogous to §11.3.

### 11.5 Bottleneck profile

*Pending.* For each scenario where a target was missed, the
specific subsystem responsible (write path / autovacuum /
projection apply / state cache / connection pool / locking).

### 11.6 Recommendation

*Pending.* PASS / QUALIFIED PASS / FAIL with ranked escape-hatch
options where applicable.

### 11.7 Harness location

*Pending.* Path to `estest/bench/` packages once they land.
