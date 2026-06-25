# Spike 0001 — Eventstore tenancy at `idi_*` scale

**Status:** Audit complete (Step 1 of 4). Smoke + Phase 1 pending.
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

### 11.1 Schema audit (Step 1) — DONE 2026-06-25

The Postgres adapter ships 14 active migrations (00001–00015,
with 00008 reserved/skipped and 00010 dropping a deprecated table).
12 tables exist post-migration. The audit catalogs each against
the spike's scenarios (A=steady-state, C=autovacuum, E=deletion).

**Source of truth:**
`adapters/storage/postgres/migrations/0000{1..15}_*.sql`.

#### 11.1.1 Per-table audit

Legend: ✅ partitioned (hash on `tenant_id`, 16 partitions). ❌
single table. **C-risk** = autovacuum / churn risk at 1M tenants.
**E-risk** = right-to-erasure cost at 1M tenants.

| Table | Partitioning | Write frequency | Row growth at 1M tenants | C-risk | E-risk | Notes |
| --- | --- | --- | --- | --- | --- | --- |
| `events` | ✅ | Every command (1× per event) | Linear in events, 16 partitions ≈ 62.5K tenants each | Low | Low (cascading delete by tenant_id is partition-bounded) | PK `(tenant_id, stream_id, version)`. UNIQUE on `(tenant_id, event_id)`. Index on `global_position` is parent-level (btree across all 16). Append-only — no UPDATE churn. **Lowest concern.** |
| `unique_claims` | ✅ | Per claim-issuing command | Linear in claim count | Low | Low | Insert-only in steady state. Per-claim row, scoped delete on tenant erasure works via partition. |
| `subject_keys` | ✅ | Per new subject (rare); ForgetSubject UPDATE | One row per (tenant, subject); inactive | Low | Low | ForgetSubject is an UPDATE (zero the DEK), not delete — keeps the audit tombstone. Volume stays small. |
| `outbox` | ✅ | Every event (1× per event) | High insert + DELETE churn at drain time | **MEDIUM** | Low | Drained then deleted on a cadence. Hot insert path; partial index `WHERE published_at IS NULL` keeps the pending scan cheap. **At 1M tenants the cleanup pass becomes a hot autovacuum target.** |
| `state_cache` | ❌ | Every successful command (UPSERT) | One row per (tenant, stream); JSONB column rewritten in place | **HIGH** | **MEDIUM** | **Tier-1 read model; biggest concern.** Single unpartitioned table at 1M tenants holding the rolling state of every active stream. JSONB UPDATE churn drives autovacuum pressure. No fillfactor set (default 100) → HOT updates unlikely → bloat ratio likely > 1.3× under sustained burst write. **This is the table I'd bet hits the brief's autovacuum SLO first.** |
| `projection_checkpoint` | ❌ | Per projection batch (every IdleSleep, ~250ms-1s) | One row per (name, tenant) | **HIGH** | Low | Row count stays small (projections × tenants ≈ 10 × 1M = 10M rows max), but **HIGH WRITE FREQUENCY** — every projector tick UPDATEs its cursor. At 10 projections × 1M tenants × 1Hz = 10M UPDATEs/sec theoretical; realistic steady-state is far lower but still substantial. Default autovacuum thresholds (20% dead-tuple) means vacuum lags badly on this table. |
| `projection_dlq` | ❌ | Only on handler failures | Low (failure-only) | Low | Low | Failure path; expected near-zero in steady state. |
| `processed_events` | ❌ | Per `WithDedup` projection × event | Linear in (projections-using-dedup × events) | **MEDIUM** | Low | Opt-in dedup table. Adopters who enable it on hot projections face a large unpartitioned table at scale. Could become a row-count problem (not a churn problem — append-only). |
| `state_stream_subscribers` | ❌ | Per state-stream drain delivery | (subscribers × tenants × streams) | **MEDIUM** | **MEDIUM** | At 1M tenants × 3 state-stream subscribers × 5 streams = **15M rows in one unpartitioned table**. Erasure for one tenant requires DELETE across the index. |
| `subscriber_dlq` | ❌ | Only on subscriber failures | Low (failure-only) | Low | Low | Failure path; expected near-zero in steady state. |
| Sequences (`events_global_position_seq`) | n/a | Every event (advisory-lock-serialized) | n/a | n/a | n/a | ADR 0009. Single global hot path — every append takes `pg_advisory_xact_lock` to serialize. Sequence consumption itself is cheap; the lock is the throughput ceiling. **Not a row-count concern; possibly a contention concern under burst writes.** |

#### 11.1.2 Cross-cutting findings

**Finding 1 — `state_cache` is the leading C-risk.** Unpartitioned,
high-frequency UPSERT, JSONB-column UPDATE-in-place, no fillfactor
tuning. This is the single highest-leverage place to target the
spike's measurements. The brief's scenario C autovacuum SLO
(< 1 h cycle, < 1.3× bloat) is most likely to fail here first.

**Finding 2 — Four hot-write tables are unpartitioned.**
`state_cache`, `projection_checkpoint`, `processed_events`,
`state_stream_subscribers`. The framework's partition strategy
addresses the *event* path (events, outbox, subject_keys) but does
not extend to the projection / state-cache layer. ADR 0007 was
sized for an event-volume-dominated workload; the spike's tenancy
question reveals that the **state-cache layer** is the unpartitioned
side of the same workload.

**Finding 3 — Zero table-level autovacuum tuning.** No
`reloptions` clauses anywhere in the migrations. All tables use
Postgres defaults: `autovacuum_vacuum_scale_factor = 0.2` (vacuum
when 20% of rows are dead), `autovacuum_naptime = 60s`. For
`state_cache` and `projection_checkpoint` under steady burst load
at 1M tenants, defaults will almost certainly fall behind. The
spike should treat per-table tuning as a mitigation to MEASURE,
not a recommendation to ASSUME.

**Finding 4 — Advisory-lock contention is the burst-write ceiling,
not partition design.** `events_global_position_seq` is allocated
under `pg_advisory_xact_lock` (ADR 0009) — every append serializes
store-wide. This caps the absolute write rate regardless of how
many tenants are participating. The brief's scenario B (100K
writes/minute = ~1.7K writes/sec) is well within reach, but the
spike should measure the actual ceiling and document it as a
deployment-shape parameter.

**Finding 5 — Erasure cascade hits four unpartitioned tables.**
Right-to-erasure for one tenant needs DELETEs on `state_cache`,
`projection_checkpoint`, `processed_events`,
`state_stream_subscribers`. At 1M tenants, each of these is a
single table whose indexed delete pays a multi-million-row btree
walk per tenant. The brief's scenario E target (< 5 s per delete)
is plausible but **the order of magnitude depends on whether
these tables get partitioned for the spike**.

**Finding 6 — RLS adds per-query overhead.** Migration 00015
enables `FORCE ROW LEVEL SECURITY` on every tenant-scoped table
with policy `tenant_id = current_setting('app.tenant_id', false)`.
The planner can use the GUC value for predicate pushdown
(partition pruning works), but the policy adds CPU on every query.
**Magnitude is small per-query but compounds at high QPS.** Spike
should measure with RLS on (production shape) and once with RLS
off (delta attribution).

#### 11.1.3 Updated outcome prior (vs §6's pre-audit prior)

The audit shifts my prior. The dominant finding is the gap between
the partitioned event path and the unpartitioned state-cache layer:

- **30% PASS** (was 40%) — events path stays solid, but
  `state_cache` autovacuum behaviour under sustained burst is the
  variable that's hardest to predict from schema alone. PASS requires
  it landing within SLO without modification.
- **55% QUALIFIED PASS** (was 45%) — the schema audit clearly
  points at four hot tables needing either hash partitioning or
  aggressive autovacuum tuning. The qualified-pass recipe lands
  as the spike's recommendation: framework ships a migration that
  hash-partitions `state_cache`, `projection_checkpoint`,
  `processed_events`, `state_stream_subscribers`, plus per-table
  autovacuum overrides on the two hottest, plus fillfactor=85 on
  `state_cache`. ADR 0008 §2 stands with these mitigations
  documented as Phase 12a substrate work.
- **15% FAIL** (unchanged) — even with mitigations applied, the
  combined unpartitioned-table pressure at 1M tenants under burst
  exceeds what Postgres-as-configured can absorb on the deployment
  shape we test. Escape hatch 1 (sharded virtual tenants) becomes
  the recommendation.

#### 11.1.4 Phase 1 measurement priorities (informed by the audit)

The audit lets Phase 1's harness focus on the highest-leverage
measurements rather than uniform coverage:

1. **`state_cache` bloat ratio + autovacuum cycle duration** at
   10K, 100K, 500K, 1M tiers under steady-state write. This is the
   most important single measurement in the whole spike.
2. **`state_cache` HOT update ratio** — `pg_stat_user_tables.n_tup_hot_upd /
   n_tup_upd`. Predicts whether fillfactor tuning will move the needle.
3. **`projection_checkpoint` UPDATE rate + lag** as a function of
   how many projections are active. Cheap to measure; reveals the
   second-order pressure.
4. **Advisory lock wait time at `events_global_position_seq`**
   under scenario B's 100K-writes/minute burst. Sets the deployment-
   shape ceiling.
5. **Erasure cascade time at 1M tenants** (scenario E) with and
   without partitioning on the four hot tables. Establishes the
   per-tenant delete cost delta.

Items the audit explicitly de-prioritizes for Phase 1:

- Detailed measurements on `events` partitioned tables — partition
  strategy is solid; volume scales linearly; surprises unlikely.
- `unique_claims`, `subject_keys`, `outbox`, `projection_dlq`,
  `subscriber_dlq` measurements beyond row counts — these are
  either well-partitioned or low-volume and unlikely to be the
  bottleneck.

#### 11.1.5 Pre-Phase-1 recommendation

Before Phase 1 starts, draft a candidate mitigation migration
(`00016_partition_state_layer.sql`) that hash-partitions the four
unpartitioned hot tables AND tunes their autovacuum thresholds. The
Phase 1 harness then runs each scenario **twice**: once on
current main, once on the mitigation branch. Apples-to-apples
delta tells us exactly which mitigations earned their keep.

This roughly doubles the measurement work in Phase 1 but produces
the qualified-pass recipe directly (you can read the recommended
mitigations off the delta), turns the spike's output from
"directional" into "operationally concrete," and saves a full
re-run after the spike concludes.

Estimated additional effort: +3–5 days (the migration is small;
the doubled measurement runs reuse the harness). Highly worth it.

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
