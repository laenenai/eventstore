# Spike 0001 — Scenario C soak: interim report

**Status:** In progress (run live). **Snapshot:** 2026-06-29 11:33 +04,
at **74h30m of 168h** elapsed (~44% complete).
**Arm under measurement:** partitioned state layer (PR #35,
`feat/postgres-partition-state-layer`), migration 00016 applied.
**Companion to:** [`0001-laenen-tenancy.md`](./0001-laenen-tenancy.md) §11.3
and the [Mac Studio soak runbook](./0001-mac-studio-soak-runbook.md).

> This is a live interim snapshot, not the final verdict. The 7-day run
> is still executing; the SLO scoring and the `main`-vs-PR#35 delta land
> when it concludes. Numbers below are current as of the snapshot time.

## What we are testing

A **7-day continuous soak** that drives a realistic, sustained write load
against a **1,000,000-tenant** Postgres event store and watches whether
the synchronously-updated `state_cache` table accumulates bloat (dead
tuples) faster than autovacuum can reclaim it — which would surface as
**append-latency drift over time**.

It is **not** a throughput benchmark. The offered rate is deliberately
held *below* saturation so the measurement isolates *per-Append cost as
state ages*, not queue depth.

## Why we are testing it

The soak is the evidence behind a specific merge decision: **PR #35**,
which hash-partitions the state-cache / projection layer. The measurement
is a **delta** — the same harness, on the same host, with the same
Postgres tuning, run once on `main` and once on the partition branch, then
compared. Same hardware on both arms is what makes the comparison
legitimate; absolute numbers don't generalize to managed Postgres (Neon),
but the *difference between arms* does.

The table under the microscope is **`state_cache`**. Per **ADR 0023**,
aggregate state is mirrored synchronously into `state_cache` inside the
append transaction, so **every `Append` issues an `UPDATE`** of that
tenant's row. Under Postgres MVCC each `UPDATE` leaves a dead tuple for
autovacuum to reclaim. The open question — deferred to this scenario from
the 100K comparison in §11.2.7 — is whether, at 1M tenants under sustained
update load over *days*, `state_cache` bloats without bound (dead tuples
climbing, table growing, HOT-update ratio collapsing, autovacuum falling
behind) and whether that degrades latency. If it does, that is exactly the
failure mode PR #35 is meant to remove.

## How the test works

**Population** (`estest/bench/tenants.go`). One million tenants in three
cohorts, mirroring real traffic where most tenants are idle:

| Cohort | Share | Behaviour during soak |
| --- | --- | --- |
| Cold | ~90% | seeded once, then never written again |
| Warm | ~9%  | a handful of writes |
| Hot  | ~1%  | the bulk of the writes |

Only hot + warm form the candidate pool during the soak; cold tenants sit
untouched. Concentrating churn on ~10% of rows is realistic and the worst
case for HOT-update locality.

**Load** (`estest/bench/scenario_c.go`). After a one-`Append`-per-tenant
seed phase (~16 min for 1M at ~1060/s), a fixed worker pool fed by a
token-bucket pacer drives a steady **167 writes/sec aggregate** for 168
hours. Each write is one `Append` with optimistic concurrency
(`ExpectedVersion`) that writes the event and updates `state_cache` in the
same transaction. 167/s sits just under the measured advisory-lock
serialization ceiling (~180/s on testcontainers), so the soak measures
per-Append cost as state ages, not saturation. If the system can't keep
up, the pacer drops ticks and cumulative counts fall below expected —
that drop is the saturation signal.

**Measurement.** Every 30 minutes a heartbeat snapshots the window's
latency percentiles (p50/p99/p999, then drains the sample buffer so memory
stays bounded), the `pg_stat` counters for `state_cache`, `events`, and
the projection/subscriber tables (live tuples, dead tuples, HOT-update %,
autovacuum cycle count, table size), and cumulative WAL bytes.

## Current results (snapshot @ 74h30m)

149 heartbeats captured. Soak process alive, **zero failures**.

| Signal | Value | Read |
| --- | --- | --- |
| Appends succeeded / failed | 44,775,728 / **0** | steady ~167/s, no failures |
| Append p50 | 2.82 ms | flat since start |
| Append p99 | 3.87 ms | flat |
| Append p999 | 19.4 ms | within its 17–35 ms oscillation band |
| `state_cache` live rows | 1,000,000 | fixed cardinality |
| `state_cache` dead tuples | 5,255 | flat (~5k all run) |
| `state_cache` HOT-update % | **100%** | every update stays in-page |
| `state_cache` autovacuum cycles | 128 | unchanged since seed — never needed |
| `state_cache` size | **309 MB** | **zero growth** despite 44.8M updates |
| `events` live rows / size | 45.8M / 29.8 GB | append-only linear growth |
| WAL generated (cumulative) | 1.83 TB | ~24.6 GB/h, steady |
| WAL retained on disk (`pg_wal`) | 1008 MB | bounded — checkpoints recycling |
| Host disk free | 227 GB | comfortable |
| Postgres data dir | 91 GB used / 773 GB free | comfortable |

### Trajectory — stability over time

The point of a soak is that the numbers *don't move*. Sampled every 12h:

| Elapsed | succ | p50 | p99 | p999 | `state_cache` dead | HOT% | autovac | `state_cache` size | `events` rows |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 0h30m | 300,588 | 2.55 ms | 3.91 ms | 9.6 ms  | 4,890 | 100% | 128 | 309 MB | 1.30M |
| 12h   | 7.21M   | 2.69 ms | 3.89 ms | 32.6 ms | 5,248 | 100% | 128 | 309 MB | 8.21M |
| 24h   | 14.43M  | 2.59 ms | 3.89 ms | 26.5 ms | 5,037 | 100% | 128 | 309 MB | 15.4M |
| 36h   | 21.64M  | 2.73 ms | 3.99 ms | 28.4 ms | 5,311 | 100% | 128 | 309 MB | 22.6M |
| 48h   | 28.85M  | 2.73 ms | 4.26 ms | 35.7 ms | 5,262 | 100% | 128 | 309 MB | 29.8M |
| 60h   | 36.06M  | 2.65 ms | 3.66 ms | 17.7 ms | 5,120 | 100% | 128 | 309 MB | 37.1M |
| 72h   | 44.27M  | —       | —       | —       | 5,201 | 100% | 128 | 309 MB | 44.3M |

`state_cache` size, dead-tuple count, HOT ratio, and autovacuum count are
**flat across the whole run**. Latency p50/p99 are flat; p999 oscillates
in a ~17–35 ms band with no upward drift. `events` grows linearly because
it is an append-only log.

## Interpretation — why `state_cache` does not change

This flat behaviour is the result the spike was built to test, and it has
a precise mechanism:

1. **Fixed cardinality.** `state_cache` is keyed `PRIMARY KEY (tenant_id,
   stream_id)` — one row per aggregate. The soak only `UPDATE`s existing
   rows (no aggregates are created during the soak), so `live` is pinned at
   1M and the table cannot grow by row count. (`events`, by contrast,
   `INSERT`s a row per Append and therefore grows linearly.)

2. **Every update is a HOT (Heap-Only Tuple) update** — `hot=100%`. An
   `UPDATE` qualifies as HOT, staying on the same page with no index work,
   when (a) no indexed column changes and (b) the new tuple version fits on
   the same page. Both hold here:
   - The updated columns (`state`, `version`, `updated_at`) are in neither
     the PK nor the `(tenant_id, type_url, stream_id)` index, so indexes
     are untouched.
   - Migration 00016 sets **`fillfactor = 85`** on each `state_cache`
     child partition, leaving 15% page headroom precisely so the new
     version fits in place; the benchmark's counter state is small and
     fixed-size, so it always does.

   The dead version is then reclaimed by **HOT pruning** on the next page
   access — no full vacuum required — so the heap file never extends.

3. **Autovacuum never fires.** HOT pruning keeps dead tuples (~5k) an order
   of magnitude below the trigger threshold
   (`autovacuum_vacuum_scale_factor = 0.05 × 1M ≈ 50k`), so autovacuum
   simply never runs on `state_cache` during the soak. The `av=128` count
   is leftover from the seed phase.

**Caveat for generalizing.** This near-perfect result leans on the
benchmark's **constant-size state**. A real aggregate whose state grows
over its lifetime would eventually produce a version too large for the 15%
page headroom → HOT breaks → the new version moves to another page → index
update + bloat + autovacuum activity. The soak proves the *mechanism* is
sound; adopters with large or growing JSONB state should expect a lower
HOT ratio and some real autovacuum work.

## What "pass" looks like / decision criterion

For the partitioned arm over the full 7 days: `state_cache` dead tuples
bounded, table size flat, HOT% high, append latency flat, zero append
failures, retained WAL bounded. At 74h30m all six hold. The merge decision
on PR #35 is the comparison of this arm against the `main` arm, not either
arm's absolute numbers — that comparison is pending the run's completion.

## Reproducing

```sh
# Full 7-day run (guard-railed wrapper; launch inside tmux):
task soak

# Quick code-path validation (not a real soak):
go test -tags soak -run TestSoak_CodepathSmoke ./estest/bench/...
```

Heartbeats append live to `$BENCH_SOAK_LOG`
(`~/spike-0001-soak-<branch>-<stamp>.log`); the full per-heartbeat
trajectory is written as a markdown report when the run finishes. Harness:
`estest/bench/scenario_c.go`, `reporter_c.go`, `tenants.go`,
`soak_test.go`.
