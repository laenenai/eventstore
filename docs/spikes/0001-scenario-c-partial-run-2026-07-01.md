# Spike 0001 — Scenario C partial run (5 of 7 days)

**Status:** Partial result — terminated early by mains power loss.
Owner: Pascal Laenen. Written up 2026-07-02.
**Companion to:** `docs/spikes/0001-laenen-tenancy.md` (§11.3 Scenario C),
`docs/spikes/0001-mac-studio-soak-runbook.md`.
**Raw log:** `$HOME/spike-0001-soak-fix-soak-shakeout-heartbeat-assertion-20260626-084442.log`
(240 heartbeats) + `…-stdout.log`.

This is the report the harness would have emitted via `ReportC`, hand-
assembled from the heartbeat log because the run never reached its clean
exit — a household power cut killed the Mac Studio at ~2026-07-01 09:0x,
120 hours (5.0 days) into the 168-hour (7-day) target. There is no clean
resume; scenario C is not checkpoint-able (runbook §"Recovery from
interruption").

## Run configuration

| Field | Value |
| --- | --- |
| Population | 1,000,000 tenants (one Append per tenant to seed) |
| Offered rate | 167 writes/sec sustained |
| Heartbeat | every 30 min |
| Postgres | `postgres:17-alpine` in Docker Desktop (64 GB allocation) |
| Migrations | through `00016_partition_state_layer.sql` — **PR #35 mitigated config** (partition state layer, per-partition autovacuum thresholds, `fillfactor=85`) |
| Soak start | 2026-06-26 08:45:52 +04:00 |
| Seed complete | +15m41s |
| Last heartbeat | 2026-07-01 09:01:35 +04:00, `elapsed=120h0m0s` |

**Early termination:** mains power loss at ~120h (host hard-off; no
graceful shutdown, no final `ReportC`, no markdown summary in TempDir).

## Headline result

**5 days at 1M tenants and 167 writes/sec, zero failed appends, no bloat
growth on the hot table, latency flat within target the whole time.** The
one blemish is a ~7-hour overnight tail-latency excursion that
self-recovered (see below). Nothing in the run trends toward failure; the
truncation cost us the last 2 days of confirmation, not a verdict in
doubt.

## Throughput and durability

| Metric | Value |
| --- | --- |
| Cumulative appends succeeded | **72,028,203** |
| Cumulative appends failed | **0** |
| Offered vs. achieved | 72,028,203 / 72,144,000 expected @167/s = 99.8% |
| WAL generated (cumulative, since lsn 0/0) | **3.056 TB** |
| Sustained WAL rate | ~13.3 GB / 30 min → **~639 GB/day** |

At the observed WAL rate the full 7-day run would have generated ~4.4 TB.
That is the number to size the storage/backup budget against — flagged for
the spike's "within storage budget" line item, which needs an explicit
budget to check against (see Open items).

## Append latency — steady state

Rolling 30-min window, representative of the ~113 non-excursion heartbeats:

| | p50 | p99 | p99.9 |
| --- | --- | --- | --- |
| Steady state | ~2.82 ms | ~3.9 ms | ~10–22 ms |

Against the scenario A targets (p50 < 20 ms, p99 < 100 ms) the write path
ran ~7× under on p50 and ~25× under on p99 for the entire soak. p50 never
moved off ~2.8 ms — not during seed pressure, not during the excursion,
not at 5 days in.

### The overnight tail-latency excursion

From `elapsed=110h30m` to `117h30m` (2026-06-30 23:31 → 2026-07-01 06:31
local, ~7 hours) p99 rose to a plateau of **~21 ms** and p99.9 to
**~0.5–1.0 s**, then recovered on its own — p99 was back to 5.3 ms by
118h and 4.0 ms by 119h.

What did **not** change during the window:

- `p50` stayed flat at ~2.8 ms — median unaffected, this was a tail event.
- `fail` stayed 0; `window_n`/throughput held ~300k per 30 min.
- `state_cache.dead` stayed in its normal ~5k band; `state_cache.size_kb`
  stayed constant; WAL rate stayed linear; autovacuum counts advanced at
  their normal cadence.

So the excursion is invisible in every table-health metric the heartbeat
captures — it looks host-side, not database-side. The window is squarely
overnight, which points at macOS/Docker background activity (Time
Machine, Spotlight reindex, an OS maintenance task competing for disk
I/O) rather than anything in the write path. **This is the one thing to
investigate before the re-run** — pin the cause (host `fs_usage`/Activity
Monitor timeline, or move the Docker data volume off the Time Machine
target) so we can attribute or rule out a database-side tail. It did not
threaten the run: no failures, self-recovered, throughput intact.

## Table health — the question scenario C exists to answer

The §11.3 open question: *are per-partition 5% autovacuum thresholds +
`fillfactor=85` enough to keep autovacuum ahead of churn at 1M tenants
over 7 days?* Five days of evidence says **yes**, on this config.

**`state_cache` (the hot, mutable table — one row per tenant, updated
every write):**

| Metric | Over 120h |
| --- | --- |
| Live tuples | 1,000,000 (flat) |
| Dead tuples | bounded **4,675 – 5,458** (~0.5%), no upward trend |
| HOT-update ratio | **100%** throughout |
| Total relation size | **309,280 KB (~302 MB), constant — zero growth** |
| Bloat ratio | ≈ **1.0×** (target < 1.3×) |

This is the load-bearing result. Because every update is HOT
(`fillfactor=85` leaves in-page room), heap-only-tuple pruning reclaims
dead space in-page and the table never grows — dead tuples oscillate in a
tight band and full autovacuum barely needs to run. The hot projection
table stayed at 302 MB after 72M updates.

**`events` (append-only, largest table):**

| Metric | Over 120h |
| --- | --- |
| Live tuples | 1.3M → **73,028,886** |
| Dead tuples | **0** throughout (append-only) |
| Total relation size | **~47.7 GB** |
| Autovacuum cycles | 144 → **448** (+304 over 119.5h ≈ one every ~24 min) |

Largest table autovacuumed roughly every 24 minutes — comfortably under
the "< 1 h cycle" target, and no table went anywhere near 24 h without a
vacuum.

## Scenario C target scorecard (partial — 5 of 7 days)

| Metric | Measured (120h) | Target | Verdict |
| --- | --- | --- | --- |
| Autovacuum cycle on largest table (`events`) | ~24 min | < 1 h | ✅ |
| Bloat ratio on hot projection (`state_cache`) | ~1.0× | < 1.3× | ✅ |
| WAL generation rate (sustained) | ~639 GB/day (~4.4 TB / 7d projected) | within storage budget | ⚠️ needs a budget to check against |
| Tables without vacuum > 24 h | 0 | 0 (hard) | ✅ |
| Append failures | 0 | (implicit) | ✅ |
| Append p99 | ~4 ms steady | (scenario A: < 100 ms) | ✅ |

## Verdict

**Qualified PASS at 5/7 days, on the PR #35 (partition state layer)
config.** Every hard target held with wide margin; the mutable hot table
demonstrated the no-bloat behaviour the mitigation was designed to
produce. Two caveats keep it from being an unqualified 7-day pass:

1. **Truncated.** 120 of 168 hours. The failure modes scenario C hunts
   for (slow autovacuum debt accumulation, creeping bloat) are the kind
   that can appear late — 5 clean days is strong but not the 7 the SLO
   asks for.
2. **The overnight excursion** is unexplained. Almost certainly host-side,
   but "almost certainly" isn't the bar for a merge decision on PR #35.

## Open items / re-run checklist

- [ ] **Re-run the full 168h** to close the truncation. Same config,
      same host. Kick off when a ~8-day power-safe window is available.
- [ ] Before re-running, **root-cause the 23:30–06:30 tail excursion**:
      check Time Machine / Spotlight schedules, consider relocating the
      Docker data volume off the backup target, capture a host I/O
      timeline for one overnight.
- [ ] **Pin the WAL storage budget** so the ~639 GB/day line gets a real
      pass/fail instead of ⚠️.
- [ ] Consider a UPS for the Mac Studio before the next multi-day soak —
      this is the second early termination class the runbook calls out
      (§"Recovery from interruption") and the only one we can actually
      engineer away.
- [ ] Feed these numbers into `docs/spikes/0001-laenen-tenancy.md` §11.3
      (done — marked partial) and revisit §11.6 recommendation once the
      full run lands.

## Cross-references

- `docs/spikes/0001-laenen-tenancy.md` §11.3 (Scenario C table),
  §"Open question deferred to scenario C" (the autovacuum question this
  run answers).
- `docs/spikes/0001-mac-studio-soak-runbook.md` §"Recovery from
  interruption" — the no-resume policy this partial run ran into.
- `docs/adr/0023-state-cache-supersedes-snapshots.md` — why `state_cache`
  is the table whose bloat behaviour matters most.
- `estest/bench/scenario_c.go`, `estest/bench/reporter_c.go` — the harness
  and the report format this document mirrors.
</content>
</invoke>
