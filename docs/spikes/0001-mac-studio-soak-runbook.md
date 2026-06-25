# Spike 0001 — Mac Studio Soak Runbook

**Status:** Draft (2026-06-25). Owner: Pascal Laenen.
**Companion to:** `docs/spikes/0001-laenen-tenancy.md`.
**Purpose:** procedure for running the spike's scenario C (7-day
autovacuum soak at 1M tenants) on a Mac Studio M3 Ultra instead of
managed Neon. Amendment to the kick-off decision in §8 of the
spike doc.

## Why the Mac Studio is a legitimate venue

The spike's primary purpose is the merge / don't-merge decision on
PR #35. That's a **delta** measurement: same hardware, same
postgres tuning, run scenario X on `main` and on `feat/postgres-
partition-state-layer`, compare. Mac Studio gives a valid delta
because both runs share the host.

The Mac Studio M3 Ultra 96 GB addresses every concern that ruled
out a laptop:

| Laptop concern | Mac Studio M3 Ultra |
| --- | --- |
| Sleep on lid close | No lid. Disable display + system sleep in System Settings. |
| Thermal throttling | Cooling sustains M3 Ultra under continuous load. |
| Battery thermal | Always wall-powered. |
| "I want to use my computer" | Postgres + harness in Docker has bounded resource use; you can keep working. |
| Forced macOS updates | Disable automatic updates for the soak window. |

What the Mac Studio does NOT give us: numbers that generalize
directly to "what an adopter sees on Neon." That's an optional
follow-up (§ Optional Neon validation below).

## Pre-flight setup

Run through this once before the soak. ~10 minutes.

### 1. System settings

```text
System Settings → Energy Saver
  Prevent automatic sleeping when display is off:    ON
  Wake for network access:                           ON
  Start up automatically after power failure:        ON

System Settings → Software Update → Automatic Updates → "ⓘ"
  Install macOS updates:                             OFF  (during soak only)
  Install application updates from the App Store:    OFF  (during soak only)
  Install security responses and system files:       OFF  (during soak only)
```

Remember to turn these back on after the soak completes.

### 2. Docker Desktop allocation

Docker → Settings → Resources

```text
Memory:                48 GB    (96 GB host, leaves 48 GB for macOS + you)
Swap:                  8 GB
CPUs:                  16        (M3 Ultra has 24; leaves 8 for host)
Disk image size:       200 GB    (50-100 GB used by Postgres + WAL + bloat headroom)
```

Apply & Restart Docker.

### 3. Verify Docker

```sh
docker info | grep -E "Total Memory|CPUs|Server Version"
docker info | grep -E "Storage Driver"
```

Want to see: Total Memory ≥ 45 GB, CPUs = 16, Server Version 20.10+,
Storage Driver overlay2 or similar.

### 4. Disk space sanity check

```sh
df -h /
# Want: at least 100 GB free on the volume Docker uses
```

Mac Studio 96 GB shipped with at least 1 TB SSD; this should be
comfortable.

### 5. Disable App Nap for Docker Desktop and Terminal

App Nap can suspend background apps after extended idle, which
interrupts the soak.

```sh
defaults write -app "Docker Desktop" NSAppSleepDisabled -bool true
defaults write -app "Terminal" NSAppSleepDisabled -bool true
# Same for iTerm if you use it:
defaults write -app "iTerm2" NSAppSleepDisabled -bool true
```

### 6. Caffeinate the soak session (alternative to system sleep settings)

If you don't want to disable system sleep globally, run the soak
under `caffeinate` which prevents sleep only for the lifetime of
the soak process:

```sh
caffeinate -d -i -s -u -- go test -tags soak -timeout 200h -run TestSoak_1M_7Day -v ./estest/bench/...
```

Flags:
- `-d` prevent display sleep
- `-i` prevent idle sleep
- `-s` prevent system sleep on AC
- `-u` declare user activity (keeps screen on if you want monitoring)

## Running the soak

The 7-day soak is gated behind a build tag so it can't be
accidentally launched. Build tag: `soak`.

```sh
# Verify everything is in order with a 1M smoke first (~60 min)
BENCH_TIER=1m go test -timeout 90m -run TestSmoke_1M -v ./estest/bench/...

# If the 1M smoke looks healthy, launch the 7-day soak:
caffeinate -d -i -s -- go test -tags soak -timeout 200h \
  -run TestSoak_1M_7Day -v ./estest/bench/... \
  | tee /var/log/spike-0001-soak.log
```

The soak's expected wall time is 7 × 24 = 168 hours plus seed
(~2-3 hours at 1M tenants on M3 Ultra Docker). Build a buffer:
200 h timeout.

## Monitoring during the soak

The soak writes a heartbeat snapshot every 30 minutes to
`/var/log/spike-0001-soak.log` containing:

- Current append latency (rolling 5-min p50/p99)
- Cumulative successful + failed appends
- `pg_stat_user_tables` snapshot for each hot table
- `pg_total_relation_size` per table
- `n_dead_tup`, `autovacuum_count`, `last_autovacuum`

Recommended monitoring rhythm:

- **Day 0** (kick-off): tail the log for the first hour. Confirm the
  seed completes, the run loop starts, the heartbeat is firing.
- **Day 1-6**: glance once per day. Look for sustained increases
  in dead-tuple count, growing total_relation_size (signal of
  bloat the autovacuum isn't reclaiming), or sustained p99 drift.
- **Day 7**: review the final report. Soak ends; harness writes
  the markdown summary.

If the harness or Postgres dies mid-soak, the failure mode is "no
recovery, run again." A 7-day soak isn't checkpoint-able. Schedule
the kick-off when you can tolerate a re-run if something goes
wrong on day 5.

## What the soak measures

Per the spike brief (§Scenario C):

| Metric | Target | Source |
| --- | --- | --- |
| Autovacuum cycle on largest table | < 1 hour | `pg_stat_user_tables.last_autovacuum` deltas |
| Bloat ratio on hot projections | < 1.3× | `pg_total_relation_size / pg_relation_size` |
| WAL generation rate (sustained) | within storage budget | `pg_stat_wal.wal_bytes` deltas |
| Tables without vacuum > 24 h | 0 (hard) | `last_autovacuum > now() - interval '24h'` |

The soak runs both branches sequentially (or in two Mac Studio
runs if you'd rather):

1. **Baseline run on `main`** (current unmitigated state): 7 days
2. **Mitigated run on `feat/postgres-partition-state-layer`** (PR #35): 7 days

Total: 14 days for the full comparison. Or, accept the spike's
recommendation from a one-sided 7-day run on whichever branch is
considered the candidate.

## Recovery from interruption

If the soak gets interrupted (kernel panic, Docker crash, power
loss):

1. Capture the partial log: `cp /var/log/spike-0001-soak.log /var/log/spike-0001-soak-partial-day-N.log`
2. Tear down the testcontainer (it's stuck): `docker ps -a | grep postgres | awk '{print $1}' | xargs docker rm -f`
3. Free the Docker disk: `docker system prune --volumes`
4. Re-run from step 1 of the kick-off.

There is no clean resume. Spike measurements are not checkpoint-
able under the current harness.

## Optional Neon validation

The Mac Studio soak gives a valid go/no-go signal for the merge of
PR #35. To check that the numbers generalize to adopter
deployments on Neon (the framework's strategic target), run a
shorter scenario A at 1M on a paid Neon project:

- Cost: ~$50-100 for 1-2 hours of paid Neon time
- Output: confirms the absolute latency / autovacuum numbers are
  in the same ballpark as the Mac Studio measurements
- Result: 1-paragraph addendum to the spike report saying "Mac
  Studio numbers within X% of Neon paid tier for tier=1M scenario
  A; soak measurements considered representative."

This is optional. The spike's primary purpose (decide PR #35) is
served by the Mac Studio runs alone.

## What's NOT in this runbook

- **Scenario A and E at smaller tiers** (10K, 100K, 500K): use the
  `BENCH_TIER` env var on `go test ./estest/bench/...`. See the
  smoke harness package doc.
- **The PR #35 vs main delta capture**: run scenario A or C on
  both branches with the same Mac Studio + Docker config. The
  comparison runner (`bench.CompareRuns`) isn't shipped yet —
  follow-up commit will land it.

## Roll-back of Mac-specific tweaks

After the soak completes, restore default settings:

```text
System Settings → Energy Saver: restore defaults
System Settings → Software Update → Automatic Updates: restore defaults
Docker Desktop → Resources: revert if you want the previous allocation

defaults delete -app "Docker Desktop" NSAppSleepDisabled
defaults delete -app "Terminal" NSAppSleepDisabled
defaults delete -app "iTerm2" NSAppSleepDisabled  # if you set this
```

Schedule a calendar reminder for "soak complete, restore settings"
so this doesn't get forgotten.

## Cross-references

- `docs/spikes/0001-laenen-tenancy.md` — the spike's plan + report.
  §8 (kick-off decisions) needs amending to reflect the Mac Studio
  posture; see the doc PR amending it alongside this runbook.
- `estest/bench/smoke_test.go` — the harness's tier drivers
  (10K / 100K / 500K / 1M).
- `docs/adr/0009-postgres-global-position.md` — explains the
  advisory-lock single-writer ceiling that bounds the seed phase
  throughput.
