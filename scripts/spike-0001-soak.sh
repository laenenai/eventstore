#!/usr/bin/env bash
#
# spike-0001-soak.sh — kick off the 7-day autovacuum soak (scenario C).
#
# Operator entry point for the runbook's "Kicking off the real 7-day"
# step. See docs/spikes/0001-mac-studio-soak-runbook.md for the full
# procedure, pre-flight settings (Energy Saver, Docker allocation, App
# Nap), and the Step A/B shakeout you should pass BEFORE running this.
#
# Why a script and not just the raw `go test` line: a 7-day commitment
# earns guard rails. This refuses to start if Docker is down or disk is
# tight (the two failure modes that waste days), names the log after the
# branch under measurement so the main-vs-PR#35 delta can't clobber
# itself, and prints the report path at the end.
#
# This run is NOT checkpoint-able. If the host or Postgres dies mid-soak
# it's a full re-run (runbook §Recovery). Launch it inside tmux/screen so
# a closed terminal doesn't take the soak with it:
#
#     tmux new -s soak './scripts/spike-0001-soak.sh'
#
# Overrides (any BENCH_SOAK_* already in the environment is respected;
# unset ones fall back to DefaultConfigC — 1M tenants / 168h / 167 q/s /
# 30m heartbeat):
#
#     BENCH_SOAK_TENANTS   population size            (default test: 1000000)
#     BENCH_SOAK_DURATION  soak length               (default test: 168h)
#     BENCH_SOAK_RATE      aggregate writes/sec       (default test: 167)
#     BENCH_SOAK_HEARTBEAT heartbeat interval         (default test: 30m)
#     BENCH_SOAK_LOG       heartbeat log path         (default: see below)
#     SOAK_TIMEOUT         go test -timeout value     (default: 200h)
#     SOAK_CAFFEINATE      caffeinate flags           (default: -d -i -s)
#     SOAK_YES=1           skip the confirmation prompt (for unattended runs)

set -euo pipefail

# --- locate the repo root so the script works from any CWD ------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

# --- identify the arm under measurement -------------------------------
# The spike is a DELTA: same host/tuning, scenario C on `main` vs
# `feat/postgres-partition-state-layer`. The branch belongs in the log
# name so a second run doesn't overwrite the first.
BRANCH="$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo unknown)"
BRANCH_SLUG="$(printf '%s' "$BRANCH" | tr '/ ' '--')"
STAMP="$(date +%Y%m%d-%H%M%S)"

: "${SOAK_TIMEOUT:=200h}"
: "${SOAK_CAFFEINATE:=-d -i -s}"
: "${BENCH_SOAK_LOG:=$HOME/spike-0001-soak-${BRANCH_SLUG}-${STAMP}.log}"
export BENCH_SOAK_LOG
STDOUT_LOG="${BENCH_SOAK_LOG%.log}-stdout.log"

# --- pre-flight: the two failure modes that waste days ----------------
fail() { printf 'ERROR: %s\n' "$1" >&2; exit 1; }

command -v go >/dev/null     || fail "go toolchain not on PATH"
command -v docker >/dev/null || fail "docker not on PATH"
docker info >/dev/null 2>&1  || fail "Docker daemon not reachable — start Docker Desktop and retry"

# Docker memory: the runbook asks for 48 GB; warn (don't block) under 45.
DOCKER_MEM_BYTES="$(docker info --format '{{.MemTotal}}' 2>/dev/null || echo 0)"
DOCKER_MEM_GB=$(( DOCKER_MEM_BYTES / 1024 / 1024 / 1024 ))
if [ "$DOCKER_MEM_GB" -lt 45 ]; then
  printf 'WARNING: Docker reports %sGB memory; runbook expects ~48GB.\n' "$DOCKER_MEM_GB" >&2
fi

# Disk: 1M tenants + 7 days of WAL + bloat headroom. Warn under 100 GB.
DISK_FREE_GB="$(df -g / 2>/dev/null | awk 'NR==2 {print $4}')"
if [ -n "${DISK_FREE_GB:-}" ] && [ "$DISK_FREE_GB" -lt 100 ]; then
  printf 'WARNING: only %sGB free on /; runbook wants >=100GB.\n' "$DISK_FREE_GB" >&2
fi

# Confirm the soak-tagged test compiles before committing to days of
# wall time — a typo in the bench package should fail in seconds, here.
printf 'Vetting bench package... '
go vet -tags soak ./estest/bench/ >/dev/null
printf 'ok\n'

# --- summary + confirmation -------------------------------------------
cat <<EOF

Spike 0001 — 7-day soak (scenario C)
  branch under measurement : $BRANCH
  tenants                  : ${BENCH_SOAK_TENANTS:-1000000 (default)}
  duration                 : ${BENCH_SOAK_DURATION:-168h (default)}
  rate                     : ${BENCH_SOAK_RATE:-167/s (default)}
  heartbeat                : ${BENCH_SOAK_HEARTBEAT:-30m (default)}
  go test -timeout         : $SOAK_TIMEOUT
  caffeinate flags         : $SOAK_CAFFEINATE
  heartbeat log            : $BENCH_SOAK_LOG
  stdout log               : $STDOUT_LOG
  Docker memory            : ${DOCKER_MEM_GB}GB
  disk free on /           : ${DISK_FREE_GB:-?}GB

This run is not checkpoint-able. Tail the heartbeat log to monitor:
  tail -f "$BENCH_SOAK_LOG"

EOF

if [ "${SOAK_YES:-}" != "1" ]; then
  read -r -p "Start the soak on branch '$BRANCH'? [y/N] " reply
  case "$reply" in
    y|Y|yes|YES) ;;
    *) echo "Aborted."; exit 1 ;;
  esac
fi

# --- launch -----------------------------------------------------------
# caffeinate keeps the Mac awake only for the lifetime of the soak
# process. tee mirrors go test's stdout (container/migration/verdict
# lines) to a file; the heartbeat snapshots go to BENCH_SOAK_LOG via the
# harness itself.
echo "Soak started $(date). Heartbeats -> $BENCH_SOAK_LOG"
set +e
caffeinate $SOAK_CAFFEINATE -- \
  go test -tags soak -timeout "$SOAK_TIMEOUT" \
    -run TestSoak_1M_7Day -v ./estest/bench/... \
  | tee "$STDOUT_LOG"
status=${PIPESTATUS[0]}
set -e

echo
echo "Soak finished $(date) with go test exit status $status."

# Surface the markdown report ReportC writes to a temp file.
REPORT="$(grep -Eo 'full report written to .*' "$STDOUT_LOG" | tail -n1 | sed 's/full report written to //')"
if [ -n "$REPORT" ]; then
  echo "Report: $REPORT"
fi

if [ "$status" -ne 0 ]; then
  echo "Non-zero exit — read $STDOUT_LOG and the Step C signals in the runbook." >&2
fi
exit "$status"
