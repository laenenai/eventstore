#!/usr/bin/env bash
set -euo pipefail

# Synchronized multi-module release.
# Tags the root module and every published submodule at the same version,
# using Go's module-path-prefixed tag convention.
#
# Usage: scripts/release.sh vX.Y.Z[-prerelease]

VERSION="${1:-}"
if [[ -z "$VERSION" ]]; then
  echo "usage: $0 vX.Y.Z[-prerelease]" >&2
  exit 1
fi

if ! [[ "$VERSION" =~ ^v[0-9]+\.[0-9]+\.[0-9]+(-[A-Za-z0-9.-]+)?$ ]]; then
  echo "error: version must be semver (e.g. v0.1.0 or v0.1.0-rc.1), got: $VERSION" >&2
  exit 1
fi

# Modules to tag. Empty string = root module. Keep this list in sync with go.work
# (minus examples/, which are not published).
MODULES=(
  ""
  "adapters/storage/postgres"
  "adapters/storage/sqlite"
  "adapters/authz/cedar"
  "adapters/cmdworkflow/restate"
  "adapters/cmdworkflow/dbos"
  "adapters/httpedge/connect"
  "cmd/esctl"
)

tag_for() {
  local mod="$1"
  if [[ -z "$mod" ]]; then echo "$VERSION"; else echo "$mod/$VERSION"; fi
}

# Preflight: clean tree, HEAD matches origin/main. The branch-name
# check is intentionally skipped — CI runs in detached HEAD on the
# main ref, which is the same commit but not the same branch handle.
# The SHA comparison below is the load-bearing safety check.
if [[ -n "$(git status --porcelain)" ]]; then
  echo "error: working tree is dirty" >&2
  git status --short >&2
  exit 1
fi
git fetch origin --tags --quiet
local_sha="$(git rev-parse HEAD)"
remote_sha="$(git rev-parse origin/main)"
if [[ "$local_sha" != "$remote_sha" ]]; then
  echo "error: HEAD ($local_sha) does not match origin/main ($remote_sha)" >&2
  exit 1
fi

# Preflight: tags must not already exist.
for mod in "${MODULES[@]}"; do
  tag="$(tag_for "$mod")"
  if git rev-parse -q --verify "refs/tags/$tag" >/dev/null; then
    echo "error: tag $tag already exists" >&2
    exit 1
  fi
done

# Preflight: each submodule must build and test cleanly. The root and submodules
# all participate in go.work, so a single 'go test ./...' per module is enough.
for mod in "${MODULES[@]}"; do
  dir="${mod:-.}"
  echo ">> go test in $dir"
  (cd "$dir" && go test ./...)
done

# Create all tags locally first, then push as a single refspec so the remote
# either sees the full set or rejects all of them.
created=()
for mod in "${MODULES[@]}"; do
  tag="$(tag_for "$mod")"
  git tag -a "$tag" -m "release $tag"
  created+=("$tag")
done

echo ">> pushing tags: ${created[*]}"
git push origin "${created[@]}"

echo ">> released $VERSION across ${#MODULES[@]} modules"
