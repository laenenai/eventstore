# ADR 0021: JSONB Storage on SQLite (3.45+)

- **Status:** Proposed
- **Date:** 2026-05-14
- **Amends:** ADR 0020 (Projections and Read Models — specifically the
  `state_cache` storage format choice for SQLite). Not yet shipped.

## Context

ADR 0020 chose JSONB for the Tier 1 `state_cache` column. On Postgres
that translates directly: native `JSONB` type, binary storage, the
JSON path operators (`->`, `->>`) work without parsing the column on
every read.

On SQLite the column is currently `TEXT` — the JSON1 extension's
functions (`json_extract`, `json_set`, etc.) parse the text on every
call. Performance is acceptable at small scale but rebuilds the parser
state on each read.

SQLite 3.45.0 (released January 2024) added **JSONB BLOB storage**: a
binary on-disk format with `jsonb_extract`, `jsonb_set`, and friends
that skip the parse step. Same functions, smaller storage, faster
reads/writes.

The libSQL fork (which Turso uses) tracks upstream SQLite. As of late
2025, libSQL has merged the JSONB functions in recent releases. The
exact version pin matters per Turso/sqld release.

This ADR documents the optimization and the migration path. It does
**not** mandate the change — Tier 1 works today as TEXT/JSON1, and the
framework's public API returns raw bytes either way.

## Decision

### Adopt JSONB BLOB storage for `state_cache` on SQLite

When the deployment target's SQLite (or libSQL) version supports
JSONB BLOB:

- `state_cache.state` migrates from `TEXT NOT NULL` to `BLOB NOT NULL`.
- Writes wrap input as `jsonb(?)` to convert protojson text into the
  binary form on insert.
- Reads use the binary as-is; the framework's `StateCacheReader`
  contract is unchanged (the API returns `[]byte`, callers unmarshal
  with the same `protojson.Unmarshal` that worked before).
- User-side filtered queries (cookbook recipe 07) switch
  `json_extract(state, '$.field')` → `jsonb_extract(state, '$.field')`.

The migration is **adapter-internal**. No framework public API change.
No proto change. No application code change above the `state_cache`
query layer.

### Pin the minimum supported SQLite/libSQL version

The framework declares a minimum supported SQLite/libSQL version
high enough to guarantee JSONB BLOB availability:

- **SQLite ≥ 3.45.0** (Jan 2024) — direct deployments.
- **libSQL ≥ {first-version-with-JSONB}** — confirm against the Turso
  release notes when scheduling the migration.

If a deployment target's pinned version is older, it stays on the
existing TEXT JSON1 path. Both are produced from the same migration
sequence (TEXT remains the historical column type; JSONB is a forward
migration).

### Migration strategy

A new framework migration (numbered after the current
`00007_processed_events.sql`) does:

```sql
-- +goose Up

-- Re-encode state_cache.state from TEXT JSON to BLOB JSONB. SQLite's
-- jsonb(json_text) converts safely; the column type change requires
-- a copy because BLOB ≠ TEXT affinity.
CREATE TABLE state_cache_new (
    tenant_id   TEXT    NOT NULL,
    stream_id   TEXT    NOT NULL,
    type_url    TEXT    NOT NULL,
    state       BLOB    NOT NULL,
    version     INTEGER NOT NULL,
    terminal    INTEGER NOT NULL DEFAULT 0,
    updated_at  TEXT    NOT NULL,
    PRIMARY KEY (tenant_id, stream_id)
);

INSERT INTO state_cache_new
SELECT tenant_id, stream_id, type_url, jsonb(state), version, terminal, updated_at
FROM state_cache;

DROP TABLE state_cache;
ALTER TABLE state_cache_new RENAME TO state_cache;

-- Recreate any indexes (e.g., state_cache_by_type_idx).
CREATE INDEX state_cache_by_type_idx
    ON state_cache (tenant_id, type_url, stream_id);

-- +goose Down

-- Reverse: BLOB jsonb → TEXT json.
CREATE TABLE state_cache_old (
    tenant_id   TEXT    NOT NULL,
    stream_id   TEXT    NOT NULL,
    type_url    TEXT    NOT NULL,
    state       TEXT    NOT NULL,
    version     INTEGER NOT NULL,
    terminal    INTEGER NOT NULL DEFAULT 0,
    updated_at  TEXT    NOT NULL,
    PRIMARY KEY (tenant_id, stream_id)
);
INSERT INTO state_cache_old
SELECT tenant_id, stream_id, type_url, json(state), version, terminal, updated_at
FROM state_cache;
DROP TABLE state_cache;
ALTER TABLE state_cache_old RENAME TO state_cache;
CREATE INDEX state_cache_by_type_idx
    ON state_cache (tenant_id, type_url, stream_id);
```

Applied via goose like any other migration. For large tenants, the
`INSERT INTO ... SELECT` walks the full table — schedule the migration
during a quiet window or stage by tenant.

### Update the SQLite UpsertStateCache query

```sql
-- Before:
INSERT INTO state_cache (...) VALUES (..., ?, ...)  -- state is TEXT param

-- After:
INSERT INTO state_cache (...) VALUES (..., jsonb(?), ...)  -- wrap as JSONB
```

The Go-side parameter stays `string` (the protojson bytes); the
adapter SQL wraps it.

### Reads stay simple

`SELECT state FROM state_cache WHERE ...` returns the BLOB. The
adapter casts to `[]byte`; callers unmarshal via `protojson.Unmarshal`
unchanged — protojson reads either JSON text or BLOB JSONB
identically because the wire format of `jsonb(json)` decodes back
through `json(jsonb)`.

If a query needs to extract individual fields server-side:

```sql
-- Old (still works):
SELECT json_extract(state, '$.status') FROM state_cache WHERE ...

-- New (faster — skip parse):
SELECT jsonb_extract(state, '$.status') FROM state_cache WHERE ...
```

Both functions exist on JSONB-supporting SQLite. Migrate at leisure.

## Consequences

**Positive:**

- **Faster reads on SQLite.** Skipping JSON parse on every
  `jsonb_extract` is a measurable win for filtered queries and views
  (cookbook recipe 07 — SQLite alternatives).
- **Smaller storage.** Binary JSONB typically saves 10–30% over the
  text form (no whitespace, compact tag bytes).
- **Symmetric with Postgres.** Both adapters now use a binary JSON
  format; mental model converges.

**Negative:**

- **Migration cost.** `INSERT INTO ... SELECT` rewrites every row.
  Acceptable on Profile B (Turso, embedded) deployments where
  `state_cache` is per-tenant and rarely huge. Coordinate with the
  user.
- **Version dependency.** The framework's SQLite adapter now needs a
  minimum runtime version. Deployments on older SQLite must skip
  this migration (stay on TEXT) — documented in the migration's
  header comment.

## Alternatives considered

### Keep TEXT JSON1 forever

Rejected. Performance pain is real for filtered queries against
`state_cache`; the optimization is essentially free once JSONB is
available.

### Migrate other JSON columns (events.payload_json, outbox state, etc.)

Out of scope for this ADR. Events' payload is proto-binary (`BYTEA`),
not JSON. The outbox table has no JSON column. Future ADRs can extend
to specific columns if needed.

### Conditional schema (TEXT or BLOB based on detected version)

Rejected. Adapter complexity, two code paths, no win. Either the
deployment supports JSONB (apply migration) or it doesn't (stay on
the previous migration).

### Use libSQL-specific JSONB extensions

There aren't any beyond what upstream SQLite provides. libSQL's
JSONB story is "we merged upstream's".

## Open questions

- **Confirm libSQL minimum version.** Check Turso release notes for
  the first stable release that ships `jsonb_extract` + `jsonb` cast
  function. Document the pin in the migration header before merging
  this ADR's implementation.
- **Same change for snapshot.state?** The snapshot column is already
  `BLOB` (proto-binary), so no change needed there.
- **Same change for events.payload_json?** That column stores
  optional JSON sidecar for events (ADR 0006). It is already `TEXT`
  on SQLite. Migration is straightforward but the value-add is
  smaller — payload_json is rarely queried server-side. Defer until
  a use case appears.

## Status

Proposed. Implementation deferred until the next time someone needs
filtered queries over `state_cache` on SQLite at a scale where the
parse overhead matters. The ADR exists so the migration path is
already designed when that day arrives.
