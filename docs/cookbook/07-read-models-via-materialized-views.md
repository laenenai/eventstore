# 07: Read Models via Materialized Views on `state_cache`

A large fraction of "list X with these fields filtered by Y joined to
Z" use cases never need a hand-rolled projection. The framework's
**Tier 1 `state_cache`** (ADR 0020) already holds the current state of
every stream as JSONB; **Postgres materialized views** layer typed,
indexed, filtered/aggregated/joined read shapes on top with plain SQL.

The pattern is: state cache for source of truth, MV for the read shape
your UI or API actually wants.

## When this recipe applies

| You want | Reach for |
| -------- | --------- |
| Current state of a single entity                                  | `store.GetState` (Tier 1) |
| Paginated list of all entities of one type, no filters            | `store.ListStates` (Tier 1) |
| Filtered list (`WHERE status='active'`, `WHERE total > 10k`, etc.) | **This recipe — Tier 2 MV** |
| Joins across aggregates ("invoice with customer name")             | **This recipe — Tier 2 MV** |
| Aggregations ("count open invoices per customer")                  | **This recipe — Tier 2 MV** |
| Sub-second freshness on aggregations / windows                     | Streaming SQL (RisingWave/Materialize) — Tier 2.5 |
| Anything event-driven, not state-driven (audit log, search index)  | Custom projection (Tier 3) — see recipe 08 |

## Setup

Prerequisite: at least one aggregate is opted into `state_cache`:

```go
rt := &aggregate.Runtime[*invoicev1.State, invoicev1.Command, invoicev1.Event]{
    Store:      store,
    Decider:    invoice.Decider,
    Codec:      invoicev1.EventCodec{},
    StateCodec: aggregate.ProtoStateCodec[*invoicev1.State]{},  // <-- enables cache
}
```

Now every `Handle` writes a `state_cache` row in the same transaction
as the events. Reads after Append are guaranteed consistent.

## The pattern (Postgres)

Create the materialized view in an application migration. Cherry-pick
the fields you'll query and filter on, project them as typed columns,
add indexes for the access patterns you care about:

```sql
-- migrations/00003_invoice_active_mv.sql
-- +goose Up

CREATE MATERIALIZED VIEW invoice_active AS
SELECT
    i.tenant_id,
    i.stream_id                                AS invoice_id,
    (i.state->>'customer_id')                  AS customer_id,
    (i.state->>'total')::numeric               AS total,
    (i.state->>'status')                       AS status,
    (i.state->>'currency')                     AS currency,
    i.version,
    i.updated_at
FROM state_cache i
WHERE i.type_url  = 'myapp.invoice.v1.State'
  AND i.terminal  = false
  AND (i.state->>'status') IN ('open', 'pending', 'partially_paid');

-- REFRESH CONCURRENTLY requires a unique index. The (tenant_id,
-- invoice_id) pair is the natural PK.
CREATE UNIQUE INDEX ON invoice_active (tenant_id, invoice_id);

-- Query-path index: "list this customer's active invoices".
CREATE INDEX ON invoice_active (tenant_id, customer_id);

-- "Total invoices above a threshold."
CREATE INDEX ON invoice_active (tenant_id, total);

-- +goose Down
DROP MATERIALIZED VIEW IF EXISTS invoice_active;
```

Query it like a normal table:

```sql
SELECT invoice_id, total, currency
FROM invoice_active
WHERE tenant_id   = $1
  AND customer_id = $2
ORDER BY total DESC
LIMIT 25;
```

## Refresh cadence

Materialized views don't update themselves. Refresh on a schedule —
typically the same scheduled trigger that runs your outbox drain
(cookbook recipe 06):

```go
// Cloudflare Worker / Lambda / Cloud Run entrypoint
func ScheduledTick(ctx context.Context) error {
    if _, _, err := outboxDrain.Run(ctx); err != nil {
        return err
    }
    _, err := pool.Exec(ctx, "REFRESH MATERIALIZED VIEW CONCURRENTLY invoice_active")
    return err
}
```

`REFRESH CONCURRENTLY` requires the unique index above. It rebuilds
the view in a side copy and swaps atomically — readers see the old
view until the swap, then the new one. No blocking.

**Refresh frequency vs freshness vs cost.** Refreshing every 30 seconds
gives you "data is at most 30s stale" with very little load. Don't
refresh in a tight loop — `REFRESH CONCURRENTLY` walks the underlying
table and can be expensive on large data. Align refresh frequency
with how stale the read shape can actually be in the UI.

## Joining across aggregates

The state cache is one table; multiple aggregates' states share it,
discriminated by `type_url`. Joins are straightforward:

```sql
CREATE MATERIALIZED VIEW invoice_with_customer AS
SELECT
    i.tenant_id,
    i.stream_id                          AS invoice_id,
    (i.state->>'total')::numeric         AS total,
    c.state->>'name'                     AS customer_name,
    c.state->>'email'                    AS customer_email
FROM state_cache i
LEFT JOIN state_cache c
       ON c.tenant_id = i.tenant_id
      AND c.stream_id = (i.state->>'customer_stream_id')
      AND c.type_url  = 'myapp.customer.v1.State'
WHERE i.type_url = 'myapp.invoice.v1.State';

CREATE UNIQUE INDEX ON invoice_with_customer (tenant_id, invoice_id);
```

The join key (`customer_stream_id`) lives inside the invoice's JSON
state — the invoice aggregate stored it there at decide time. Be
explicit about this contract: changing the field name in the invoice
proto silently breaks the join. Worth a CI smoke test that runs
`EXPLAIN` against the MV definition after any proto change.

## Aggregations

```sql
CREATE MATERIALIZED VIEW open_invoice_totals_per_customer AS
SELECT
    i.tenant_id,
    (i.state->>'customer_id')              AS customer_id,
    COUNT(*)                               AS open_count,
    SUM((i.state->>'total')::numeric)      AS open_total,
    MAX(i.updated_at)                      AS latest_change
FROM state_cache i
WHERE i.type_url = 'myapp.invoice.v1.State'
  AND (i.state->>'status') = 'open'
GROUP BY i.tenant_id, i.state->>'customer_id';

CREATE UNIQUE INDEX ON open_invoice_totals_per_customer (tenant_id, customer_id);
```

Same pattern: project, group, index, schedule refresh.

## SQLite

SQLite has no native materialized view. Options, in roughly increasing
sophistication:

### Option A — regular `VIEW` (re-runs the query each time)

```sql
CREATE VIEW invoice_active AS
SELECT
    i.tenant_id,
    i.stream_id                                AS invoice_id,
    json_extract(i.state, '$.customer_id')     AS customer_id,
    CAST(json_extract(i.state, '$.total') AS REAL) AS total,
    json_extract(i.state, '$.status')          AS status
FROM state_cache i
WHERE i.type_url = 'myapp.invoice.v1.State'
  AND i.terminal = 0;
```

No refresh needed (the view is computed on every query), but every
query pays the JSON parsing cost. Fine for small datasets and admin
UIs; not for hot paths over millions of rows.

### Option B — trigger-maintained denormalized table

```sql
CREATE TABLE invoice_active (
    tenant_id    TEXT,
    invoice_id   TEXT,
    customer_id  TEXT,
    total        REAL,
    status       TEXT,
    PRIMARY KEY (tenant_id, invoice_id)
);

CREATE TRIGGER state_cache_invoice_upsert
AFTER INSERT OR UPDATE OF state ON state_cache
WHEN NEW.type_url = 'myapp.invoice.v1.State'
BEGIN
    INSERT OR REPLACE INTO invoice_active VALUES (
        NEW.tenant_id,
        NEW.stream_id,
        json_extract(NEW.state, '$.customer_id'),
        CAST(json_extract(NEW.state, '$.total') AS REAL),
        json_extract(NEW.state, '$.status')
    );
END;
```

Updates are synchronous with `state_cache` writes — no refresh cycle,
no lag. Indexes work normally. Cost is migration complexity and the
trigger sitting in the schema.

### Option C — Tier 3 custom projection

Write a small projection that consumes `Invoice` events, maintains
`invoice_active` itself, and uses normal indexes. See recipe 08 for
the projection rebuild story. Use this when the read shape needs
logic the JSON extraction can't express (e.g., looking up
external data, computing fields from event metadata).

## Pitfalls

**Schema drift.** Adding a field to a State proto doesn't update MVs
referencing the JSON. They quietly return NULL for old rows until the
next REFRESH, then NULL for the new field on rows that haven't been
re-cached. Mitigation: include an `EXPLAIN` against every MV in CI
after any proto change.

**Removing a field.** `state->>'old_field'` becomes NULL silently.
Add a CI check that grep's MV definitions for fields that don't exist
in the state proto's reflected descriptor.

**Wide aggregations on large tables.** `REFRESH CONCURRENTLY` walks
the source table and rebuilds the entire MV. On 10M+ rows, refresh
itself becomes a workload. Solutions: (a) shrink the MV's source
predicate to recent data only; (b) move to Tier 2.5 (RisingWave /
Materialize) for incremental refresh.

**Don't put PII in JSON paths you index.** Functional indexes on
`state->>'email'` make those values queryable cheaply — which is the
point — but it also means the index file holds the value. If you
crypto-shred a row (ADR 0010), the index needs to drop too. The
state_cache row is shredded by the framework's shred machinery; MV
indexes built on top are not — refresh after a shred to clear them.

## Reference

- ADR 0020 — Projections and Read Models (the three-tier model)
- Cookbook recipe 06 — Running the Outbox Drain (your scheduler also
  runs `REFRESH`)
- Cookbook recipe 08 — Rebuilding projections (for Tier 3)
- [`adapters/storage/postgres/migrations/00004_state_cache.sql`](../../adapters/storage/postgres/migrations/00004_state_cache.sql) — `state_cache` schema
