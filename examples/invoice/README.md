# Example: Invoice

A worked example demonstrating two framework features against a
realistic business aggregate:

| Feature | How this example uses it |
| ------- | ------------------------ |
| **Tier 1 `state_cache`** (ADR 0020, cookbook 07) | `aggregate.Runtime.StateCodec` enabled — every Append writes a cache row in the same transaction. `GetState` and `ListStates` return the current invoice with read-your-writes consistency. |
| **`IsTerminal`** (ADR 0003) | Paid and Voided invoices close the stream; the aggregate runtime rejects further commands with `es.ErrTerminal`. |

## Lifecycle

```
            ┌── MarkPaid ──→  Paid (terminal)
Created ───┤
            └── Void     ──→  Voided (terminal)
```

## Run the tests

```bash
cd examples/invoice
go test ./...
```

Three tests cover the full lifecycle: happy path with state_cache
reads + terminal transition, void path, and business-rule validation.

## What the state_cache enables

Once an invoice is created, queries like "list of all unpaid
invoices for tenant X" don't require replaying events. With the
Tier 2 materialized-view pattern from cookbook 07, you can join
invoices to customers and filter by status with normal Postgres
indexes — no projection runtime, just SQL.

```sql
CREATE MATERIALIZED VIEW invoice_open AS
SELECT
    tenant_id,
    stream_id        AS invoice_id,
    (state->>'customerId')   AS customer_id,
    (state->>'totalCents')::bigint AS total_cents,
    (state->>'currency') AS currency
FROM state_cache
WHERE type_url  = 'myapp.invoice.v1.Invoice'
  AND terminal  = false;

CREATE UNIQUE INDEX ON invoice_open (tenant_id, invoice_id);
CREATE INDEX        ON invoice_open (tenant_id, customer_id);
```

The protojson field names are camelCase by default (`customerId`,
not `customer_id`) — adjust JSON-extract expressions accordingly.

## Files

- `proto/myapp/invoice/v1/invoice.proto` — aggregate, command, event
  shapes (in the framework's proto module).
- `decider.go` — `es.Decider` with `Initial / Decide / Evolve /
  IsTerminal`. Pure business logic, no I/O.
- `invoice_test.go` — end-to-end against in-memory SQLite.

## See also

- ADR 0003 — Decider aggregate model
- ADR 0020 — Projections and Read Models (Tier 1 state_cache)
- Cookbook recipe 07 — Read models via materialized views
- Cookbook recipe 09 — Snapshots (enable for high-volume aggregates)
