# Example: commandbus

A worked example of the workflow-orchestrated command bus
(ADR 0025 / cookbook recipe 14 — pending). One Invoice aggregate, three
subscribers showcasing the three points of the subscriber matrix:

| Subscriber | Mode | MaxRetries | OnExhausted | What it shows |
| ---------- | ---- | ---------- | ----------- | ------------- |
| `active-invoices` (ReadModel) | `Sync` | 3 | `DLQ` | Local Tier 3 read model, read-your-writes via Sync inline subscriber |
| `search-index-mirror` (SearchIndex) | `Async` | 3 | `DLQ` | Best-effort durable mirror to a fake external "search service". DLQ on exhaustion. |
| `credit-reservation` (CreditReservation) | `Sync` | 2 | `Compensate` | Saga step. On exhaustion, emits a `Void` command rolling the invoice back. |

All three run against the same in-memory SQLite + the `commandbus/inproc`
runtime. Production would swap `inproc` for the (forthcoming) Restate or
DBOS adapter; the `Subscriber` definitions don't change.

## Run

```bash
cd examples/commandbus
go test ./...
```

Three end-to-end test scenarios:

- **`TestExample_HappyPath`** — Create flows through all subscribers
  cleanly. Read model has the row, search index has the doc, credit
  reservation has reserved the amount, DLQ stays empty.
- **`TestExample_AsyncDLQ`** — Search index fails 10 times. Async
  subscriber exhausts its 3-retry budget; the command itself still
  succeeds. The DLQ contains one row.
- **`TestExample_SagaCompensation`** — Credit reservation declines.
  Sync+Compensate path emits a `Void` command back through the bus.
  The audit trail has both `Created` and `Voided`; the read model is
  empty because Void is terminal.

## What this proves

- One generic `CommandBus[S, C]` serves every aggregate. No
  per-aggregate workflow code.
- The three-knob matrix is enough to express each common subscriber
  shape — strict consistency, best-effort, saga.
- Compensation is just another command appended through the same bus,
  producing a real event the audit trail keeps. No "rollback" of the
  eventstore.
- Async subscribers don't block the command. The inproc adapter's
  `Wait()` helper lets tests observe their eventual completion.
- The `subscriber_dlq` table is wired the same way `projection_dlq`
  is — operator dashboards / runbooks can use the same patterns.

## Files

- `invoice.go` — the Decider for the Invoice aggregate (inlined from
  `examples/invoice` for self-containment).
- `subscribers.go` — the three subscribers: `ReadModel`, `SearchIndex`,
  `CreditReservation`.
- `example_test.go` — three end-to-end scenarios.

## See also

- ADR 0025 — Workflow-orchestrated command bus
- Architecture overview, section 6 — the topology diagram + matrix
- `examples/invoice` — the canonical Invoice aggregate
- `examples/statestream` — sibling example for state-mirror delivery
  (the recovery path for Async-DLQ subscribers)
