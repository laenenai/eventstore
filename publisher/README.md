# publisher

`Publisher` interface — the contract the outbox drain calls when
delivering an event to subscribers. Adapter implementations live
under `adapters/publisher/{restate,nats,sns,pubsub,cfqueues}`; the
inproc adapter (under `adapters/publisher/inproc`) is for tests and
examples.

## Load-bearing primitives

- [`Publisher`](publisher.go) — single-method contract:
  `Publish(ctx, env) error`. Fire-and-forget from the writer's
  perspective; a returned error tells the drain to leave the row
  unmarked so the next cycle retries.

## Contract

At-least-once delivery to subscribers. Subscribers MUST be
idempotent. The framework provides no ordering guarantee across
streams; per-stream order is preserved by the outbox drain
(see [`outbox/`](../outbox/)) before the event reaches the
publisher. A non-nil error is the only signal that delivery
should be retried; the publisher MUST NOT swallow errors.

## Where to start reading

1. [`publisher.go`](publisher.go) — the whole interface.
2. `adapters/publisher/inproc/` — the simplest implementation;
   useful as the reference shape for new adapters.

## Relevant ADRs

- [0012 — Event Delivery and EventPublisher](../docs/adr/0012-event-delivery.md)
  — the delivery model, durability split between outbox and publisher,
  and the at-least-once contract.
- [Cookbook 06 — Running the Drain](../docs/cookbook/06-running-the-drain.md)
- [Cookbook 10 — Restate Publisher](../docs/cookbook/10-restate-publisher.md)
