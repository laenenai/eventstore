# Architecture Decision Records

This directory records the load-bearing architectural decisions for the
eventstore framework. Each ADR captures one decision: the context in which
it was made, the alternatives considered, and the consequences accepted.

ADRs are immutable. If a decision changes, write a new ADR that
**supersedes** the old one — do not edit history.

## Index

| #    | Title                                                                                   | Status     |
| ---- | --------------------------------------------------------------------------------------- | ---------- |
| 0001 | [Profile A — Always-On Database Deployment](./0001-profile-a-always-on-deployment.md)   | Deferred   |
| 0002 | [Library Delivery Model](./0002-library-delivery-model.md)                              | Accepted   |
| 0003 | [Decider Aggregate Model](./0003-decider-aggregate-model.md)                            | Accepted   |
| 0004 | [Sum-Type Representation](./0004-sum-type-representation.md)                            | Accepted   |
| 0005 | [Event Envelope Schema](./0005-event-envelope-schema.md)                                | Accepted   |
| 0006 | [Payload Format](./0006-payload-format.md)                                              | Accepted   |
| 0007 | [First-Class Multi-Tenancy](./0007-first-class-multi-tenancy.md)                        | Accepted   |
| 0008 | [Stream Identity](./0008-stream-identity.md)                                            | Accepted   |
| 0009 | [Postgres Global Position](./0009-postgres-global-position.md)                          | Accepted   |
| 0010 | [Crypto-Shredding](./0010-crypto-shredding.md)                                          | Accepted   |
| 0011 | [Snapshots](./0011-snapshots.md)                                                        | Superseded by ADR 0023 |
| 0012 | [Event Delivery and EventPublisher](./0012-event-delivery.md)                           | Accepted   |
| 0013 | [Schema Evolution and Upcasters](./0013-schema-evolution-upcasters.md)                  | Accepted   |
| 0014 | [Outbox Table Shape](./0014-outbox-shape.md)                                            | Accepted   |
| 0015 | [Decider Output Scope and Saga Boundary](./0015-decider-output-and-saga-scope.md)       | Accepted   |
| 0016 | [Codegen Plugin Packaging — buf only](./0016-codegen-plugin-packaging.md)                | Accepted   |
| 0017 | [Module and Package Layout](./0017-module-and-package-layout.md)                        | Accepted   |
| 0018 | [Schema Migrations and Query Generation](./0018-migrations-and-queries.md)              | Accepted   |
| 0019 | [SQLite Driver Strategy](./0019-sqlite-driver-strategy.md)                              | Accepted (amends 0017, 0018) |
| 0020 | [Projections and Read Models](./0020-projections-and-read-models.md)                    | Accepted   |
| 0021 | [JSONB Storage on SQLite (3.45+)](./0021-sqlite-jsonb-storage.md)                       | Accepted   |
| 0022 | [Linked Projections (Tier 3.5)](./0022-linked-projections.md)                           | Accepted (runtime; codegen deferred) |
| 0023 | [state_cache subsumes snapshots](./0023-state-cache-supersedes-snapshots.md)            | Accepted (supersedes 0011) |
| 0024 | [state_stream — coalesced state-mirror delivery](./0024-state-stream.md)                | Accepted   |
| 0025 | [Workflow-Orchestrated Command Bus](./0025-workflow-orchestrated-command-bus.md)        | Accepted (Phase 1: framework + inproc) |
| 0026 | [Workflow Adapters — Restate and DBOS](./0026-workflow-adapters.md)                     | Accepted   |
| 0027 | [Data Governance Model — Classification, Access Levels, Codegen](./0027-data-governance-model.md) | Accepted |
| 0028 | [Tamper-Evident Hash Chain](./0028-tamper-evident-chain.md)                             | Accepted   |
| 0029 | [Per-Command Subscriber Batch Delivery](./0029-per-command-subscriber-batch-delivery.md) | Accepted (amends 0025) |
| 0030 | [Schema Migration Discipline](./0030-schema-migration-discipline.md)                    | Accepted   |
| 0031 | [Execution Queues — Backend-Neutral Routing Hint](./0031-execution-queues.md)            | Accepted (amends 0025, 0026) |

## Conventions

- **Status:** Proposed, Accepted, Deferred, Superseded by ADR-XXXX, Deprecated.
- **Format:** loosely MADR-style — context, decision, consequences,
  alternatives. Keep each ADR to one decision.
- **Numbering:** sequential, zero-padded to four digits.
- **Tone:** prose, not bullets-only. A future maintainer should be able to
  reconstruct the reasoning without needing to ask anyone.
