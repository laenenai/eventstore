# Compliance Architecture — Event Store Framework

| Field | Value |
| --- | --- |
| **Document title** | Compliance Architecture and Controls — Event Store Framework |
| **Version** | 1.0 (draft) |
| **Date** | 2026-06-10 |
| **Status** | Preparatory — issued ahead of first regulated production deployment |
| **Classification** | Confidential — Regulator / External Auditor handover |
| **Intended audience** | SOC 2 Type II auditors; ISO/IEC 27001:2022 lead auditors; EU competent authorities under DORA Article 28; internal compliance / risk functions |
| **Document owner** | Laenen Partners — Platform Engineering |
| **Subject system** | `github.com/laenenai/eventstore` — Go event-sourcing framework (library delivery model, ADR 0002) |

---

## Table of contents

- [Compliance architecture at a glance](#compliance-architecture-at-a-glance)
1. [Executive summary](#1-executive-summary)
2. [Document scope, audience, and framing](#2-document-scope-audience-and-framing)
3. [System architecture overview](#3-system-architecture-overview)
4. [Data persistence and lifecycle](#4-data-persistence-and-lifecycle)
5. [Encryption and key management](#5-encryption-and-key-management)
6. [Personal data governance and data subject rights](#6-personal-data-governance-and-data-subject-rights)
7. [Tenant isolation](#7-tenant-isolation)
8. [Authentication and authorisation](#8-authentication-and-authorisation)
9. [Audit trail and tamper evidence](#9-audit-trail-and-tamper-evidence)
10. [Schema evolution and data integrity](#10-schema-evolution-and-data-integrity)
11. [Operational resilience](#11-operational-resilience)
12. [Change management and software development life cycle](#12-change-management-and-software-development-life-cycle)
13. [Observability](#13-observability)
14. [Software supply chain](#14-software-supply-chain)
15. [Compliance control mapping](#15-compliance-control-mapping)
    - [15.1 SOC 2 Type II — Trust Services Criteria](#151-soc-2-type-ii--trust-services-criteria)
    - [15.2 ISO/IEC 27001:2022 — Annex A](#152-isoiec-270012022--annex-a)
    - [15.3 EU DORA and EBA ICT risk-management guidelines](#153-eu-dora-and-eba-ict-risk-management-guidelines)
16. [Addendum A — Identified gaps](#16-addendum-a--identified-gaps)
17. [Addendum B — Remediation roadmap](#17-addendum-b--remediation-roadmap)
18. [Annex — Glossary, references, and verification notes](#18-annex--glossary-references-and-verification-notes)

---

## Compliance architecture at a glance

The diagram below summarises the components, trust boundaries, and compliance-relevant invariants that the rest of the document elaborates. Detailed treatment of each element follows in Sections 3 through 14.

```
                             ┌──────────────────────┐
                             │ CALLER (external)    │
                             │ end-user / service   │
                             └──────────┬───────────┘
                                        │
═══════════════════ Trust boundary 1 (adopter-owned) ════════════════════
       TLS / mTLS / bearer-token / OIDC — framework provides no wiring
                                        │
            ┌───────────────────────────▼────────────────────────────┐
            │            ADOPTER APPLICATION (Go binary)             │
            │                                                        │
            │   HTTP edge  →  authn  →  authz (Cedar, pluggable)     │
            │                                                        │ 
            │   ctx = es.WithTenant(authz.WithPrincipal(ctx, P), T)  │
            │   ─────────────────────────────────────────────────    │
            │   MANDATORY: framework refuses calls without tenant    │
            └───────────────────────┬────────────────────────────────┘
                                    │ command + ctx{principal, tenant}
            ┌───────────────────────▼────────────────────────────────┐
            │      FRAMEWORK  (github.com/laenenai/eventstore)       │
            │                                                        │
            │   aggregate.Runtime → Decider(Initial/Decide/Evolve)   │
            │                                  │                     │
            │   Codegen: EncryptPII per field classified             │
            │   (es.v1.data_classification) — AES-256-GCM            │
            │   per (tenant, subject) DEK; wrapped by per-tenant KEK │
            │                                  │                     │
            │   SAD classification  ►  REJECTED at runtime (PCI DSS) │
            └───────────────────────┬────────────────────────────────┘
                                    │ append (encrypted payload + envelope)
                       ╔════════════▼════════════╗
                       ║   SINGLE TRANSACTION    ║  atomic on PG / SQLite
                       ║   pg_advisory_xact_lock ║  store-wide write serialise
                       ║   SET LOCAL             ║  binds RLS context
                       ║     app.tenant_id = T   ║  (ADR 0032, migr. 00015)
                       ╚════════════╤════════════╝
                                    │
     ┌────────┬────────┬────────────┼────────────┬─────────┬─────────┐
     ▼        ▼        ▼            ▼            ▼         ▼         ▼
  events  state_   outbox     subject_     unique_   processed   proj_
          cache    (ref)      keys         claims    _events     dlq+chkpt
    │                            │
    │ append-only +              │ wrapped DEK + shredded_at tombstone
    │ SHA-256 hash chain         │ Shred(subject) → destroy DEK
    │ ComputeChainHash /         │ ⇒ all that subject's ciphertext
    │ VerifyStreamChain          │   becomes computationally inaccessible
    │ (ADR 0028)                 │   (GDPR Art. 17 — right to erasure)
    │                            │
    ▼                            ▼
   ────────────────────────────────────────────────────────────────────────
   ALL tenant-scoped tables: ENABLE + FORCE ROW LEVEL SECURITY
   Policy:  USING tenant_id = current_setting('app.tenant_id', false)
   Pools:
     eventstore_app    (no BYPASSRLS)  — every tenant-scoped operation
     eventstore_admin  (BYPASSRLS)     — cross-tenant ops via WithAdminPool
                                    │
   ═══════════════════ Trust boundary 2 (KMS custody) ══════════════════════
                                    │
                                    ▼   KEK wrap / unwrap   (cold path)
            ┌──────────────────────────────────────────────────────┐
            │           KMS  (external system, pluggable)          │
            │   per-tenant KEK — KEYS NEVER STORED IN FRAMEWORK DB │
            │   Reference adapters: AWS KMS · in-process (dev)     │
            │   Adopter integrates: GCP KMS / Vault / HSM / Azure  │
            └──────────────────────────────────────────────────────┘

  ── Async read & delivery (no blocking writers, RLS-filtered on app pool) ──

   Reads:    Tier 1   state_cache (in-transaction, read-your-writes)
             Tier 2   PG MATERIALIZED VIEW over state_cache (REFRESH)
             Tier 3   projection.Runtime — global cursor, checkpoint, DLQ

   Drains:   Outbox        → Publisher (at-least-once, per-subscriber DLQ)
             State-stream  → coalesced "latest state per stream" (ADR 0024)

   Audit log = the events themselves — every event carries:
     Actor(principal, kind, on-behalf-of, api_key_id), correlation_id,
     causation_id, command_id, occurred_at, recorded_at, hash, prev_hash.
     No separate audit table by design (ADR 0005).
```

**Key invariants surfaced by the diagram:**

1. **Two trust boundaries.** Boundary 1 is adopter-owned (transport security, authentication); boundary 2 separates the framework's database from the KMS holding key material. A compromise of either zone alone does not yield personal-data plaintext.
2. **Tenant context is mandatory.** Every framework call requires `es.WithTenant(ctx, T)`; the runtime refuses to operate without it (`es/tenant.go:30`–`36`).
3. **Single atomic write.** Events, current state, outbox handoff, uniqueness claims, and subject-key bookkeeping all commit together. There is no eventual-consistency window inside the framework's own data.
4. **Defense-in-depth tenant isolation.** Application layer (`RequireTenant`) is the primary boundary; PostgreSQL RLS policies are the second line, applied even to the table owner via `FORCE ROW LEVEL SECURITY` (ADR 0032).
5. **Field-level encryption opt-in by classification.** Default is plaintext; declaring `data_classification = PERSONAL` or stricter engages per-subject AES-256-GCM. `SAD` (PCI sensitive authentication data) is structurally rejected.
6. **Right to erasure via crypto-shredding.** Destroying the DEK in `subject_keys` renders all encrypted personal data for that subject computationally inaccessible while preserving the immutable record of *what happened*.
7. **Tamper-evident chain.** Every event carries a SHA-256 hash linking to its predecessor on the stream; verification is an opt-in idempotent operation auditors may run on any cadence.
8. **Events are the audit log.** No separate audit table; the system of record and the audit trail are the same row, eliminating the failure mode where they diverge.

---

## 1. Executive summary

The Event Store Framework is a Go library that provides event-sourcing primitives — an append-only event log, sealed sum-type commands and events generated from Protocol Buffers, an aggregate runtime, projections, an at-least-once outbox publisher, and pluggable storage adapters for PostgreSQL and SQLite. It is delivered as a library (Architecture Decision Record — hereinafter "ADR" — 0002), embedded by adopter applications into their own deployment topology. The framework does not run as a long-lived service; the adopter owns the operational perimeter.

The framework was designed with regulated multi-tenant SaaS deployments as the primary use case. The following controls are built into the library and codegen, available to every adopter without additional integration:

- **First-class multi-tenancy** (ADR 0007). Every operation requires an explicit tenant identifier in context; the runtime refuses to operate without one. Postgres tables are partitioned by tenant hash; every index leads with `tenant_id`.
- **Defense-in-depth tenant isolation on PostgreSQL** via Row-Level Security policies and a split role topology (ADR 0032, implemented in migration 00015). Application-layer enforcement is the primary boundary; RLS is the second line.
- **Field-level crypto-shredding for personal data** (ADR 0010, ADR 0027). Fields are tagged with a `data_classification` Protocol Buffers option that drives codegen to emit per-subject AES-256-GCM encryption. Forgetting a data subject destroys the per-subject Data Encryption Key, after which all ciphertext written for that subject is computationally inaccessible.
- **Tamper-evident hash chain** (ADR 0028). Each event carries a SHA-256 hash linking it to its predecessor on the stream. Verification is an opt-in, idempotent operation auditors may run on any cadence.
- **Pluggable Key Management Service** (ADR 0010). Reference adapters ship for AWS KMS and an in-process implementation for development. Adopters integrate their KMS of choice through a single `KeyStore` interface.
- **Schema migration discipline** (ADR 0030). Every schema-touching pull request declares its migration tier; the codebase carries an enforced taxonomy of compatible-vs-breaking change types.
- **OpenTelemetry tracing and metrics** on the hot paths (command dispatch, event append, stream read). Adopters wire a TracerProvider and MeterProvider of their choice.

The framework is presented to auditors as a **set of compliance enablers** rather than a certified product. As a library, it cannot itself be SOC 2 or ISO 27001 certified; the certification scope belongs to the adopter's deployment. This document maps the framework's primitives to the control objectives auditors most commonly assess against, and identifies the gaps the adopter or the framework maintainer should close before a regulated production launch.

**Gaps summary** (full detail in [Addendum A](#16-addendum-a--identified-gaps)): no DSAR export tooling; no automated key rotation; no CI vulnerability scanning; no Software Bill of Materials generation; no chain-verification CLI; no formal threat model; no SQLite database-layer tenant isolation; no transport-level security wiring (TLS, mutual TLS, bearer-token validation), which is delegated to the adopter by design. A prioritised twelve-month remediation roadmap is provided in [Addendum B](#17-addendum-b--remediation-roadmap).

---

## 2. Document scope, audience, and framing

### 2.1 Scope of this document

This document covers the Event Store Framework as published at `github.com/laenenai/eventstore`, including:

- The core library packages (`es/`, `aggregate/`, `projection/`, `outbox/`, `publisher/`, `shred/`, `authz/`, `kms/`, `cmdworkflow/`).
- Storage adapters (`adapters/storage/postgres`, `adapters/storage/sqlite`).
- KMS adapters (`adapters/kms/aws`, `adapters/kms/inproc`).
- Authorisation adapter (`adapters/authz/cedar`).
- HTTP edge helper (`adapters/httpedge/connect`).
- Workflow adapters (`adapters/cmdworkflow/{inproc,restate,dbos}`).
- Codegen plugin (`cmd/protoc-gen-es-go`).
- Operator command-line tool (`cmd/esctl`).

This document does **not** cover:

- The adopter's deployment topology, network architecture, operating system hardening, container runtime, or cloud-provider configuration.
- Adopter-owned components built on top of the framework (custom transport layers, projections, sagas, integration handlers).
- Identity-provider integration (the framework consumes a principal injected into context; it does not authenticate users).
- Physical security of the underlying infrastructure.

### 2.2 Audience and intended use

The document is structured to serve three audiences in one pass:

1. **External SOC 2 Type II auditors** assessing the framework as a software component within a service organisation's system description.
2. **ISO/IEC 27001:2022 lead auditors** mapping framework primitives to Annex A controls within an Information Security Management System.
3. **Competent authorities and supervisors under EU Regulation 2022/2554 (DORA)** assessing ICT risk-management capabilities, in particular Articles 5–16 (ICT risk-management framework), Articles 17–23 (ICT-related incident management), Articles 24–27 (digital operational resilience testing), and Articles 28–30 (third-party ICT risk).

Section 15 provides explicit control-by-control mapping for each audience. Sections 3 through 14 carry the substantive evidence.

### 2.3 Framing — library, not platform

The Event Store Framework is delivered as a Go library that the adopter compiles into their own binary. This is a deliberate design decision (ADR 0002) with material implications for the compliance posture:

- The framework provides **primitives**: data structures, codecs, storage adapters, runtime helpers. The adopter assembles these into a running system in their own `main.go`.
- The framework **enforces invariants** at the API boundary (for example, refusing operations without a tenant context, refusing to accept Sensitive Authentication Data) but cannot enforce operational practices (key rotation cadence, backup schedule, retention enforcement). Those remain the adopter's responsibility.
- The framework **does not include**: a long-running daemon, a control plane, an out-of-the-box web server with security middleware, a built-in identity provider, or a secrets management system. Each is the adopter's integration responsibility.
- Compliance certifications (SOC 2, ISO 27001, etc.) are properties of a service, not a library. The framework can be assessed for its design and code quality; the adopter's deployment must be assessed for operational controls.

This document therefore uses the phrase "the framework provides" for capabilities baked into the library, and "the adopter must ensure" for operational responsibilities that fall outside the library's scope. Both lists matter to a regulator's assessment.

### 2.4 Framing — preparatory document

At the time of issue, the framework is **not yet in production with a regulated customer**. This document is therefore framed as a control-readiness self-assessment rather than an operational evidence pack. Where a control is fully implemented in code, the evidence is the code itself (cited with file paths and line numbers). Where a control depends on adopter practice or is not yet implemented, the gap is recorded in Addendum A and a remediation path is recorded in Addendum B.

---

## 3. System architecture overview

### 3.1 Conceptual model

The framework implements **event sourcing** with the **decider pattern** (ADR 0003). The conceptual model has three layers:

1. **Commands** — caller intent, validated and authorised at the application boundary. Commands are immutable Protocol Buffers messages produced from sealed-sum-type proto containers (ADR 0004).
2. **Events** — the canonical record of what happened. Events are immutable, append-only, ordered by `(tenant_id, stream_id, version)`. Each event carries an envelope (ADR 0005) with provenance metadata: `event_id`, `tenant_id`, `stream_id`, `version`, `global_position`, `type_url`, `schema_version`, `occurred_at`, `recorded_at`, `correlation_id`, `causation_id`, `command_id`, `actor` (principal, kind, on-behalf-of, API-key id), payload, optional payload-JSON sidecar, encryption key references, hash, and predecessor hash.
3. **State** — the materialised view of an aggregate, derived from its event history. State is held in two complementary stores: the `state_cache` table (Tier 1 — synchronous, written in the same transaction as the events that produced it, ADR 0023) and projection-owned read models built via the `projection.Runtime` (Tier 3).

### 3.2 The write transaction

Every state change goes through a **single database transaction** that atomically commits the event(s), the updated aggregate state, and an outbox row referencing the event for downstream delivery. The transaction sequence on PostgreSQL is (`adapters/storage/postgres/append.go:43`–`213`):

1. `BEGIN`.
2. `SET LOCAL app.tenant_id` — binds the row-level security context (ADR 0032).
3. `pg_advisory_xact_lock` on a store-wide constant key — serialises all writers store-wide and guarantees gap-free monotonic allocation of `global_position` from the sequence (ADR 0009).
4. Constraint operations (uniqueness claims and releases) per ADR 0010.
5. Read predecessor hash for the stream; compute SHA-256 chain hash per event in the batch.
6. `INSERT` each event into `events`, allocating `global_position` from `events_global_position_seq`.
7. `INSERT` corresponding outbox rows into `outbox` (reference-only — JOINs to `events` at publish time, per ADR 0014).
8. `UPSERT` the new aggregate state into `state_cache`.
9. `COMMIT`.

The single-transaction guarantee means events, current state, and outbox handoff cannot disagree.

### 3.3 The read and delivery paths

Reads and deliveries run asynchronously on their own schedules. Three read tiers and two drains coexist:

- **Tier 1 — `state_cache`**. Synchronous in-database query against the materialised state row. Read-your-writes is guaranteed.
- **Tier 2 — PostgreSQL materialised views** over `state_cache`, refreshed `CONCURRENTLY` on the adopter's schedule (ADR 0021).
- **Tier 3 — projection runtime**. Codegen-emitted dispatchers per aggregate, advancing a global-position cursor with checkpoint persistence, fail-stop on handler error, shard locking via PostgreSQL advisory locks.
- **Outbox drain**. Reads pending outbox rows, JOINs to `events`, hands the envelope to the configured `Publisher` implementation, marks the row published (or failed with backoff) on completion.
- **State-stream drain** (ADR 0024). Coalesced delivery of state mirrors for subscribers that want "latest state per stream" semantics.

### 3.4 Module topology

The framework is a multi-module monorepo. Each adapter with heavy dependencies carries its own `go.mod` so adopters do not pull driver code they do not use:

```
.                                  root module
adapters/storage/postgres/         pgx, sqlc, goose
adapters/storage/sqlite/           modernc.org/sqlite, sqlc
adapters/cmdworkflow/restate/      restate-sdk
adapters/cmdworkflow/dbos/         dbos-sdk
adapters/cmdworkflow/inproc/       (root module)
adapters/authz/cedar/              cedar-go
adapters/httpedge/connect/         connectrpc.com/connect
adapters/kms/{aws,inproc}/         (root or aws-sdk-go-v2/kms)
adapters/publisher/{inproc,restate}/
proto/                             framework + example protos
gen/                               generated Go (never hand-edited)
cmd/protoc-gen-es-go/              codegen plugin
cmd/esctl/                         operator CLI
```

A `go.work` file ties the modules together for local development. The release script (`scripts/release.sh`) synchronises versions across every published module so an adopter consuming, say, the framework and the Postgres adapter at incompatible versions is structurally avoided.

### 3.5 Trust boundaries

The framework recognises the following trust boundaries:

1. **Caller ↔ adopter's transport layer.** Authentication of the caller, transport encryption (TLS / mutual TLS), CORS, rate limiting, and security headers are the adopter's responsibility. The framework consumes an authenticated `Principal` injected into context by the adopter.
2. **Adopter's application code ↔ framework runtime.** The framework refuses operations without a tenant context (`es.RequireTenant`). The runtime enforces the decider contract (Initial / Decide / Evolve) at the type system level.
3. **Framework ↔ storage adapter.** The Postgres adapter binds `app.tenant_id` per transaction; the RLS policies enforce isolation at the row level (ADR 0032). The SQLite adapter relies on application-layer enforcement and the recommended one-database-per-tenant deployment.
4. **Framework ↔ KMS.** Per-subject DEKs are wrapped under a per-tenant KEK held in a pluggable KMS. KMS calls are on the cold path; a forgotten subject's DEK is destroyed in the framework's `subject_keys` table on shred.
5. **Adopter's application ↔ downstream publishers.** The outbox-publisher seam decouples write durability from publish reliability. At-least-once delivery semantics are the contract.

---

## 4. Data persistence and lifecycle

### 4.1 Source of truth — the `events` table

The `events` table is the authoritative record. It is **append-only**: there is no application path that updates or deletes event rows. The only mutation supported on event rows is the chain-backfill operation introduced by ADR 0028, which populates `hash` and `prev_hash` columns on pre-existing rows whose chain values are NULL — and that operation is guarded by `WHERE hash IS NULL`, so rows that already carry a hash are never overwritten (`adapters/storage/postgres/queries/chain.sql`; `es/chain.go:168`–`194`).

PostgreSQL schema (`adapters/storage/postgres/migrations/00001_initial_schema.sql:37`–`56`):

```sql
CREATE TABLE events (
    event_id            UUID         NOT NULL,
    tenant_id           TEXT         NOT NULL,
    stream_id           TEXT         NOT NULL,
    version             BIGINT       NOT NULL,
    global_position     BIGINT       NOT NULL,
    type_url            TEXT         NOT NULL,
    schema_version      INTEGER      NOT NULL,
    occurred_at         TIMESTAMPTZ  NOT NULL,
    recorded_at         TIMESTAMPTZ  NOT NULL DEFAULT clock_timestamp(),
    correlation_id      UUID         NOT NULL,
    causation_id        UUID         NOT NULL,
    command_id          UUID         NOT NULL,
    actor               JSONB        NOT NULL,
    actor_principal     TEXT         NOT NULL,
    payload             BYTEA        NOT NULL,
    payload_json        JSONB,
    encryption_key_refs JSONB,
    PRIMARY KEY (tenant_id, stream_id, version)
) PARTITION BY HASH (tenant_id);
```

The table is partitioned by `HASH(tenant_id)` with 16 partitions by default (ADR 0007). Adopters with a small known set of tenants may override to `LIST` partitioning at schema-generation time for cleaner per-tenant `pg_dump` and `DETACH PARTITION` workflows.

The `hash` and `prev_hash` columns added by migration `00013_tamper_evident_chain.sql` carry the per-stream SHA-256 chain (ADR 0028, Section 9 of this document).

### 4.2 Supporting tables

| Table | Purpose | Source ADR | Tenant-scoped |
| --- | --- | --- | --- |
| `events` | Canonical event log | 0005 | Yes (partitioned) |
| `unique_claims` | Transactional uniqueness constraints | 0010 | Yes (partitioned) |
| `subject_keys` | Per-(tenant, subject) wrapped DEKs and shred tombstones | 0010 | Yes (partitioned) |
| `outbox` | At-least-once publish queue (references to events) | 0014 | Yes (partitioned) |
| `state_cache` | Tier-1 aggregate state mirror | 0023 | Yes |
| `projection_checkpoint` | Per-projector global-position cursor | 0020 | Yes (default `''` for cross-tenant projectors) |
| `projection_dlq` | Failed event handler outputs for projection-side retry | 0020 | Yes |
| `processed_events` | Dedup ledger for at-least-once projection handlers | 0020 | Yes |
| `state_stream_subscribers` | State-stream coalesced delivery cursors | 0024 | Yes (default `''` for cross-tenant subscribers) |
| `subscriber_dlq` | Per-command-batch subscriber failure queue | 0029 | Yes |

Every tenant-scoped table carries `tenant_id` as the leading column of its primary key and is subject to the RLS policies enabled by migration `00015_rls_tenant_isolation.sql` on PostgreSQL.

### 4.3 Stream identity

`StreamID` is the canonical `tenant:type:id` form (ADR 0008). Stream identity is parsed and validated at every API boundary; the type system makes it impossible to construct an "anonymous" stream without a tenant. The encoding survives serialisation, logging, and database round-trip.

### 4.4 Global ordering

The `global_position` column on `events` is allocated from a single PostgreSQL sequence (`events_global_position_seq`) inside the write transaction, under the store-wide advisory lock. This guarantees:

- **Gap-free monotonic allocation** in commit order.
- **Cross-tenant ordering** — a single timeline of all events in the store, useful for billing aggregation, compliance export, and downstream feeds that need a global cursor.
- **Trade-off**: writes serialise store-wide. The framework deliberately accepts this constraint as the simplest correct design for sub-million-events-per-day workloads. Adopters with higher throughput must either shard or use a workflow back-end (Restate, DBOS) for command serialisation (ADR 0025, ADR 0026).

### 4.5 Retention

The framework does not enforce a retention policy. Events are kept indefinitely by default. Two retention-relevant primitives are provided:

- **Crypto-shredding** (ADR 0010, ADR 0027) — destroys a data subject's DEK, rendering all encrypted personal data for that subject computationally inaccessible. The event rows themselves remain (preserving the audit trail of *what happened*), but the personal-data fields cannot be decrypted.
- **Outbox cleanup** (`adapters/storage/postgres/queries/outbox.sql` → `CleanupPublishedOutbox`) — removes outbox rows that have been published and are older than a caller-supplied retention window.

Enforcement of a calendar-based retention policy (e.g. "delete tax-classified events after 10 years") is the **adopter's responsibility**. A retention worker would run on the adopter's schedule, query events by classification and age, and shred subjects whose retention has expired. This is documented as a gap in Addendum A.

### 4.6 Backup and recovery

Backup is the adopter's operational responsibility. The framework's deliberate design choices that facilitate backup:

- **PostgreSQL partitioning by tenant** enables clean per-tenant `pg_dump` or `DETACH PARTITION` workflows (ADR 0007).
- **SQLite one-file-per-tenant deployment** (recommended in production) reduces backup to file copy and gives trivial offboarding (delete the file).
- **Migrations are reversible by design** when possible (every `+goose Up` carries a `+goose Down`, see migrations in `adapters/storage/{postgres,sqlite}/migrations/`).

The framework does **not** ship backup tooling, scheduled backup workers, or restore-test automation. Documented in Addendum A.

---

## 5. Encryption and key management

### 5.1 Encryption at rest — design

The framework implements **per-field, per-subject envelope encryption** (ADR 0010). The design has four layers:

1. **Field classification** (ADR 0027). Each Protocol Buffers message field declares its data classification via the `(es.v1.data_classification)` option. Classifications drive encryption (and downstream PII handling — see Section 6).
2. **Per-subject Data Encryption Keys (DEKs)**. One DEK per `(tenant_id, subject)` pair, stored encrypted in the `subject_keys` table. Subjects are typically `customer:abc`, `employee:42`, etc.
3. **Per-tenant Key Encryption Keys (KEKs)**. Held in a pluggable KMS — never in the framework's database. KEKs wrap DEKs at write time and unwrap them at read time. KMS is on the cold path; hot reads use a process-local DEK cache.
4. **Cryptographic primitive**. AES-256-GCM with a fresh 96-bit IV per encryption operation. Wire format: `version(1B) | iv(12B) | ciphertext | tag(16B)` — algorithm constant `wireV1AESGCM = 0x01` at `shred/shred.go:303`; seal/open implementation at `shred/shred.go:306`–`352`. Version byte `0x01` denotes AES-256-GCM; new algorithm choices in the future would increment the version with a documented upgrade path.

#### Worked example — declaring classified fields

The classification is declared in the schema, not in code. The following is the actual `Employee` aggregate schema shipped in the framework's example tree (`proto/myapp/employee/v1/employee.proto`):

```protobuf
syntax = "proto3";

package myapp.employee.v1;
import "es/v1/options.proto";

message Employee {
  option (es.v1.aggregate) = "employee";

  string employee_id   = 1 [(es.v1.subject_field) = true];
  string legal_name    = 2 [(es.v1.data_classification) = DATA_CLASSIFICATION_PERSONAL];
  string email         = 3 [(es.v1.data_classification) = DATA_CLASSIFICATION_PERSONAL];
  string date_of_birth = 4 [(es.v1.data_classification) = DATA_CLASSIFICATION_QUASI_IDENTIFIER];
  string department    = 5 [(es.v1.data_classification) = DATA_CLASSIFICATION_INTERNAL];
  string current_role  = 6 [(es.v1.data_classification) = DATA_CLASSIFICATION_INTERNAL];
  Status status        = 7 [(es.v1.data_classification) = DATA_CLASSIFICATION_INTERNAL];
}

message Hired {
  string employee_id   = 1 [(es.v1.subject_field) = true];
  string legal_name    = 2 [(es.v1.data_classification) = DATA_CLASSIFICATION_PERSONAL];
  string email         = 3 [(es.v1.data_classification) = DATA_CLASSIFICATION_PERSONAL];
  string date_of_birth = 4 [(es.v1.data_classification) = DATA_CLASSIFICATION_QUASI_IDENTIFIER];
  string department    = 5 [(es.v1.data_classification) = DATA_CLASSIFICATION_INTERNAL];
  string initial_role  = 6 [(es.v1.data_classification) = DATA_CLASSIFICATION_INTERNAL];
}
```

The `subject_field` option on `employee_id` designates the subject identifier — the value the framework uses to look up or create the per-subject DEK. Classified fields with `PERSONAL` or stricter classifications drive codegen to emit per-field encryption; `INTERNAL`-classified fields remain plaintext (they are excluded from DSAR export but visible in operational tooling). A field with no classification annotation is treated as `PUBLIC` — plaintext, exportable.

For events that touch multiple subjects (for example a money-transfer event encrypted under both the sender's and recipient's keys), the `(es.v1.subject)` per-field option overrides the default subject inferred from `subject_field`. This is described in ADR 0010.

#### Worked example — the codegen output

The codegen plugin reads the schema above and emits an `EncryptPII` method per event type that has at least one encrypted field. The actual generated code for `Hired` is in `gen/myapp/employee/v1/employee_es.pb.go` and looks like this (excerpt, lines 315–338):

```go
// EncryptPII encrypts every classification-PERSONAL+ field in
// place using s. Called by aggregate.Runtime before Codec.Encode
// when Runtime.Shredder is configured.
//
// `bytes` fields hold raw ciphertext after encrypt; `string` fields
// classified PERSONAL+ hold base64-encoded ciphertext so the field
// remains UTF-8-valid. Fields classified
// (es.v1.data_classification) = DATA_CLASSIFICATION_SAD cause this
// method to return an error before any field is touched — SAD MUST
// NOT be persisted (PCI-DSS §3.2). See ADR 0027.
func (e *Hired) EncryptPII(ctx context.Context, s *shred.Shredder, tenantID, subject string) error {
    if e.LegalName != "" {
        sealed, err := s.EncryptField(ctx, tenantID, subject, []byte(e.LegalName))
        if err != nil {
            return fmt.Errorf("Hired.EncryptPII legal_name: %w", err)
        }
        e.LegalName = base64.RawStdEncoding.EncodeToString(sealed)
    }
    if e.Email != "" { /* …same pattern… */ }
    if e.DateOfBirth != "" { /* …same pattern… */ }
    return nil
}
```

The corresponding `DecryptPII` (`employee_es.pb.go:345`) reverses the operation; if the DEK for the subject has been destroyed (Section 6.3), the per-field decrypt returns a `RedactedField{Name, Subject, Reason: "shredded"}` placeholder rather than failing the whole decode.

The application code never calls `EncryptField` or `DecryptField` directly. The aggregate runtime invokes `EncryptPII`/`DecryptPII` automatically when a `Shredder` is configured on `Runtime.Shredder`. Forgetting to wire the Shredder produces a clear error at startup; it cannot silently bypass encryption.

### 5.2 Encryption at rest — implementation

The encryption mechanism lives in the `shred` package (`shred/shred.go`). The codegen plugin (`cmd/protoc-gen-es-go/main.go:840`–`949`) emits `EncryptPII` and `DecryptPII` methods on every event type that has at least one classified field. The methods iterate the classified fields and call into `shred.Shredder.EncryptField` / `DecryptField`, which:

- On encrypt: looks up or generates a DEK for the `(tenant, subject)` pair (lazy creation), unwraps the DEK via the KMS, encrypts the field value with AES-256-GCM, returns the ciphertext.
- On decrypt: looks up the DEK from `subject_keys`, returns `shred.ErrShredded` if the DEK has been destroyed, otherwise unwraps and decrypts.

For string fields, ciphertext is base-64 encoded so it round-trips through proto's UTF-8 string semantics. For bytes fields, ciphertext is stored raw.

The framework refuses to accept fields classified `SAD` (Sensitive Authentication Data — PCI DSS card-not-present authentication data, the cryptogram and full magnetic stripe contents). Attempting to encrypt such a field returns an error at write time (`shred/shred.go:160`; `cmd/protoc-gen-es-go/main.go:850`). This is a deliberate compliance constraint: SAD must never persist post-authorisation under PCI DSS Requirement 3.2, so the framework structurally prevents it.

### 5.3 Encryption in transit

The framework does **not** wire transport-level encryption. The HTTP edge helper (`adapters/httpedge/connect/dispatch.go`) is intentionally thin: it provides a request dispatcher that decodes incoming Connect-RPC requests and hands them to the command bus, with no opinion on TLS, certificate handling, or transport configuration.

The adopter is responsible for:

- Terminating TLS at a reverse proxy or load balancer (or in the application process, using Go's `net/http` TLS support).
- Enforcing mutual TLS for service-to-service traffic where the adopter's risk model requires it.
- In-cluster traffic: plain HTTP is acceptable behind a service-mesh TLS boundary (Istio, Linkerd) per the user's global standards document referenced in `~/.claude/CLAUDE.md`.

For Restate-backed deployments (ADR 0026), the Restate runtime handles its own TLS and is the boundary at which transport security is enforced.

### 5.4 Key management — KMS abstraction

The `kms` package defines a single `KeyStore` interface (`kms/keystore.go:5`–`46`) with four operations:

| Operation | Purpose |
| --- | --- |
| `GenerateDEK` | Produce a 32-byte random DEK + its wrapped form under the current tenant KEK |
| `UnwrapDEK` | Decrypt a wrapped DEK using the appropriate KEK version |
| `RewrapDEK` | Re-encrypt a DEK under a newer KEK version (rotation path) |
| `CurrentKEKVersion` | Returns the active KEK version for a tenant |

Two reference adapters are provided:

- **`adapters/kms/aws`** — AWS Key Management Service. Wraps and unwraps DEKs using AWS KMS keys; supports KMS alias-based addressing for tenant key rotation; relies on AWS IAM for KMS API access control.
- **`adapters/kms/inproc`** — in-process implementation using AES-256-GCM with a process-local KEK held in memory. Intended for development, integration tests, and the SQLite-backed local profile. Not suitable for production.

Adopters needing Google Cloud KMS, HashiCorp Vault Transit, Azure Key Vault, or a hardware security module integrate by implementing the `KeyStore` interface in their own module. The framework does not constrain the KMS choice.

### 5.5 Key custody — separation of duties

The compliance-relevant property: **the framework database never contains an unwrapped DEK or a KEK in plaintext**. The `subject_keys` table holds DEKs encrypted under the tenant KEK; the KEK lives in the KMS, which the framework reads from but does not write to (the KMS's own lifecycle — rotation, deletion, multi-region replication — is the KMS's responsibility).

This separation means:

- A compromise of the framework database alone does not yield personal data plaintext (the attacker has only wrapped DEKs).
- A compromise of the KMS alone does not yield personal data plaintext (the attacker has only KEKs, but no ciphertext).
- An attacker would need both the database and KMS access to compromise personal data.

This satisfies the typical regulator expectation that key material and protected data live in separate trust zones with separate access controls.

### 5.6 Key rotation

The framework supports KEK versioning. Each wrapped DEK carries the KEK version under which it was wrapped. `RewrapDEK` re-encrypts a DEK under a newer KEK version; `ListStaleSubjectKeys` (in the storage adapters) returns subjects whose DEKs are wrapped under an older KEK.

The mechanics are present; the operational tooling is not. There is no `esctl rotate-kek` command, no scheduled rotation worker, no automated retirement of old KEK versions. Rotation today requires the adopter to write a small Go program that iterates stale subject keys and calls `RewrapDEK` for each. Documented in Addendum A; an `esctl` subcommand is on the roadmap.

### 5.7 Plaintext sidecar — `payload_json`

The `events` table includes a `payload_json` JSONB sidecar column intended for operational tooling (Tier-2 materialised views, debugging, ad-hoc analytics — ADR 0006). The compliance-relevant rules around this sidecar are:

- **`payload_json` is NULL on any event with at least one encrypted field.** The codegen-emitted `Encode` method clears the sidecar when classification rules engage encryption.
- **Application code must not read `payload_json` as a primary source.** It is documented as an operational sidecar only; the canonical source is the encrypted-bytes `payload` column.
- **`payload_json` is regenerated from the proto bytes at every write.** It is not independently sourced. There is no path by which an adopter can write inconsistent values across the two columns through framework APIs.

This eliminates the silent-leak failure mode where developers accidentally read personal data from the JSON sidecar after the fields were supposed to be encrypted.

---

## 6. Personal data governance and data subject rights

### 6.1 Classification taxonomy

The framework's data governance model is defined in ADR 0027 ("Data Governance — Classification, Access Levels, Codegen"). Each event field carries an `(es.v1.data_classification)` Protocol Buffers option whose value drives downstream handling. The classification enum is reproduced below with regulator-relevant interpretation:

| Value | Classification | Encryption | DSAR export | Audit-on-read | Retention policy | Notes |
| ---: | --- | --- | --- | --- | --- | --- |
| 0 | `UNSPECIFIED` | none (treated as `PUBLIC`) | yes | no | standard | Implicit default; codegen warns for sensitive types |
| 1 | `PUBLIC` | none | yes | no | standard | Publicly disclosable |
| 2 | `INTERNAL` | none | **no** | no | standard | Excluded from data subject export — internal metadata |
| 3 | `PERSONAL` | per-subject | yes | no | standard | GDPR Article 4(1) personal data |
| 4 | `QUASI_IDENTIFIER` | per-subject | yes | no | standard | Re-identifiable via combination |
| 5 | `SENSITIVE` | per-subject | yes (with consent) | **yes** | shorter | GDPR Article 9 special-category data |
| 6 | `FINANCIAL` | per-subject | yes | optional | tax-locked | Tax / accounting retention windows apply |
| 7 | `CARDHOLDER` | per-subject | yes | **yes** | PCI scope | PCI DSS cardholder data |
| 8 | `SAD` | **rejected at runtime** | n/a | n/a | n/a | PCI DSS Sensitive Authentication Data — never persistable |
| 9 | `CREDENTIAL` | per-subject | **never** | **yes** | standard | Authentication credentials, API keys |
| 10 | `UNSTRUCTURED` | per-subject | yes | no | standard | Free-form text — assumed to contain PII |

The classification is read by the codegen plugin and the runtime. It is the single source of truth for every PII-relevant decision in the framework.

### 6.2 PII manifest — auditable artifact

For each proto package containing aggregates, the codegen plugin emits a `<package>_pii_manifest.json` file alongside the generated Go code (`cmd/protoc-gen-es-go/main.go:1044`–`1051`). The manifest lists every field of every event with its classification, the chosen encryption mode (`subject_bytes`, `subject_string_base64`, `none`, or `rejected_sad`), DSAR-export disposition, audit-on-read flag, and retention category. The manifest is checked into the repository and reviewed in every code change.

#### Worked example — the manifest

The actual manifest generated for the test `shred` aggregate (`gen/test/shred/v1/shred_pii_manifest.json`) is reproduced in full:

```json
{
  "source": "test/shred/v1/shred.proto",
  "package": "test.shred.v1",
  "events": [
    {
      "name": "test.shred.v1.Registered",
      "fields": [
        {"name": "person_id",    "classification": "DATA_CLASSIFICATION_SUBJECT_FIELD", "encryption": "none",          "dsar_export": true,  "audit_on_read": false, "retention": "standard"},
        {"name": "display_name", "classification": "DATA_CLASSIFICATION_PERSONAL",      "encryption": "subject_bytes", "dsar_export": true,  "audit_on_read": false, "retention": "standard"},
        {"name": "email",        "classification": "DATA_CLASSIFICATION_PERSONAL",      "encryption": "subject_bytes", "dsar_export": true,  "audit_on_read": false, "retention": "standard"},
        {"name": "referrer_id",  "classification": "DATA_CLASSIFICATION_INTERNAL",      "encryption": "none",          "dsar_export": false, "audit_on_read": false, "retention": "standard"}
      ]
    },
    {
      "name": "test.shred.v1.AuthorizedWithSAD",
      "fields": [
        {"name": "person_id", "classification": "DATA_CLASSIFICATION_SUBJECT_FIELD", "encryption": "none",                  "dsar_export": true,  "audit_on_read": false, "retention": "standard"},
        {"name": "auth_code", "classification": "DATA_CLASSIFICATION_PERSONAL",      "encryption": "subject_string_base64", "dsar_export": true,  "audit_on_read": false, "retention": "standard"},
        {"name": "cvv",       "classification": "DATA_CLASSIFICATION_SAD",           "encryption": "rejected_sad",          "dsar_export": false, "audit_on_read": false, "retention": "standard"}
      ]
    }
  ]
}
```

The manifest serves as the **auditable evidence** for several regulator-relevant questions, answerable by inspection of the file alone:

1. *"Show me every place this system stores personal data."* Filter manifests for `classification` containing `PERSONAL`, `QUASI_IDENTIFIER`, `SENSITIVE`, `FINANCIAL`, `CARDHOLDER`, `CREDENTIAL`, or `UNSTRUCTURED`. The proto file at `source` is the authoritative cross-reference.
2. *"What classification did you assign to this specific field on this specific event?"* Direct lookup by event name + field name.
3. *"Which fields will appear in a DSAR export?"* Filter for `dsar_export: true`.
4. *"Which fields trigger access logging?"* Filter for `audit_on_read: true`.
5. *"How is this field encrypted at rest?"* Read `encryption` — `subject_bytes` for `bytes`-typed PII, `subject_string_base64` for `string`-typed PII (base64-encoded ciphertext so the field remains UTF-8 valid), `rejected_sad` for SAD fields (which the framework refuses to persist), `none` for `INTERNAL`, `PUBLIC`, or subject-field columns.

The `DATA_CLASSIFICATION_SUBJECT_FIELD` value in the manifest is the codegen's marker that the field is the subject identifier — it is unencrypted by design (a subject's identifier cannot itself be encrypted under that subject's key without a chicken-and-egg problem) and is the value passed to the Shredder as the lookup key for the DEK.

### 6.3 Right to be forgotten — crypto-shredding

The framework implements GDPR Article 17 (right to erasure) via **crypto-shredding**. The operator-facing API is a single call:

```go
// Adopter code, typically invoked from a DSAR-handling workflow.
err := shredder.ForgetSubject(ctx, "tenant-acme", "employee:42")
```

The operation:

1. The `shred.Shredder.ForgetSubject(ctx, tenantID, subject)` operation is called by the adopter when a data subject's erasure request is granted.
2. The operation executes `UPDATE subject_keys SET dek_wrapped = '', shredded_at = now() WHERE tenant_id = $1 AND subject = $2` (`adapters/storage/postgres/queries/shred.sql`).
3. The DEK is destroyed in place. The tombstone row remains (with the empty `dek_wrapped` column and the `shredded_at` timestamp) as compliance audit evidence that the operation occurred and when.
4. Every subsequent decrypt attempt for that subject returns `shred.ErrShredded`. The codegen handles this case by returning a `RedactedField{Name, Subject, Reason: "shredded"}` placeholder instead of plaintext, so application code that reads historical events for the subject sees explicit redaction rather than a decryption failure (`shred/shred.go:273`–`275`).

The compliance properties of this design:

- **Effectively irreversible.** Once the DEK is overwritten, no party — including the framework operator with full database access — can decrypt the affected ciphertext. The tombstone row cannot be undone without a backup; backup retention is the adopter's responsibility.
- **Preserves the audit trail of what happened.** Events themselves are not deleted; the immutable record that, for example, "an order was placed at 14:32 on 2025-04-12 by subject X" remains. The redaction affects only the personal-data fields.
- **Compatible with append-only stream guarantees.** No event is mutated by shredding; the integrity hash chain (Section 9) is not affected.
- **Tenant-scoped.** Shredding subject X in tenant A has no effect on subject X in tenant B (different DEK, different KEK).

### 6.4 Data subject access requests — current state

The framework exposes the primitives needed to fulfil a Data Subject Access Request (Article 15) but does not ship a turnkey export tool:

- The `pii_manifest.json` per aggregate identifies which fields are exportable (`dsar_export = true` for all classifications except `INTERNAL` and `CREDENTIAL`).
- The storage adapters expose `ReadAllForTenant` and per-stream `ReadStream`; an adopter can iterate the data subject's streams and apply the manifest filter.
- The Shredder's `DecryptField` returns plaintext for live subjects and `RedactedField` placeholders for shredded subjects.

What is **not** provided:

- An `esctl export-subject --tenant T --subject S --format json|csv` command.
- A pre-built mapping from "subject identifier in the application sense" to "stream identifiers that contain that subject's data" — the adopter must maintain that mapping themselves, typically via a denormalised lookup table.
- A redaction policy for derived data in projections — once events are read into a projection, the adopter must implement projection-side shredding.

DSAR export tooling is on the roadmap (Addendum B, Phase 2).

### 6.5 Special-category data and consent

Fields classified `SENSITIVE` (GDPR Article 9 special-category data: health, biometric, sexual orientation, political opinions, etc.) are subject to additional controls:

- **Encryption is mandatory** — the codegen will not emit a `SENSITIVE` field that is not encrypted.
- **Audit-on-read is mandatory** — the codegen flags these fields for runtime audit emission. **The audit logger itself is not yet wired in the framework**; the codegen produces the flag, and the adopter is expected to wire a logger to emit access events. This is recorded in Addendum A as Gap G-016.
- **Adopters are expected to record a lawful basis** for processing (typically explicit consent under Article 9(2)(a) or another Article 9 exemption). The framework does not record consent; that is an adopter responsibility.

### 6.6 Cross-border data transfers

The framework has **no awareness of data residency**. It does not tag events with origin region, does not enforce geo-restriction on reads or projections, and does not provide a multi-region replication adapter (the `global_position` design is store-wide monotonic and incompatible with naive multi-master replication — ADR 0009).

Adopters with cross-border-transfer obligations (GDPR Chapter V, UK GDPR equivalent) must:

- Deploy the framework within the relevant jurisdiction (one deployment per residency region).
- Implement cross-region data flows at the application or transport layer (typically via the publisher seam — events published to a region's bus stay in that region unless an explicit cross-region forwarder is built).
- Document Standard Contractual Clauses, Transfer Impact Assessments, and supplementary measures separately.

This is recorded in Addendum A (Gap G-013) and on the roadmap (Phase 4) for a possible future cross-region replication adapter.

---

## 7. Tenant isolation

### 7.1 Design — defense in depth

The framework treats multi-tenancy as a first-class, non-optional concern (ADR 0007). Tenant isolation is enforced in **two layers** on PostgreSQL deployments:

1. **Application layer** — every framework operation requires an explicit tenant identifier in context via `es.WithTenant(ctx, tenantID)`. The runtime calls `es.RequireTenant(ctx)` at every API boundary and returns `ErrTenantMissing` if absent. The empty string is rejected; there is no "default tenant", no "guess from connection", no fallback (`es/tenant.go:13`–`36`).
2. **Database layer** — PostgreSQL Row-Level Security policies key off a session-local GUC (`app.tenant_id`) bound at the start of every transaction by the framework's pgx interceptor. RLS is enabled with `FORCE ROW LEVEL SECURITY` on every tenant-scoped table, so even the table owner is subject to the policy (ADR 0032, migration `00015_rls_tenant_isolation.sql`).

The application-layer contract is intentionally explicit in code. A typical command handler looks like:

```go
// Adopter HTTP handler — after the authn middleware has identified
// the caller and the authorisation layer has determined the caller's
// tenant scope.
ctx = authz.WithPrincipal(ctx, principal)
ctx = es.WithTenant(ctx, principal.TenantID)

// Any framework call from here on inherits both. Omitting WithTenant
// causes the next framework call to return es.ErrTenantMissing.
result, err := runtime.Handle(ctx, command)
```

There is no API to call `runtime.Handle` without a tenant in context; the runtime extracts and validates it before any other work. Reviewers can grep the codebase for `es.WithTenant` to see every point where tenant context is established.

The two layers are independent and complementary. The application layer catches the "I forgot to bind a tenant" failure mode loudly; the database layer catches the "I wrote a hand-rolled query that forgot the WHERE clause" failure mode silently (returning zero rows rather than leaking).

### 7.2 Database layer — PostgreSQL RLS

Migration `00015_rls_tenant_isolation.sql` (added 2026-06-04, merged in pull request #23) introduces:

- Two PostgreSQL roles: `eventstore_app` (no `BYPASSRLS`) for tenant-scoped operations, and `eventstore_admin` (with `BYPASSRLS`) for the framework's own cross-tenant operations (the store-wide global-position cursor, cross-tenant outbox drain, cross-tenant state-cache invalidation, admin tooling).
- `ENABLE ROW LEVEL SECURITY` and `FORCE ROW LEVEL SECURITY` on the ten tenant-scoped tables: `events`, `unique_claims`, `subject_keys`, `outbox`, `state_cache`, `projection_checkpoint`, `projection_dlq`, `processed_events`, `state_stream_subscribers`, `subscriber_dlq`.
- A `tenant_isolation` policy on each table:

  ```sql
  USING      (tenant_id = current_setting('app.tenant_id', false))
  WITH CHECK (tenant_id = current_setting('app.tenant_id', false));
  ```

  The `false` argument to `current_setting` causes the policy to **error** if the GUC is unset on a connection that has never had it set — a loud signal that the tenant binding was bypassed.
- A relaxed policy on `unique_claims` that admits the `__global__` sentinel for cross-tenant uniqueness constraints (ADR 0007 reserves this string and forbids it as a real tenant identifier).

The framework's pgx integration issues `SELECT set_config('app.tenant_id', $1, true)` at the head of every transaction (the parameterised equivalent of `SET LOCAL`, scoped to the transaction so it cannot leak past commit). Cross-tenant code paths use a separate `eventstore_admin` pool, configured by the adopter via `WithAdminPool` on the adapter constructor. Without that admin pool, cross-tenant calls fail with the explicit `ErrAdminPoolRequired` sentinel (`adapters/storage/postgres/adapter.go:47`; descriptive comment at lines 41–46).

### 7.3 Database layer — SQLite

SQLite has no row-level security feature. The framework's SQLite adapter relies entirely on application-layer enforcement and recommends one database file per tenant in production (ADR 0007). The reasoning:

- One file per tenant gives **structural isolation by file-system boundary** — a tenant compromise cannot read another tenant's file without a separate file-system breach.
- Backup, restore, and offboarding become file-level operations (copy, restore, delete).
- The recommendation is enforced operationally by the adopter; the framework does not enforce it programmatically.

For development and integration testing, the SQLite adapter supports multi-tenant deployments in a single file with application-layer-only isolation. This is **not recommended for production** with regulated data and is documented as Gap G-007 in Addendum A. The roadmap (Phase 3) proposes a `SQLitePerTenantPool` helper that opens and caches one connection pool per tenant from a single configuration, structurally preventing cross-tenant queries.

### 7.4 Migration ramp for existing deployments

For adopters upgrading from a pre-RLS framework version, an explicit migration ramp is provided. The `WithoutRLSEnforcement()` option allows the adapter to fall back to the main pool for cross-tenant operations when no admin pool is configured (`adapters/storage/postgres/adapter.go`, ADR 0032 § Migration ramp). The option is safe in two states only:

1. Migration 00015 has not been applied (no policies to enforce).
2. The main pool's PostgreSQL role has `BYPASSRLS` (a superuser used in development, or a transitional grant during the role split).

After migration 00015 is applied and the main pool's role is non-privileged, the option silently returns zero rows from cross-tenant queries. The option's doc string spells out the unsafe state explicitly, and production deployments are expected to remove the option once `WithAdminPool` is wired.

### 7.5 Cross-tenant operations — explicit and loud

Operations that legitimately span tenants are explicitly routed through the admin pool and labelled at the call site:

- `ReadAll(ctx, fromPosition, limit)` — store-wide global-position cursor.
- `PendingOutbox(ctx, "", ...)` — cross-tenant outbox drain.
- `QuarantinedStreams(ctx, "", ...)` — cross-tenant quarantine list.
- `WipeStateCacheForType(ctx, "", typeURL)` — cross-tenant state-cache invalidation for schema-version rollouts.
- `List(ctx)` projection-checkpoint enumeration.
- `ListStateStreamSubscribers(ctx)`.
- `CleanupProcessedEvents(ctx, name, olderThan)`.

Each of these methods requires the admin pool. Code reviewers can grep the codebase for `a.admin()` to see every cross-tenant call site; operators can audit which deployments have `WithAdminPool` configured. The two-pool design makes cross-tenant intent observable, which auditors typically value.

---

## 8. Authentication and authorisation

### 8.1 Authentication — delegated to the adopter

The framework does **not** authenticate users, services, or API callers. Authentication is the adopter's transport-layer responsibility. The framework consumes an authenticated `Principal` injected into context by the adopter's middleware (`authz/authz.go:54`–`64`):

```go
ctx := authz.WithPrincipal(ctx, p)
// ... framework call uses authz.PrincipalFrom(ctx)
```

Typical adopter implementations:

- **Bearer token / JWT** — adopter parses `Authorization: Bearer ...` in HTTP middleware, validates the token (signature, expiry, audience, issuer), maps claims to a `Principal`, calls `WithPrincipal`.
- **Mutual TLS** — adopter inspects the client certificate from the TLS handshake, maps the subject DN to a `Principal`.
- **API key** — adopter looks up the key in a credential store, constant-time-compares (`crypto/subtle`), maps to a `Principal`.

The framework provides the seam (`Principal` type, context helpers) but ships no authentication implementation. This is consistent with the library-delivery model and avoids the framework opining on identity-provider choice.

### 8.2 Authorisation — Cedar adapter

The framework defines a pluggable authorisation interface in `authz/authz.go` (`authz.Policy.Authorize(ctx, req)`). The reference implementation uses Amazon's Cedar policy language, packaged as `adapters/authz/cedar/policy.go`.

Cedar provides:

- A declarative policy language with formal verification properties.
- Entity-attribute-resource-action evaluation: `permit (principal in Group::"admins", action == Action::"WriteEvent", resource in Tenant::"acme")`.
- Static analysis of policy sets (Cedar's analyser proves that a policy never permits unintended actions).

The Cedar adapter is **not wired by default** into the framework's runtime. The cookbook recipe 5 (`docs/cookbook/05-layered-authz.md`) documents the integration pattern: wrap `aggregate.Runtime.Handle` with an authorisation interceptor that constructs a Cedar request from the incoming command + the principal in context, calls `cedar.Authorize`, and returns `ErrUnauthorized` on denial.

Adopters that prefer OPA, Casbin, or hand-rolled authorisation can implement `authz.Policy` against their engine of choice; the framework's interface is intentionally minimal.

### 8.3 Actor identity on events

Every event carries an `Actor` struct in its envelope (`es/actor.go:32`–`41`):

```go
type Actor struct {
    Kind       ActorKind         // user, system, service, integration
    Principal  string            // primary identifier; denormalised to indexed column
    OnBehalfOf string            // "service acting on behalf of"; empty when N/A
    APIKeyID   string            // key id (not the secret), if applicable
    Attributes map[string]string // free-form attribution (device, region, …)
}
```

`ActorKind` is one of `ActorUser`, `ActorSystem`, `ActorService`, `ActorIntegration`, or `ActorUnspecified`. A typical adopter-side construction:

```go
// HTTP handler — after authn middleware identifies the caller.
actor := es.Actor{
    Kind:      es.ActorUser,
    Principal: "user:alice@example.com",
    APIKeyID:  "ak_2hG7…", // never the secret itself
    Attributes: map[string]string{
        "request_id": req.Header.Get("X-Request-Id"),
        "client_ip":  realClientIP(req),
    },
}
cmd := &orderv1.PlaceOrder{ /* … */ }
// Actor is attached to the command, propagates into the resulting event(s).
result, err := runtime.Handle(ctx, cmd, aggregate.WithActor(actor))
```

The Actor is denormalised to a dedicated `actor_principal` column on the `events` table with a covering index (`events_actor_principal_idx`), making audit queries by principal cheap:

```sql
-- "Show me every action user:alice took in tenant T over the past 30 days."
SELECT event_id, type_url, occurred_at, recorded_at, payload
  FROM events
 WHERE tenant_id = 'tenant-acme'
   AND actor_principal = 'user:alice@example.com'
   AND recorded_at >= now() - interval '30 days'
 ORDER BY global_position;
```

The framework does **not** auto-populate the Actor — the adopter is responsible for ensuring every command carries an accurate Actor, and reviewers should refuse commands that hard-code an empty or anonymous Actor.

### 8.4 Tenant authorisation

Tenant assignment is set at the application boundary (typically derived from the authenticated principal's tenant claim) and threaded through every framework call. The framework does not validate the principal's right to act in the named tenant — that authorisation decision is the adopter's responsibility (it is the canonical use case for the Cedar adapter or equivalent).

The framework's contribution to tenant authorisation:

- It refuses to operate without a tenant in context, structurally preventing the "missed tenant check" class of bugs.
- It enforces tenant isolation at both the application and database layers (Section 7).
- The Actor + tenant combination on every event provides an auditable record of "who acted in which tenant".

---

## 9. Audit trail and tamper evidence

### 9.1 Events as the audit log

In an event-sourced system, the event log **is** the audit log. Every state change is recorded as an immutable event with full provenance:

- **What** happened — the event type and payload.
- **When** — `occurred_at` (logical / domain time) and `recorded_at` (wall-clock at commit).
- **Who** — the `Actor` (principal, kind, on-behalf-of, API-key id).
- **Why / under what** — `correlation_id` (the originating request), `causation_id` (the immediate parent event), `command_id` (the command that produced this event).
- **In which tenant** — the `tenant_id` from the stream identifier.
- **What position in the timeline** — `version` per stream, `global_position` store-wide.
- **What integrity link** — `hash` and `prev_hash` for the SHA-256 chain (Section 9.3).

There is no separate "audit log" stream. The system of record is the audit log. This is a deliberate design decision (ADR 0005) that eliminates the failure mode where a "real" change is made but the audit log fails to record it — the two cannot diverge because they are the same row.

### 9.2 Append-only enforcement

- **No UPDATE or DELETE paths exist** in the storage adapters for the `events` table, with one exception: the chain-backfill operation (Section 9.4), guarded by `WHERE hash IS NULL`.
- The PostgreSQL Row-Level Security policy on `events` uses `WITH CHECK (tenant_id = current_setting(...))`, which constrains INSERTs to the bound tenant — there is no path to insert events under a different tenant identifier even with raw SQL.
- The advisory lock and sequence allocation guarantee monotonic, gap-free `global_position` in commit order, eliminating the "events appear out of order" attack on the global timeline.

### 9.3 Tamper-evident hash chain

ADR 0028 introduces a per-stream SHA-256 hash chain. Each event carries:

- `hash` — `SHA256(prev_hash || canonical(envelope-minus-mutable-fields))`.
- `prev_hash` — the hash of the predecessor event on the same stream, or 32 zero bytes for the genesis event.

The hash subset is locked: a fixed list of envelope fields participates in the hash (`event_id`, `tenant_id`, `stream_id`, `version`, `type_url`, `schema_version`, `occurred_at`, `correlation_id`, `causation_id`, `command_id`, `actor`, `payload`, `encryption_key_refs`). Mutable fields (`hash`, `prev_hash`, `recorded_at`, `global_position`, `payload_json`) are excluded — they are either set at insert time outside the application's control, or operationally rewritten.

Hash computation: `es.ComputeChainHash(prevHash, envelope) ([]byte, error)` at `es/chain.go:36`–`52`.

Verification: `es.VerifyStreamChain(ctx, store, sid) error` at `es/chain.go:97`–`114`. Replays the stream, recomputes each hash from content + predecessor, compares to the stored value. Returns `ErrChainBroken` wrapped with the offending version on mismatch. Verification is opt-in — it runs at the cadence the auditor chooses, not on every read.

**Compliance properties:**

- Any post-hoc mutation of an event row's content (or reordering of versions) breaks the chain. Verification surfaces the offending stream and version.
- The chain is local to each stream; verifying one tenant's stream does not require reading another tenant's data.
- The hash is computed on the canonical proto serialisation; replays are deterministic across Go versions.

### 9.4 Chain rebuild for pre-existing streams

Streams that pre-date the ADR 0028 migration carry NULL `hash` and `prev_hash` values. The `RebuildStreamChain` operation (`es/chain.go:168`–`194`) walks such a stream, recomputes the chain values from the historical content, and writes them via a guarded UPDATE (`WHERE hash IS NULL`). Properties:

- **Idempotent.** A second pass returns `BackfilledCount = 0`, `VerifiedCount = full-stream-length`.
- **Never overwrites a non-NULL hash.** A row that already carries a hash is verified, not rewritten.
- **Per-stream.** Backfill can be parallelised across streams without coordination.

This satisfies the regulator-facing question of "how do you handle the historical data that predates your integrity controls?"

### 9.5 External witness and Merkle anchoring

For the highest assurance posture — where the framework operator's own database cannot be trusted in the worst case — the framework documents (but does not yet ship) the pattern of periodically computing a Merkle root over a window of events and publishing the root to an external append-only log: a blockchain (Bitcoin OP_RETURN, Ethereum log topic), a public transparency log (Certificate Transparency, Sigstore Rekor), or a regulator-operated witness service.

The cookbook recipes 19 (Merkle anchors) and 20 (external witness) document the design. Runtime code for the Merkle batcher and the external-witness publisher are **not yet implemented**. This is recorded in Addendum A as Gap G-010 and on the roadmap (Phase 3).

### 9.6 Time integrity

Every event carries two timestamps:

- `occurred_at` — the domain-meaningful time, set by the caller. May be in the past for retroactive corrections.
- `recorded_at` — the wall-clock time at database commit, set by the storage adapter via `clock_timestamp()` on PostgreSQL and equivalent on SQLite.

The framework does **not** sign timestamps with a trusted time source (no RFC 3161 timestamping, no Trusted Computing Group attestation). For regulators requiring proof of when an event was recorded, the recommended pattern is to chain the framework's hash output into an external witness on a tight cadence (Section 9.5) — the witness service's own time is the trusted reference.

---

## 10. Schema evolution and data integrity

### 10.1 Schema migration discipline

ADR 0030 defines a mandatory migration tier taxonomy. Every schema-touching change (proto field changes, storage migrations, runtime state-machine changes) must declare one of six tiers in its pull request:

| Tier | Description | Required artefacts |
| --- | --- | --- |
| A | Pure additive | none — proto3 default values handle compatibility |
| B | Semantic shift behind same wire | `schema_version` bump + upcaster |
| C | State shape changed | `StateSchemaVersion` bump + `state_cache` invalidation |
| D | Storage schema migration | new numbered migration file + reversible `+goose Down` |
| E | Wire-format break | dual-write window + cutover plan + ADR |
| F | Breaking semantic change without migration story | only with ADR justifying why no migration is possible |

The discipline is enforced by reviewer practice (the pull-request template requires tier declaration) and by code-level guards (the `state_schema_version` column on `state_cache` triggers replay-from-scratch on mismatch — `ADR 0023`).

### 10.2 Upcasters

ADR 0013 defines the schema-evolution mechanism. Each event carries a `schema_version` field (`uint32`) in its envelope. On read, the framework hands `(typeURL, schemaVersion, payload)` to a `aggregate.Codec[T]` implementation. Adopters implement upcasting by wrapping the codegen-emitted `EventCodec` with a translation layer that dispatches on `schemaVersion` and transforms legacy payloads into the current shape.

Two reference upcasters are shipped in the test tree:

- `gen/test/unitsmigration/v1/migrating_codec.go` — Tier B example, millisecond-to-microsecond unit conversion at the same wire encoding.
- `gen/test/piimigration/v1/migrating_codec.go` — Tier B example, PII classification evolution.

The cookbook recipe 21 documents the pattern.

### 10.3 State-cache invalidation on schema change

When an aggregate's State proto changes shape (Tier C), the cached state rows would decode incorrectly under the new shape. The framework handles this by:

1. The adopter bumps `aggregate.Runtime.StateSchemaVersion`.
2. On Load, the runtime compares the cached `state_schema_version` against the runtime's value.
3. On mismatch, the cached row is silently discarded; the aggregate is rebuilt from full event replay.
4. Optionally, the adopter runs `aggregate.RebuildStateCache` to repopulate the cache proactively after deploy.

This ordering matters: the runtime change must deploy before the State proto change, so old state rows are invalidated before new code attempts to decode them. The discipline is documented in CLAUDE.md and ADR 0023.

### 10.4 Backward and forward compatibility

- **Forward compatibility** (old code reads new data) — protected by proto3's default-value semantics for added optional fields. New fields not understood by old code are ignored.
- **Backward compatibility** (new code reads old data) — protected by upcasters for semantic changes. The historical bytes on disk are never rewritten; reads flow through the upcaster path.
- **Breaking changes** require explicit dual-write windows and operator coordination (Tier E in the migration discipline).

---

## 11. Operational resilience

### 11.1 Durability — write transaction

The single-transaction guarantee (Section 3.2) is the foundation. Once `COMMIT` returns successfully, the event(s), the updated state, and the outbox handoff are all durably persisted. There is no eventual consistency between these three.

The adopter's PostgreSQL configuration determines the durability of `COMMIT`:

- `synchronous_commit = on` — the default for PostgreSQL, requires writes to reach the WAL on local disk before acknowledgement.
- `synchronous_commit = remote_write` or `remote_apply` with synchronous replication — required for the highest durability profile in HA deployments.

The framework does not opine on these settings; they are deployment configuration.

### 11.2 At-least-once delivery — outbox publisher

The outbox table is the durability seam between the writer transaction and the downstream publisher (ADR 0012, ADR 0014). The drain process:

1. Selects pending outbox rows (`published_at IS NULL`), JOINs to `events` for the envelope.
2. Hands the envelope to the configured `Publisher`.
3. On success, sets `published_at = now()`.
4. On failure, increments `attempts`, sets `last_error`, schedules `next_attempt_at` with exponential backoff (`adapters/storage/postgres/migrations/00003_outbox_next_attempt_at.sql`).
5. After `MaxAttempts` (operator-configurable), the row enters a quarantine state visible via `QuarantinedStreams`.

The contract is **at-least-once delivery**. Subscribers must be idempotent. The framework provides a `processed_events` ledger for projection-side deduplication (`adapters/storage/postgres/queries/processed_events.sql`).

### 11.3 Per-command subscriber batch delivery

ADR 0029 introduces per-command-batch subscriber delivery, batching together all events produced by a single command so subscribers receive a coherent unit. Failures fail the whole batch (recorded in `subscriber_dlq` with the entire `event_ids[]` array), making subscriber retry and replay operate on the same unit as the original write.

### 11.4 Workflow back-ends

For adopters who require workflow semantics beyond at-least-once delivery (long-running sagas, deterministic replay, exactly-once command effects), the framework provides workflow back-end adapters (ADR 0025, ADR 0026):

- **Restate** (`adapters/cmdworkflow/restate`) — Restate runtime handles command serialisation, idempotency, retries, and durable execution. Codegen emits Restate handlers per aggregate.
- **DBOS** (`adapters/cmdworkflow/dbos`) — DBOS Transact provides similar guarantees over a PostgreSQL backend.
- **In-process** (`adapters/cmdworkflow/inproc`) — a non-durable in-process workflow runner for tests.

The choice of workflow back-end is the adopter's. Each is wired through the same `cmdworkflow.Workflow` interface.

### 11.5 Drain locker and projection locker

To prevent two replicas from concurrently draining the outbox or running the same projection, the framework uses PostgreSQL session-level advisory locks (`adapters/storage/postgres/drain_locker.go`, `projection_locker.go`). The locker contract:

- A replica calls `TryAcquireDrainLock(ctx, key)` and proceeds only if it succeeds.
- The lock is held for the lifetime of the connection; release on disconnect.
- Other replicas observe the lock and skip without retrying.

This provides leader election without an external coordination service.

### 11.6 Disaster recovery — current state

The framework's recovery story rests on the durability of the underlying PostgreSQL or SQLite deployment. The adopter is responsible for:

- Configuring PostgreSQL replication (streaming, logical, or cluster solutions like Patroni, Stolon, Postgres Operator).
- Snapshotting or continuously archiving the database (pg_basebackup, WAL archiving, file-system snapshots).
- Testing restore on a documented cadence.
- Documenting Recovery Time Objective (RTO) and Recovery Point Objective (RPO) commitments.

The framework's contribution to recoverability:

- Event sourcing means **all derived state can be rebuilt from the event log**. A complete loss of `state_cache` and projection read models is recoverable from the events; the framework provides `aggregate.RebuildStateCache` and the projection runtime's reset-and-replay path.
- Per-tenant partitioning enables per-tenant restore.
- The chain hash provides a tamper-detection mechanism that survives backup/restore — a corrupted backup is detectable by re-verifying the chain on restore.

What is **not provided**:

- Backup automation tooling.
- Restore-test automation in CI.
- A documented disaster-recovery runbook.

These are recorded in Addendum A and on the roadmap.

---

## 12. Change management and software development life cycle

### 12.1 Source control and review

- The framework is developed on GitHub (`github.com/laenenai/eventstore`).
- The main branch is protected; all changes land via pull request with at least one review.
- Conventional commit messages drive the release tag automation (`scripts/release.sh`).
- Pre-commit hooks enforce formatting and lint; `--no-verify` is forbidden in the project conventions (CLAUDE.md).

### 12.2 CI pipeline

`.github/workflows/ci.yml` runs on every pull request and on `main`:

1. `go vet` across every module in the workspace.
2. `task generate:check` — fails the build if generated code is out of date relative to source `.proto` files. This prevents stealth API changes that bypass codegen.
3. `task build` across every module.
4. `task test:cover` — runs unit and integration tests, including the PostgreSQL adapter via testcontainers and the SQLite adapter in-process.
5. `task lint:proto` — `buf lint` plus `buf breaking` against `main`. Breaking proto changes require explicit ADR and tier-E migration discipline.

The CI pipeline does **not currently include**:

- Vulnerability scanning of dependencies (`govulncheck`).
- Static security analysis (`gosec`).
- Container-image scanning (`trivy`, `grype`).
- Software Bill of Materials generation (`syft` → CycloneDX or SPDX).
- Renovate or Dependabot for dependency-update automation.

These are recorded in Addendum A (Gap G-001) and on the roadmap (Phase 1).

### 12.3 Architecture Decision Records

All material design decisions are captured as ADRs in `docs/adr/`. ADRs are **immutable once Accepted**: a changed decision is recorded as a new ADR that supersedes the old one, never as an edit. This produces a verifiable audit trail of why the system is the way it is.

At the time of writing there are 32 ADRs. The index is at `docs/adr/README.md`. The most recent additions are:

- ADR 0030 — Schema Migration Discipline.
- ADR 0031 — Execution Queues (backend-neutral routing hint).
- ADR 0032 — PostgreSQL Row-Level Security for Tenant Isolation.

### 12.4 Cookbook recipes

`docs/cookbook/` provides 22 recipes for application-level patterns the framework deliberately does not bake in: sagas, process managers, schema evolution, projection rebuilds, snapshots, layered authorisation, crypto-shredding, the HTTP edge, etc. Each recipe follows a uniform structure (problem, primitives used, primitives deliberately not used, failure modes), suitable as design-review input for adopters.

### 12.5 Release process

Releases are operator-triggered via `task release VERSION=vX.Y.Z`, which:

1. Validates a clean tree on `main`, in sync with origin.
2. Tags every published module at the requested version.
3. Pushes tags.
4. The CI release workflow (`.github/workflows/release.yml`) publishes the modules.

Synchronised release across all modules prevents the failure mode where an adopter consumes the framework at `v1.2.0` and the Postgres adapter at `v1.1.5` and hits compatibility issues.

### 12.6 Code conventions enforcing security posture

The project's `CLAUDE.md` and the user's global standards document codify several security-relevant code conventions, enforced by review:

- No `init()` functions — dependencies wired explicitly in `main.go` or constructor functions, eliminating hidden setup that bypasses validation.
- All filesystem paths validated via a `safePath()` function preventing traversal.
- API key comparison via `crypto/subtle` constant-time comparison.
- TLS terminated at reverse proxy or load balancer; plain HTTP in-cluster is explicitly acceptable.
- Standard security headers (`X-Content-Type-Options: nosniff`, `X-Frame-Options: DENY`, `X-XSS-Protection: 0`, `Referrer-Policy: strict-origin-when-cross-origin`) required at the HTTP edge.
- Rate limiting (per-IP token bucket) at the HTTP edge.
- CORS explicit allowlist; no wildcards.
- Graceful shutdown with `signal.NotifyContext` and 30-second drain timeout.

These conventions apply to the adopter's deployment binaries built on the framework; the framework itself enforces them via the HTTP edge cookbook recipe and code-review discipline.

---

## 13. Observability

### 13.1 OpenTelemetry tracing and metrics

Pull request #18 (commit `a183fa3`, 2026-05-29) introduced OpenTelemetry instrumentation on the hot paths. The framework registers a tracer and a meter under instrumentation name `eventstore`:

- **Tracer** — `obs.Tracer = otel.Tracer("eventstore")` at `es/obs/tracer.go:18`. No-op when no `TracerProvider` is registered.
- **Metric instruments** initialised at package load (`es/obs/meter.go:44`–`89`):
  - `eventstore.commands.total` — command dispatch count (Int64Counter).
  - `eventstore.command.duration` — Handle latency in seconds (Float64Histogram).
  - `eventstore.events.appended.total` — events persisted count (Int64Counter).
  - `eventstore.store.append.duration` — storage append latency (Float64Histogram).
  - `eventstore.store.read_stream.duration` — stream read latency (Float64Histogram).

All spans and metrics carry the OpenTelemetry semantic-conventions attributes (`db.system`, `eventstore.tenant`, `eventstore.stream_id`, `eventstore.event_count`, `eventstore.version`).

### 13.2 Structured logging

The framework does **not** emit structured logs from library code (only metrics and traces). The user's global standards document specifies `slog` as the structured-logging library; adopters wire `slog.SetDefault` and the framework's calls inherit through context propagation for trace correlation.

### 13.3 Audit-on-read for sensitive classifications

Fields classified `SENSITIVE`, `CARDHOLDER`, or `CREDENTIAL` are flagged by the codegen for audit-on-read. **The audit logger itself is not yet wired in the framework**. The codegen produces the flag in `pii_manifest.json`; the adopter is expected to wire an interceptor that emits an audit event on each access. This is recorded as Gap G-016 in Addendum A.

### 13.4 Operational tooling — `esctl`

The `cmd/esctl` operator command-line tool provides read-only inspection:

| Subcommand | Purpose |
| --- | --- |
| `stream` | List, read, watch streams by type or id |
| `event` | Inspect a single event |
| `state` | Read aggregate state from `state_cache` |
| `projection` | List, read projection checkpoints and DLQ |
| `outbox` | Inspect outbox queue and retry state |
| `events` | Tail recent events with optional filter |
| `state-cache` | Read `state_cache` rows |

The tool auto-detects PostgreSQL versus SQLite from the `--db` URL scheme and respects a default tenant from `--tenant` or environment.

**Notably absent** (recorded as gaps): chain verification (`verify-stream-chain`), DSAR export (`export-subject`), KEK rotation (`rotate-kek`), tenant offboarding (`offboard-tenant`), and crypto-shred operations from the CLI.

---

## 14. Software supply chain

### 14.1 Dependency management

- The framework uses Go modules (`go.mod`) with semantic-version pins. There is no vendoring.
- Multi-module layout isolates dependency clusters: adopters of the SQLite adapter do not pull pgx; adopters of the Cedar adapter do not pull Restate's SDK.
- Heavy dependencies are confined to their adapter modules: `github.com/jackc/pgx/v5` in `adapters/storage/postgres`, `modernc.org/sqlite` in `adapters/storage/sqlite`, `cedar-policy/cedar-go` in `adapters/authz/cedar`, etc.

### 14.2 Generated code

- Generated code lives under `gen/` and the per-adapter `pgstore/` directories. Generated files are never hand-edited.
- The `task generate:check` CI gate fails the build if generated code drifts from source. Adopters cloning the repository at any commit can reproduce the generated artefacts.

### 14.3 Container image conventions

The user's global standards document specifies multi-stage Docker builds with `gcr.io/distroless/static-debian12` as the runtime image — a minimal base with no shell, no package manager, and a reduced attack surface. Adopters are expected to follow this convention for production deployments.

### 14.4 Gaps — supply-chain hardening

The following supply-chain controls are **not yet in place** and are recorded in Addendum A:

- No vulnerability scanning of Go dependencies in CI (`govulncheck`).
- No static security analysis (`gosec`).
- No Software Bill of Materials generation per release (`syft` or `cyclonedx-gomod`).
- No automated dependency updates (Dependabot, Renovate).
- No artefact signing for releases (Sigstore Cosign, in-toto attestations).
- No reproducible-build verification.

These are prioritised in Phase 1 of the roadmap.

---

## 15. Compliance control mapping

This section maps the framework's primitives to the control requirements of the three target frameworks: SOC 2 (Trust Services Criteria), ISO/IEC 27001:2022 (Annex A), and EU DORA / EBA ICT risk-management guidelines.

For each control, the mapping notes:

- **What the framework provides** — the primitive baked into the library.
- **What the adopter must add** — operational practice, configuration, or external tooling.
- **Coverage** — Full / Partial / Adopter-only.

### 15.1 SOC 2 Type II — Trust Services Criteria

#### Common Criteria — Control Environment, Communication, Risk Assessment, Monitoring (CC1–CC5)

These criteria address organisational controls (governance, policies, risk-assessment processes) that are **adopter-only**. The framework provides no automation for board oversight, policy approval workflows, or risk-assessment cadence. Coverage: Adopter-only.

#### CC6 — Logical and physical access controls

| Sub-criterion | Framework provides | Adopter must add | Coverage |
| --- | --- | --- | --- |
| CC6.1 — Logical access security | Tenant context enforcement (Section 7); Cedar authorisation adapter (Section 8.2); Actor on every event | Identity provider integration; authentication middleware; authorisation policy authoring | Partial |
| CC6.2 — User provisioning | — | All user lifecycle | Adopter-only |
| CC6.3 — User authorisation | Cedar adapter; Principal in context | Cedar policy set or equivalent | Partial |
| CC6.6 — Logical access to information assets | RLS policies (Section 7.2); per-subject encryption (Section 5) | Connection-string security; role split deployment | Partial |
| CC6.7 — Transmission of information | — (transport security delegated) | TLS / mTLS termination; certificate management | Adopter-only |
| CC6.8 — Prevent or detect unauthorised software | Append-only event log; hash chain (Section 9) | Code-signing; deployment integrity | Partial |

#### CC7 — System operations

| Sub-criterion | Framework provides | Adopter must add | Coverage |
| --- | --- | --- | --- |
| CC7.1 — Detection of operational issues | OpenTelemetry metrics; structured spans | Alerting; on-call rotation | Partial |
| CC7.2 — Monitoring of system performance | OpenTelemetry meter instruments | Dashboards; SLO definitions | Partial |
| CC7.3 — Evaluation of security events | — | SIEM integration; incident-response process | Adopter-only |
| CC7.4 — Response to security incidents | — | Incident-response runbook; communication plan | Adopter-only |

#### CC8 — Change management

| Sub-criterion | Framework provides | Adopter must add | Coverage |
| --- | --- | --- | --- |
| CC8.1 — Change management process | Pull-request workflow; ADRs; migration tier discipline (ADR 0030); `task generate:check` CI gate | Branch-protection rules; deployment approval workflow | Partial |

#### CC9 — Risk mitigation

| Sub-criterion | Framework provides | Adopter must add | Coverage |
| --- | --- | --- | --- |
| CC9.1 — Risk mitigation activities | — | Risk register; mitigation tracking | Adopter-only |
| CC9.2 — Vendor risk management | Pinned dependencies in `go.mod` | Vendor assessment; SBOM consumption | Partial |

#### Availability criteria (A1)

| Sub-criterion | Framework provides | Adopter must add | Coverage |
| --- | --- | --- | --- |
| A1.1 — Capacity planning | — | Capacity model; growth tracking | Adopter-only |
| A1.2 — System availability | Outbox at-least-once delivery; drain locker for replica coordination | HA database; load balancing; replication | Partial |
| A1.3 — Recovery | Event log as recoverable source of truth; per-tenant partitioning enabling per-tenant restore | Backup automation; restore testing; documented RTO/RPO | Partial |

#### Confidentiality criteria (C1)

| Sub-criterion | Framework provides | Adopter must add | Coverage |
| --- | --- | --- | --- |
| C1.1 — Identification of confidential information | `data_classification` proto option; `pii_manifest.json` per aggregate | Classification practice; review process | Full |
| C1.2 — Protection of confidential information | Per-subject AES-256-GCM encryption; KMS abstraction; SAD rejection at runtime | KMS deployment; key access controls | Full |

#### Processing Integrity criteria (PI1)

| Sub-criterion | Framework provides | Adopter must add | Coverage |
| --- | --- | --- | --- |
| PI1.1 — Inputs validated | Type-safe sealed sum types from proto (ADR 0004); decider Initial/Decide validates | Application-level input validation | Full |
| PI1.2 — Processing complete and accurate | Single-transaction write (events + state + outbox); advisory-lock-serialised global position | — | Full |
| PI1.3 — Outputs delivered | At-least-once outbox publisher; per-batch subscriber DLQ | Subscriber idempotency | Full |
| PI1.4 — Stored data complete and accurate | Hash chain (Section 9.3); tamper-detection | Periodic chain verification | Full |
| PI1.5 — Modifications authorised | Append-only event log; chain enforces no-mutation | — | Full |

#### Privacy criteria (P1–P8)

| Criterion | Framework provides | Adopter must add | Coverage |
| --- | --- | --- | --- |
| P1 — Notice and communication | — | Privacy notice; consent capture UI | Adopter-only |
| P2 — Choice and consent | — | Consent management | Adopter-only |
| P3 — Collection | `data_classification` makes collection visible in the schema | Data-collection policy | Partial |
| P4 — Use, retention, disposal | Crypto-shredding (Section 6.3) | Retention scheduling; disposal workflow | Partial |
| P5 — Access | `ReadStream`, `ReadAllForTenant`, redacted-field placeholders on shredded subjects | DSAR fulfilment process; export tool | Partial (DSAR tool missing — Addendum A) |
| P6 — Disclosure to third parties | Event publisher; Actor on every event | Third-party-disclosure log | Partial |
| P7 — Quality | Schema discipline (ADR 0030); upcasters | Data-quality monitoring | Partial |
| P8 — Monitoring and enforcement | OpenTelemetry; audit log via events | Privacy programme; complaint handling | Partial |

### 15.2 ISO/IEC 27001:2022 — Annex A

Annex A of the 2022 revision groups 93 controls into four themes: Organisational (37), People (8), Physical (14), Technological (34). The framework primarily addresses Technological controls; the others are adopter-only or partial. The mapping below covers the Technological theme and the most directly framework-relevant Organisational controls.

#### A.5 — Organisational controls (selected)

| Control | Title | Framework contribution | Coverage |
| --- | --- | --- | --- |
| A.5.7 | Threat intelligence | — | Adopter-only |
| A.5.8 | Information security in project management | ADRs; cookbook recipes | Partial |
| A.5.9 | Inventory of information and other associated assets | `pii_manifest.json`; ADR index | Partial |
| A.5.10 | Acceptable use of information and other associated assets | — | Adopter-only |
| A.5.11 | Return of assets | Crypto-shredding (subject-level) | Partial |
| A.5.12 | Classification of information | `data_classification` proto option (Section 6.1) | Full |
| A.5.13 | Labelling of information | Codegen-emitted classification on every field | Full |
| A.5.14 | Information transfer | — (transport security delegated) | Adopter-only |
| A.5.23 | Information security for use of cloud services | KMS abstraction enables cloud-KMS use | Partial |
| A.5.24 | Information security incident management planning and preparation | — | Adopter-only |
| A.5.30 | ICT readiness for business continuity | Event log as recoverable truth | Partial |
| A.5.33 | Protection of records | Append-only event log; hash chain | Full |
| A.5.34 | Privacy and protection of PII | Section 6 in its entirety | Full |
| A.5.35 | Independent review of information security | — | Adopter-only |
| A.5.36 | Compliance with policies, rules and standards for information security | This document | Partial |

#### A.6 — People controls

All eight People controls (A.6.1–A.6.8) cover personnel screening, terms of employment, awareness, disciplinary processes, etc. **Adopter-only.**

#### A.7 — Physical controls

All fourteen Physical controls (A.7.1–A.7.14) cover secure facilities, equipment maintenance, cabling, etc. **Adopter-only.**

#### A.8 — Technological controls

| Control | Title | Framework contribution | Coverage |
| --- | --- | --- | --- |
| A.8.1 | User endpoint devices | — | Adopter-only |
| A.8.2 | Privileged access rights | Two-role split on PostgreSQL (`eventstore_app` / `eventstore_admin`) — Section 7.2 | Full |
| A.8.3 | Information access restriction | Tenant isolation (app + RLS); per-subject encryption | Full |
| A.8.4 | Access to source code | GitHub access controls | Adopter-only |
| A.8.5 | Secure authentication | — (delegated) | Adopter-only |
| A.8.6 | Capacity management | OpenTelemetry meter instruments | Partial |
| A.8.7 | Protection against malware | — | Adopter-only |
| A.8.8 | Management of technical vulnerabilities | — (no scanning in CI) | Gap — Addendum A, G-001 |
| A.8.9 | Configuration management | Adopter-owned `main.go`; ADR discipline | Partial |
| A.8.10 | Information deletion | Crypto-shredding (Section 6.3) | Full |
| A.8.11 | Data masking | Classification-driven redacted-field placeholders on shredded subjects | Partial |
| A.8.12 | Data leakage prevention | RLS; `payload_json` nullified on encrypted events | Partial |
| A.8.13 | Information backup | — | Adopter-only (Gap G-006) |
| A.8.14 | Redundancy of information processing facilities | — | Adopter-only |
| A.8.15 | Logging | OpenTelemetry traces; event log itself | Partial |
| A.8.16 | Monitoring activities | OpenTelemetry meter | Partial |
| A.8.17 | Clock synchronisation | — (relies on host clock) | Adopter-only |
| A.8.18 | Use of privileged utility programs | Two-role split; `WithAdminPool` requirement | Partial |
| A.8.19 | Installation of software on operational systems | — | Adopter-only |
| A.8.20 | Networks security | — (transport delegated) | Adopter-only |
| A.8.21 | Security of network services | — | Adopter-only |
| A.8.22 | Segregation of networks | — | Adopter-only |
| A.8.23 | Web filtering | — | Adopter-only |
| A.8.24 | Use of cryptography | AES-256-GCM; per-subject envelope encryption; KMS abstraction | Full |
| A.8.25 | Secure development life cycle | ADRs; tier discipline; review process; codegen verification | Partial |
| A.8.26 | Application security requirements | This document, plus per-aggregate security requirements derived from classification | Partial |
| A.8.27 | Secure system architecture and engineering principles | ADR set (32 ADRs); cookbook (22 recipes) | Full |
| A.8.28 | Secure coding | Conventions in CLAUDE.md; constant-time API key compare; path-traversal validation | Partial |
| A.8.29 | Security testing in development and acceptance | Unit + integration tests; conformance suites | Partial — Gap G-014 (no SAST / fuzzing) |
| A.8.30 | Outsourced development | — | Adopter-only |
| A.8.31 | Separation of development, test and production environments | — | Adopter-only |
| A.8.32 | Change management | Pull-request workflow; ADRs; migration tiers | Full |
| A.8.33 | Test information | Test data fixtures; conformance suite | Partial |
| A.8.34 | Protection of information systems during audit testing | RLS prevents test queries from leaking cross-tenant | Full |

### 15.3 EU DORA and EBA ICT risk-management guidelines

The Digital Operational Resilience Act (Regulation EU 2022/2554) imposes ICT risk-management requirements on financial entities and their critical ICT third-party service providers. The mapping below covers the directly framework-relevant articles.

#### Articles 5–16 — ICT risk-management framework

| Article | Subject | Framework contribution | Coverage |
| --- | --- | --- | --- |
| Art. 6 | ICT risk-management framework | ADRs as architecture documentation; threat surfaces enumerated in this document | Partial |
| Art. 8 | ICT risk identification | Trust boundaries enumerated (Section 3.5); known gaps documented (Addendum A) | Full |
| Art. 9 | Protection and prevention | Per-subject encryption; tenant isolation; append-only log; SAD rejection | Full |
| Art. 10 | Detection | OpenTelemetry traces + metrics; hash-chain verification (manual) | Partial — chain verification automation absent (Gap G-009) |
| Art. 11 | Response and recovery | Event log as recoverable truth; outbox at-least-once | Partial — runbooks absent (Gap G-017) |
| Art. 12 | Backup policies, restoration and recovery procedures | Per-tenant partitioning; per-file SQLite tenant isolation | Partial — backup tooling absent (Gap G-006) |
| Art. 13 | Learning and evolving | ADRs are immutable audit trail of design decisions | Full |
| Art. 14 | Communication | — | Adopter-only |
| Art. 15 | Further harmonisation | EBA RTS to follow | Future work |
| Art. 16 | Simplified framework for small entities | — | Out of scope |

#### Articles 17–23 — ICT-related incident management

| Article | Subject | Framework contribution | Coverage |
| --- | --- | --- | --- |
| Art. 17 | ICT-related incident management process | — (no incident process automation) | Adopter-only |
| Art. 18 | Classification of ICT-related incidents and cyber threats | — | Adopter-only |
| Art. 19 | Reporting of major ICT-related incidents to competent authorities | — | Adopter-only |

#### Articles 24–27 — Digital operational resilience testing

| Article | Subject | Framework contribution | Coverage |
| --- | --- | --- | --- |
| Art. 24 | General requirements for digital operational resilience testing | Conformance suites; integration tests under testcontainers | Partial |
| Art. 25 | Testing of ICT tools and systems | CI pipeline | Partial — no chaos testing (Gap G-019), no formal pen test (Gap G-012) |
| Art. 26 | Advanced testing of ICT tools, systems and processes — threat-led penetration testing (TLPT) | — | Adopter-only |
| Art. 27 | Requirements for testers | — | Adopter-only |

#### Articles 28–30 — ICT third-party risk

If the framework is consumed as a critical ICT service by a financial entity, the adopter's third-party risk-management process applies to the framework's open-source supply chain.

| Article | Subject | Framework contribution | Coverage |
| --- | --- | --- | --- |
| Art. 28 | General principles | Open-source MIT-licensed library; transparent governance | Partial |
| Art. 29 | Pre-contractual analysis | Public ADRs; public source; public test results | Partial |
| Art. 30 | Key contractual provisions | — (open-source library, no contract) | Adopter-only |

#### EBA / ESA expectations on cryptographic controls

The European Banking Authority's guidelines on ICT and security risk management (EBA/GL/2019/04) require:

- **Encryption of data at rest using approved algorithms** — addressed by AES-256-GCM in `shred/shred.go`.
- **Encryption key management with separation of duties** — addressed by the KMS-versus-database separation (Section 5.5).
- **Key rotation and revocation** — partially addressed by `RewrapDEK` + KEK versioning (mechanism present, operator tooling absent — Gap G-002).
- **Cryptographic erasure for data subject deletion** — addressed by crypto-shredding (Section 6.3).
- **Audit logging of access to sensitive data** — partially addressed (codegen flag set; logger not wired — Gap G-016).

---

## 16. Addendum A — Identified gaps

Each gap below carries an identifier (G-NNN), a short description, the affected controls, an assessment of risk if left unremediated, and the targeted roadmap phase. The roadmap phases are defined in Addendum B.

### G-001 — No vulnerability scanning in CI

**Description.** The CI pipeline does not run `govulncheck`, `gosec`, container-image scanners, or Software Bill of Materials generation.

**Affected controls.** ISO/IEC 27001:2022 A.8.8 (Management of technical vulnerabilities); DORA Art. 9 (Protection); SOC 2 CC9.2 (Vendor risk).

**Risk.** A vulnerable dependency may be consumed without detection. Adopters cannot consume an SBOM for their own third-party risk process. Industry-standard hygiene control absent.

**Roadmap phase.** Phase 1 (immediate).

### G-002 — No automated key rotation tooling

**Description.** `RewrapDEK` and `KEKRotator` exist as library functions, but no `esctl rotate-kek` command, no scheduled rotation worker, no automated retirement of old KEK versions.

**Affected controls.** ISO/IEC 27001:2022 A.8.24 (Use of cryptography); DORA Art. 9; EBA/GL/2019/04 § 3.4.4.

**Risk.** Adopters must hand-roll rotation. Rotation cadence drift; long-lived KEK versions; difficult-to-prove rotation history at audit.

**Roadmap phase.** Phase 2.

### G-003 — No DSAR export tool

**Description.** The framework provides the primitives (manifest, per-stream reads, decryption with shred-aware placeholders) but no turnkey `esctl export-subject` command.

**Affected controls.** SOC 2 P5 (Access); GDPR Article 15; ISO/IEC 27001:2022 A.5.34 (Privacy).

**Risk.** Adopters must build DSAR export themselves, with risk of inconsistent implementation. Operational burden during regulator-requested response windows.

**Roadmap phase.** Phase 2.

### G-004 — No tenant offboarding tool

**Description.** No single operation to perform "destroy this tenant's KEK and shred all subjects in one transaction". Adopter must script it.

**Affected controls.** ISO/IEC 27001:2022 A.5.11 (Return of assets), A.8.10 (Information deletion); GDPR Article 17.

**Risk.** Inconsistent offboarding; orphaned ciphertext; difficulty in proving complete erasure at audit.

**Roadmap phase.** Phase 2.

### G-005 — No chain-verification CLI

**Description.** `es.VerifyStreamChain` exists as a library function; no `esctl verify-stream-chain` command, no scheduled verification worker.

**Affected controls.** DORA Art. 10 (Detection); SOC 2 PI1.4.

**Risk.** Tamper detection requires custom integration. Verification cadence drift; integrity assurance not part of routine operations.

**Roadmap phase.** Phase 2.

### G-006 — No backup tooling or restore-test automation

**Description.** Backup strategy is documented as adopter responsibility. No backup wrapper, no restore-after-migration tests in CI, no documented runbook with RTO/RPO targets.

**Affected controls.** ISO/IEC 27001:2022 A.5.30 (ICT readiness for BC), A.8.13 (Backup); DORA Art. 12; SOC 2 A1.3.

**Risk.** Restore confidence is untested. Worst-case recovery time unknown.

**Roadmap phase.** Phase 3.

### G-007 — SQLite has no database-layer tenant isolation

**Description.** SQLite lacks Row-Level Security. The framework recommends one database file per tenant in production but does not enforce it; multi-tenant SQLite deployments depend entirely on application-layer correctness.

**Affected controls.** ISO/IEC 27001:2022 A.8.3 (Information access restriction); SOC 2 CC6.6.

**Risk.** A single bug in application code can leak across tenants in SQLite deployments. PostgreSQL deployments are not affected (RLS provides the second layer).

**Roadmap phase.** Phase 3 — proposed `SQLitePerTenantPool` helper that structurally enforces one pool per tenant.

### G-008 — No external witness or Merkle anchor runtime code

**Description.** Cookbook recipes 19 and 20 document the pattern; no runtime implementation ships.

**Affected controls.** DORA Art. 10; auditor-expected control for high-assurance integrity.

**Risk.** Adopters needing external integrity attestation (regulator demand or contractual obligation) must build it themselves.

**Roadmap phase.** Phase 3.

### G-009 — No automated chain verification scheduler

**Description.** Related to G-005. Even with a CLI command, periodic verification requires an external scheduler. No built-in worker.

**Affected controls.** DORA Art. 10; SOC 2 PI1.4.

**Risk.** Verification falls off the schedule; integrity bugs detected late.

**Roadmap phase.** Phase 3.

### G-010 — No incident-response runbook templates

**Description.** No template runbooks for common framework-specific incidents (corruption detected by chain verification, KEK unavailability, tenant cross-leakage suspicion, outbox backlog exhausting disk).

**Affected controls.** ISO/IEC 27001:2022 A.5.24, A.5.26; DORA Art. 11, Art. 17.

**Risk.** Adopters incur runbook authoring cost; response times vary across deployments.

**Roadmap phase.** Phase 3.

### G-011 — No formal threat model

**Description.** Trust boundaries are enumerated in this document; no STRIDE or LINDDUN threat-model document exists in the repository.

**Affected controls.** ISO/IEC 27001:2022 A.5.7 (Threat intelligence), A.8.26 (Application security requirements).

**Risk.** Threat coverage is implicit rather than enumerated; gaps may go undetected.

**Roadmap phase.** Phase 1.

### G-012 — No penetration testing

**Description.** No formal pen test has been conducted on the framework or a reference deployment.

**Affected controls.** DORA Art. 25, Art. 26; ISO/IEC 27001:2022 A.8.29.

**Risk.** Unknown vulnerabilities. Required by many regulator and customer audits.

**Roadmap phase.** Phase 3 (commission external firm).

### G-013 — No multi-region / data-residency support

**Description.** Framework's global-position design is store-wide monotonic; no multi-region replication adapter; no region tagging on events.

**Affected controls.** GDPR Chapter V; DORA Art. 9.

**Risk.** Adopters with EU-resident customer-data obligations must deploy per region. No code-level enforcement of residency.

**Roadmap phase.** Phase 4 (research) — may remain explicitly out of scope.

### G-014 — No security-focused static analysis or fuzzing

**Description.** No `gosec` configured; no `go-fuzz` or built-in fuzz harnesses on parsing surfaces (envelope decoding, classification parsing, Cedar policy evaluation).

**Affected controls.** ISO/IEC 27001:2022 A.8.29; SOC 2 PI1.1.

**Risk.** Parser bugs and unsafe-coding patterns undetected.

**Roadmap phase.** Phase 1 (gosec), Phase 2 (fuzz harnesses on critical parsers).

### G-015 — No vulnerability disclosure policy

**Description.** Repository carries no `SECURITY.md` documenting how to report vulnerabilities.

**Affected controls.** ISO/IEC 27001:2022 A.5.7; industry expectation.

**Risk.** Reporters have no clear channel; embargoed disclosure not possible.

**Roadmap phase.** Phase 1 (add `SECURITY.md` with a published address and embargo policy).

### G-016 — SENSITIVE-classification audit logger not wired

**Description.** Codegen sets the `audit_on_read = true` flag for SENSITIVE, CARDHOLDER, and CREDENTIAL classifications; the framework does not emit audit events on decrypt. Adopter must wire an interceptor.

**Affected controls.** GDPR Article 30; SOC 2 P8; ISO/IEC 27001:2022 A.5.34.

**Risk.** Access to special-category data not logged by default; adopters may forget to wire the interceptor.

**Roadmap phase.** Phase 2 (default-on audit logger via Shredder configuration option).

### G-017 — No transport-security wiring in the framework

**Description.** TLS, mutual TLS, bearer-token validation, security headers, rate limiting, and CORS are all adopter responsibilities (by design per ADR 0002).

**Affected controls.** SOC 2 CC6.7; ISO/IEC 27001:2022 A.5.14, A.8.20.

**Risk.** Adopters new to the framework may not implement these. Industry-standard hardening absent without explicit attention.

**Roadmap phase.** Phase 1 (publish a hardened HTTP-edge cookbook recipe with reference implementation).

### G-018 — No secrets-manager integrations

**Description.** Framework does not integrate Vault, AWS Secrets Manager, GCP Secret Manager, or Kubernetes Secrets API. KMS adapters expect a pre-constructed client.

**Affected controls.** ISO/IEC 27001:2022 A.8.24; DORA Art. 9.

**Risk.** Adopters integrate secrets handling independently — risk of inconsistent practice.

**Roadmap phase.** Phase 4 (optional convenience adapters; not blocking).

### G-019 — No chaos / fault-injection tests

**Description.** Test discipline is thorough on the happy path and on individually testable error paths. No chaos testing, no network-fault simulation, no deliberate KMS unavailability tests.

**Affected controls.** DORA Art. 25; SOC 2 A1.2.

**Risk.** Resilience under partial-failure conditions untested.

**Roadmap phase.** Phase 4.

### G-020 — No data retention enforcement

**Description.** Retention policy is adopter responsibility. The framework provides crypto-shredding as a primitive but does not enforce calendar-based retention.

**Affected controls.** GDPR Article 5(1)(e); ISO/IEC 27001:2022 A.5.34; SOC 2 P4.

**Risk.** Adopters may retain personal data longer than lawful basis permits. No code-level enforcement.

**Roadmap phase.** Phase 2 (retention worker scaffold + cookbook recipe).

---

## 17. Addendum B — Remediation roadmap

The roadmap is organised into four phases, each scoped to a calendar quarter at typical staffing. Phases are sequential — Phase 1 should be in place before regulated production launch; Phase 2 should be in place before SOC 2 Type II audit; Phase 3 should be in place before DORA Article 26 threat-led penetration testing; Phase 4 covers stretch items.

### Phase 1 — Foundational hygiene (target: Q3 2026)

Pre-requisite to regulated production launch. All items are low-effort, high-leverage controls that adopters and auditors will look for first.

| Gap | Deliverable | Effort | Owner |
| --- | --- | --- | --- |
| G-001 | Add `govulncheck` to CI workflow | 0.5d | Framework |
| G-001 | Add `gosec` to CI workflow (with baseline exclusions documented) | 1d | Framework |
| G-001 | Add SBOM generation per release (`cyclonedx-gomod` → CycloneDX 1.5 JSON, attached to GitHub release) | 1d | Framework |
| G-001 | Configure Dependabot or Renovate for dependency updates | 0.5d | Framework |
| G-011 | Author formal threat model document (`docs/security/threat-model.md`) using STRIDE per trust boundary | 3d | Framework |
| G-014 | `gosec` integration above | shared | Framework |
| G-015 | Add `SECURITY.md` with vulnerability disclosure policy, contact email, embargo terms, hall-of-fame | 0.5d | Framework |
| G-017 | Publish hardened HTTP-edge cookbook recipe with reference TLS/mTLS/bearer/CORS/rate-limit implementation | 3d | Framework |

**Exit criterion:** CI pipeline shows clean `govulncheck`, `gosec`, SBOM artefact in latest release, `SECURITY.md` in repository root, threat-model document published.

### Phase 2 — Compliance-enabling tooling (target: Q4 2026)

Pre-requisite to SOC 2 Type II audit. Each item replaces an adopter-must-build pattern with framework-provided tooling, reducing the surface where regulated adopters reinvent control implementations.

| Gap | Deliverable | Effort | Owner |
| --- | --- | --- | --- |
| G-002 | `esctl rotate-kek --tenant T` command wiring `KEKRotator` + scheduled-rotation worker template | 5d | Framework |
| G-003 | `esctl export-subject --tenant T --subject S --format json` command honouring `pii_manifest.json` filter and shred placeholders | 5d | Framework |
| G-004 | `esctl offboard-tenant --tenant T --confirm` command performing KEK destruction + bulk subject shred in one operation | 3d | Framework |
| G-005 | `esctl verify-stream-chain --tenant T --stream S` command and `--all` variant | 2d | Framework |
| G-016 | Default-on audit logger for `SENSITIVE`/`CARDHOLDER`/`CREDENTIAL` decrypts (Shredder configuration option, structured slog emit, opt-out for adopters who wire their own) | 3d | Framework |
| G-020 | Retention worker scaffold + cookbook recipe (Tier 3 projection that calls Shredder on subjects matching age + classification criteria) | 5d | Framework |
| G-014 | Fuzz harnesses on envelope decoding and classification parsing (Go 1.18 native fuzz, run in CI on a time budget) | 3d | Framework |

**Exit criterion:** All compliance operations performable via `esctl`; audit emit default-on; retention pattern documented and tested.

### Phase 3 — Operational maturity (target: Q1 2027)

Pre-requisite to DORA Article 26 threat-led penetration testing. Addresses operational resilience, incident response, and assurance controls.

| Gap | Deliverable | Effort | Owner |
| --- | --- | --- | --- |
| G-006 | Backup runbook + restore-after-migration conformance test in CI; documented RTO/RPO target for the reference profile | 8d | Framework + ops |
| G-007 | `SQLitePerTenantPool` helper that opens and caches one connection pool per tenant from a single configuration; deprecation warning on multi-tenant single-file deployments in production builds | 5d | Framework |
| G-008 | Reference Merkle-anchor implementation (Tier 3 projection that batches event hashes into a Merkle tree, exposes the root via projection state, publishes to an external append-only target) | 8d | Framework |
| G-009 | Chain-verification scheduled worker (`schedule.Worker` integration that runs `VerifyStreamChain` on a tenant-and-stream cadence) | 3d | Framework |
| G-010 | Incident-response runbook templates for: chain integrity violation, KEK unavailability, suspected tenant leakage, outbox backlog | 5d | Framework |
| G-012 | Commission external pen-test of framework + reference deployment | 30d wall (10d effort) | External |

**Exit criterion:** Restore tested in CI; SQLite per-tenant enforcement available; Merkle-anchor reference shipping; chain verification scheduled; runbook templates published; pen-test report received and findings remediated.

### Phase 4 — Stretch (target: Q2 2027 and beyond)

Items that the framework may or may not absorb, depending on adopter demand and on the framework's evolving scope decisions.

| Gap | Deliverable | Effort | Notes |
| --- | --- | --- | --- |
| G-013 | Multi-region replication adapter (one-way replica for read scale; explicit anti-pattern guidance for primary-active multi-region) | 30d | May remain explicitly out of scope per ADR 0009 |
| G-018 | Secrets-manager convenience adapters (Vault, AWS Secrets Manager, GCP Secret Manager) | 5d each | Optional |
| G-019 | Chaos / fault-injection test suite using `toxiproxy` against the testcontainers PostgreSQL instance | 8d | Optional |
| — | SOC 2 Type II readiness assessment by external auditor | 15d wall | External |
| — | ISO/IEC 27001:2022 stage 1 audit | 15d wall | External |
| — | Property-based testing harness for critical invariants (chain hash determinism, global position monotonicity, RLS isolation under fuzzed queries) | 10d | Optional |

### Roadmap sequencing dependencies

- G-002, G-003, G-004, G-005, G-016 (Phase 2) all extend `esctl`. They should share a `cmd/esctl/commands/admin/` package introduced in the first of them; subsequent items reuse the structure.
- G-006 (backup runbook + restore test) depends on the testcontainer harness already present; no architectural dependency.
- G-007 (SQLite per-tenant pool) depends on understanding how adopters currently configure SQLite — survey adopters before designing the interface.
- G-008 (Merkle anchor) depends on a generic projection scaffold but no new framework primitive.
- G-012 (pen test) should occur after Phase 1 and Phase 2 are complete, so the test exercises the hardened surface.

### Update cadence for this document

This document should be reviewed and re-issued:

- **At each phase boundary** above — to reflect the gaps closed.
- **At each ADR addition** that affects a control mapping — to update Section 15.
- **At each material change to dependencies** that affects the supply-chain section.
- **Annually**, regardless of triggering events, to reflect regulatory landscape changes (DORA RTS publication, ISO 27001 revisions, SOC 2 TSC updates).

---

## 18. Annex — Glossary, references, and verification notes

### 18.1 Glossary

| Term | Definition |
| --- | --- |
| **ADR** | Architecture Decision Record — immutable record of a load-bearing design decision. |
| **Aggregate** | Event-sourced entity defined by a decider function `(state, command) → events`. |
| **AES-GCM** | Advanced Encryption Standard in Galois/Counter Mode — authenticated encryption with associated data (AEAD). |
| **Crypto-shredding** | Erasure of personal data by destruction of the key used to encrypt it. |
| **DEK** | Data Encryption Key — the key used to encrypt event-payload fields, per-subject in this framework. |
| **DSAR** | Data Subject Access Request — a request under GDPR Article 15 for a copy of personal data held about a subject. |
| **Decider** | Pure function pattern: `Initial`, `Decide(state, command) → events`, `Evolve(state, event) → state`. |
| **KEK** | Key Encryption Key — the key used to wrap DEKs, per-tenant in this framework. |
| **KMS** | Key Management Service — external system holding KEKs (AWS KMS, GCP KMS, Vault, etc.). |
| **PCI DSS** | Payment Card Industry Data Security Standard. |
| **PII** | Personally Identifiable Information; in GDPR terms, personal data. |
| **RLS** | Row-Level Security — PostgreSQL feature enforcing per-row access predicates. |
| **SAD** | Sensitive Authentication Data — PCI DSS classification covering card-not-present authentication data; never persistable post-authorisation. |
| **TSC** | Trust Services Criteria — the SOC 2 control framework. |

### 18.2 Authoritative references

- **Framework**: `github.com/laenenai/eventstore`. All file paths in this document are relative to that repository root.
- **ADRs**: `docs/adr/0001-*` through `docs/adr/0032-*`. Index at `docs/adr/README.md`.
- **Cookbook**: `docs/cookbook/01-*` through `docs/cookbook/22-*`.
- **Architecture overview**: `docs/architecture/overview.md`.
- **Project conventions**: `CLAUDE.md` at repository root; user's global standards at `~/.claude/CLAUDE.md`.

### 18.3 Regulatory references

- **SOC 2** — AICPA Trust Services Criteria 2017 (with 2022 points-of-focus revisions).
- **ISO/IEC 27001:2022** — Information security management systems — Requirements (October 2022 edition).
- **EU DORA** — Regulation (EU) 2022/2554 on digital operational resilience for the financial sector.
- **EBA guidelines** — EBA/GL/2019/04 on ICT and security risk management.
- **GDPR** — Regulation (EU) 2016/679.
- **PCI DSS** — Payment Card Industry Data Security Standard v4.0 (March 2022).

### 18.4 Verification notes for the auditor

Every concrete claim in Sections 3 through 14 is traceable to a file path and (where load-bearing) a line number. The auditor may verify by:

1. **Clone the repository at the tag corresponding to this document's date.**
2. **Read the cited files at the cited line numbers.** Line numbers refer to the state of the repository at the document date; line numbers may drift with subsequent commits.
3. **Run the framework's test suite** with `task test`. The CI pipeline produces equivalent output for any commit.
4. **Read the cited ADRs.** ADRs are immutable once Accepted, so historical claims remain verifiable.
5. **For PostgreSQL claims**, instantiate the framework's test container and inspect the schema, policies, and roles created by the migrations.
6. **For RLS-specific claims**, run the `rls_test.go` suite under `adapters/storage/postgres/`, which covers: app pool isolation, admin pool bypass, ramp-option fallback, ErrAdminPoolRequired surface, no-leakage across transactions.

For any claim where verification fails or where the cited code does not support the claim, the framework maintainer should be notified — the document is intended as truthful self-assessment, and a discrepancy is a defect.

### 18.5 Document maintenance

This document is maintained at `docs/compliance/regulator-handover.md` in the framework repository. Version history is in the repository commit log. The document should be re-issued at the cadence described in Section 17. Material discrepancies between the document and the code should be raised as repository issues tagged `compliance:document-drift`.

---

*End of document.*
