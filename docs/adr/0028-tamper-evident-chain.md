# ADR 0028: Tamper-Evident Hash Chain

- **Status:** Accepted
- **Date:** 2026-05-15
- **Pairs with:** ADR 0005 (event envelope schema), ADR 0008 (stream
  identity), ADR 0010 (crypto-shredding), ADR 0013 (schema evolution).

## Context

Every event in the store is, at rest, just a row in a table. A
malicious operator with write access can mutate `payload`, swap
`occurred_at`, or rewrite `actor.principal` and leave no evidence.
Regulated workloads (finance, healthcare, legal-discovery, audit log
mirroring) require detection of such mutation; the framework as
shipped offers only the AEAD tag check on shredded fields (ADR 0010
§ tamper detection), which protects encrypted bytes only.

Three properties are commonly bundled as "tamper-evident":

1. **Mutation detection** — any in-place edit of a stored event is
   detectable by an auditor with read access.
2. **Truncation detection** — deletion of trailing events from a
   stream is detectable.
3. **Existence proof** — proof that a given chain state existed at a
   given wall-clock time, surviving a DBA who edits both events and
   any in-database audit table.

(1) is achievable as a framework primitive at near-zero runtime cost.
(2) and (3) require operational machinery (periodic anchoring, an
external append-only log) whose cadence, format, and trust roots are
inherently application-specific — wrong axes to bake into a
framework.

## Decision

### 1. Per-stream hash chain in the framework (L1)

Every stored event carries a SHA-256 `hash` computed at append time:

```
hash[v] = SHA-256(
    prev_hash[v]                              // 32 bytes; zero for v=1
    || canonical(envelope[v] minus hash, prev_hash)
)
```

The chain is scoped to `(tenant_id, stream_id)`. Each event's
`prev_hash` is the previous version's `hash` from the same stream;
the genesis event (version 1) uses 32 zero bytes.

The chain is **not** global. Cross-stream ordering is established at
the database level by `global_position` (ADR 0009), which is assigned
inside the append transaction and is sufficient for replay ordering.
A global hash chain would require single-writer serialization across
all streams — incompatible with concurrent appenders.

### 2. Canonical serialization

The hash input is `proto.MarshalOptions{Deterministic: true}` over an
envelope where the `hash` and `prev_hash` fields are cleared. Proto
deterministic marshal is well-defined: field-tag-sorted, no
unspecified-default omission, stable across `google.golang.org/protobuf`
versions per the documented contract.

Using the same wire format the events table already stores (the
`payload` bytes are themselves deterministically marshaled by the
typed codecs from ADR 0004) keeps the hash input stable under any
read-time transformation (upcasters, decryption).

### 3. Hash subset is locked at envelope schema v1

The set of envelope fields included in the hash input is pinned. New
envelope fields added in future versions (correlation tags, indexing
hints) are **excluded** from the hash unless a `hash_version` byte is
introduced and the algorithm explicitly upgraded. This keeps existing
hashes verifiable forever; the cost is that new fields don't get
chain protection until an explicit `hash_version=2` rollover.

The framework currently emits `hash_version` only implicitly (v1). If
a future ADR introduces v2, the column moves from implicit-v1 to
explicit storage.

### 4. Computed inside the storage adapter, not the aggregate runtime

Hash computation lives in the SQL append path of each adapter
(`adapters/storage/{postgres,sqlite}.Append`). Two reasons:

- The previous hash must be read inside the same transaction as the
  append, with the same locking that OCC uses (the `(tenant_id,
  stream_id, version)` primary key). Lifting this to the aggregate
  runtime would either duplicate the read or risk a race.
- Multi-event appends (one command → N events) chain within the same
  transaction; the adapter sees the full batch and can compute each
  event's `prev_hash` from the previous event in the batch without
  re-reading.

The aggregate runtime does not see hashes during Decide/Evolve. It
receives them on `Load` via the standard envelope, so subscribers and
projections can use them if they want.

### 5. Verification is opt-in, not append-time

`Append` does not verify the chain it's extending — it trusts that
the predecessor row's hash is whatever the table says. Verifying on
every append would mean reading the full chain on every command,
turning O(events) writes into O(events²) total work. Auditors run
`es.VerifyStreamChain(ctx, store, sid)` on the streams they care
about, on the cadence they choose.

### 6. Interaction with crypto-shredding

`payload` and `encryption_key_refs` are included in the hash as their
**stored bytes**. Post-`ForgetSubject`, the encrypted payload bytes
remain at rest (only the DEK is destroyed) — the hash chain stays
valid, even though the plaintext is irrecoverable. This is the right
behavior: shredding removes data, not the audit trail of *that an
event existed*.

### 7. Interaction with upcasters

Upcasters (ADR 0013) transform the *decoded* event at read time. They
do not rewrite the stored bytes. The hash, computed over the stored
bytes at append time, is therefore unaffected by a `schema_version`
bump. A schema-mismatch deployment that upcasts on read can still
verify the chain against the originally appended bytes.

### 8. What's deliberately NOT in the framework

- **L2 — periodic Merkle anchors in a separate audit table.** Cadence,
  table layout, indexing, and retention vary per workload. Lives as
  cookbook recipe 19.
- **L3 — external witness anchoring** (Sigstore Rekor, internal
  append-only ledger, blockchain). Trust roots and protocol vary per
  org. Lives as cookbook recipe 20.
- **Per-event digital signatures.** A different threat model
  (nonrepudiation against the writer, not detection by the auditor).
  Worth a separate ADR if a consumer needs it.

## Alternatives considered

**Hash chain stored in a separate table.** Considered: keep
`events.hash` out of the envelope, put it in an `events_chain`
sibling table joined on `(tenant_id, stream_id, version)`. Rejected
because every read of the envelope-with-hash would require a join,
and subscribers receiving envelopes downstream would have no chain
visibility. Storing the hash on the envelope row is one extra column
that costs nothing on reads that don't use it.

**Compute hash in the aggregate runtime, write through.** Rejected as
described in decision 4 — predecessor read and chain extension must
be in the same transaction as the append.

**JCS-canonical JSON instead of proto deterministic marshal.** RFC
8785 (JSON Canonicalization Scheme) gives stronger
cross-implementation stability but doubles the hash input size and
costs CPU. Proto deterministic is sufficient for a single-language
runtime (Go-only consumers verify against Go-only producers).

**Global single chain across all streams.** Rejected per decision 1.

**Hash includes recorded_at and global_position.** Both are assigned
at commit by the database, so they're not knowable until inside the
transaction. Including them is technically possible but couples the
hash to commit-side fields and complicates verification (auditor must
trust the same DB clock). Excluding them keeps the hash a property of
the *applied event*, not its DB-side metadata.

## Consequences

- Every event grows by 64 bytes on disk (32 hash + 32 prev_hash).
  Negligible relative to typical event payload size; one B-tree index
  level at most on the events table.
- Append latency: one extra SQL `SELECT hash FROM events WHERE
  tenant_id=$1 AND stream_id=$2 ORDER BY version DESC LIMIT 1` per
  append, inside the existing transaction. For streams written by
  the same connection back-to-back, the OS page cache covers this.
  For cold streams, one extra page read.
- Verification is O(stream length) and `Read`-bound; trivially
  parallelizable across streams.
- The hash subset of envelope fields is now part of the framework's
  public contract. Bumping `hash_version` is a coordinated change
  (new ADR, new column, migration).
- Subscribers receive envelopes with `hash` populated. Downstream
  projections that mirror events into external systems can replay
  the hash for end-to-end attestation — enabling recipe 20 (external
  witness) without further framework changes.
- Crypto-shredding stays unchanged. The chain continues to verify
  after a shred; auditors learn "an event existed here" without
  learning what it said.
