# 18: Periodic Merkle Anchors (L2 tamper-evidence)

ADR 0028 ships an L1 per-stream hash chain in the framework. L1
detects **in-place mutation** of stored events. It does **not** detect
**deletion of trailing events** from a stream — an attacker who
truncates the last N rows leaves the remaining chain self-consistent.

This recipe shows how to bolt periodic **Merkle anchors** onto the
L1 chain to detect truncation, while staying inside the same
database. It's the natural step up when in-place tampering is
covered but you also need "this stream had at least N events at
time T" assurance.

For full end-to-end tamper-evidence against a hostile DBA (someone
who can edit both `events` AND the anchor table), pair this with
[recipe 20](./20-external-witness.md).

## What it shows

- An `events_anchor` audit table with one row per (tenant, stream,
  up_to_version) checkpoint.
- A scheduled job that, on each tick:
  1. Reads each stream's events whose version is greater than the
     last anchored version.
  2. Computes a SHA-256 Merkle root over those events' `hash`
     values.
  3. Writes one anchor row `(tenant, stream, up_to_version,
     root, anchored_at)`.
- A verification helper that takes any anchor row and proves the
  chain reached at least `up_to_version` by replaying.

## What it deliberately does NOT show

- **External signing of anchors.** That's recipe 20 — Sigstore Rekor,
  internal append-only ledger, blockchain. Without external witness,
  a DBA who edits anchors *and* events can still hide tampering.
- **Continuous Merkle accumulator on every Append.** Doable, but
  costs a write per event and a separate index per stream. Periodic
  batch anchoring is cheaper and almost always sufficient.
- **Tree-of-trees across tenants.** Each anchor scopes to one
  `(tenant, stream)` and is independent. A cross-tenant root requires
  coordination that the framework doesn't try to provide.
- **Anchor compaction / pruning.** Anchors are append-only and small;
  keep them forever. If retention pressure ever forces pruning, keep
  anchors at exponentially-spaced versions (last 100, then every 100,
  then every 10k…).

## The schema

A separate table, sibling to `events`. Postgres shape:

```sql
CREATE TABLE events_anchor (
    tenant_id        TEXT      NOT NULL,
    stream_id        TEXT      NOT NULL,
    up_to_version    BIGINT    NOT NULL,
    root             BYTEA     NOT NULL,  -- 32-byte Merkle root
    anchored_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, stream_id, up_to_version)
);

CREATE INDEX events_anchor_by_tenant_time_idx
    ON events_anchor (tenant_id, anchored_at);
```

SQLite is the same with `BLOB` instead of `BYTEA` and `TEXT` for the
timestamp (ISO 8601, per the SQLite conventions used elsewhere in
this codebase).

The anchor row is small (~80 bytes including PK overhead). One
anchor per stream per tick is fine for thousands of active streams.

## The Merkle root

Use a standard binary Merkle tree over the leaf set
`[hash(event_1), hash(event_2), …, hash(event_N)]`. Each internal
node = `SHA-256(left || right)`; odd-leaf rounds duplicate the last
leaf (RFC 6962 convention).

```go
package merkle

import "crypto/sha256"

// Root returns the Merkle root over leaves. Leaves are taken as-is
// (no domain separation — the event hashes are already SHA-256 of
// the canonical envelope). Returns ZeroHash for an empty slice.
func Root(leaves [][]byte) []byte {
    if len(leaves) == 0 {
        zero := make([]byte, sha256.Size)
        return zero
    }
    level := leaves
    for len(level) > 1 {
        if len(level)%2 == 1 {
            level = append(level, level[len(level)-1])
        }
        next := make([][]byte, 0, len(level)/2)
        for i := 0; i < len(level); i += 2 {
            h := sha256.New()
            h.Write(level[i])
            h.Write(level[i+1])
            next = append(next, h.Sum(nil))
        }
        level = next
    }
    return level[0]
}
```

## The anchoring job

A subscriber pattern or a plain scheduled job. The simplest version
runs on every tenant + stream pair every N minutes:

```go
type Anchorer struct {
    Store    es.StreamReader
    DB       *sql.DB             // direct access for the audit-table insert
    Cadence  time.Duration       // e.g. 5 * time.Minute
}

func (a *Anchorer) Tick(ctx context.Context) error {
    // Walk every (tenant, stream) pair with new events.
    // For simplicity here we accept a list; real deployments either
    // join state_cache for "active streams" or maintain a
    // streams-with-changes index per tick.
    rows, err := a.DB.QueryContext(ctx, `
        SELECT DISTINCT tenant_id, stream_id
        FROM events
        WHERE recorded_at > (
            SELECT COALESCE(MAX(anchored_at), '1970-01-01')
            FROM events_anchor
            WHERE events_anchor.tenant_id = events.tenant_id
              AND events_anchor.stream_id = events.stream_id
        )`)
    if err != nil { return err }
    defer rows.Close()

    for rows.Next() {
        var tenantID, streamID string
        if err := rows.Scan(&tenantID, &streamID); err != nil {
            return err
        }
        sid, err := es.ParseCanonical(tenantID, streamID)
        if err != nil { continue }

        // Read everything past the last anchor.
        lastVer, err := a.lastAnchoredVersion(ctx, tenantID, streamID)
        if err != nil { return err }

        envs, err := a.Store.ReadStream(ctx, sid, lastVer)
        if err != nil { return err }
        if len(envs) == 0 { continue }

        leaves := make([][]byte, len(envs))
        for i := range envs {
            leaves[i] = envs[i].Hash
        }
        root := merkle.Root(leaves)
        upTo := envs[len(envs)-1].Version

        if _, err := a.DB.ExecContext(ctx, `
            INSERT INTO events_anchor (tenant_id, stream_id, up_to_version, root)
            VALUES ($1, $2, $3, $4)`,
            tenantID, streamID, upTo, root); err != nil {
            return err
        }
    }
    return rows.Err()
}
```

Run this from your scheduler of choice — `cmdworkflow.Workflow` async
subscriber, a Restate timer (cookbook recipe 4), a cron job, or the
DBOS scheduler.

## Verification

Two checks. Run them together for any "is this stream intact?"
audit:

1. **`es.VerifyStreamChain` over the full stream** (from L1) —
   detects in-place mutation. Same as without anchors.
2. **Reconstruct the Merkle root** from the current events at
   each anchored `up_to_version` and compare to the stored `root`.
   If the stream has fewer events than the anchor's `up_to_version`,
   or the recomputed root differs, the chain has been **truncated
   or mutated** since the anchor was written.

```go
func VerifyAgainstAnchor(ctx context.Context, store es.StreamReader,
    sid es.StreamID, upToVersion uint64, expectedRoot []byte,
) error {
    envs, err := store.ReadStream(ctx, sid, 0)
    if err != nil { return err }
    if uint64(len(envs)) < upToVersion {
        return fmt.Errorf("stream truncated: have %d events, anchor says %d",
            len(envs), upToVersion)
    }
    leaves := make([][]byte, upToVersion)
    for i := uint64(0); i < upToVersion; i++ {
        leaves[i] = envs[i].Hash
    }
    if !bytes.Equal(merkle.Root(leaves), expectedRoot) {
        return errors.New("anchor mismatch — chain mutated since anchor")
    }
    return nil
}
```

## Failure modes

| Failure                                           | What happens / what to do                                                                                  |
| -------------------------------------------------- | ---------------------------------------------------------------------------------------------------------- |
| Anchoring job stalls (down, paused)                | No new anchors written; existing anchors remain valid. Verify warns "no anchor in last N hours" via metric. |
| Race: events appended between read and anchor insert | The next tick picks them up. Anchors are monotonically advancing `up_to_version`; no rollback needed.       |
| Concurrent anchor inserts for same `up_to_version` | PK conflict, second insert ignored. Idempotent.                                                            |
| Hostile DBA edits both `events` and `events_anchor` | **Not detected.** L2 alone trusts the database; for this threat use recipe 20 (external witness).          |
| Stream truncated by N events                       | Next verify against any anchor with `up_to_version > new_length` fails immediately.                        |
| Anchor table itself dropped                        | Reduces to L1 — chain still verifies, truncation undetectable.                                             |

## When NOT to use this recipe

- **No truncation threat in your model.** If the only adversary is
  application bugs and casual operator mistakes, L1 alone is enough.
- **Workflow already publishes events to an external system** (Kafka,
  another event store). The external system *is* your truncation
  witness; rebuilding Merkle anchors duplicates that work.
- **Stream count is in the millions and changes per second.** The
  anchoring job becomes the bottleneck. Either anchor only
  high-value streams (selected via a `requires_anchor` flag column
  on `state_cache`) or move to recipe 20 with a transparency-log
  service that handles fan-in.

## Reference

- [ADR 0028](../adr/0028-tamper-evident-chain.md) — the L1 chain
  that L2 anchors stand on top of.
- [Recipe 19 — L3 external witness](./20-external-witness.md) —
  the next step up when in-DB anchors aren't enough.
- RFC 6962 — Certificate Transparency Merkle tree construction
  (the reference implementation for binary Merkle trees over
  append-only logs).
