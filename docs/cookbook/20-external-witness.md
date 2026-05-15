# 20: External Witness Anchoring (L3 tamper-evidence)

L1 (ADR 0028's per-stream hash chain) detects in-place mutation.
L2 ([recipe 19](./19-merkle-anchors.md)) extends that to detect
truncation by storing Merkle roots in an audit table inside the
same database.

**Both L1 and L2 trust the database.** A hostile DBA — or anyone who
gains write access — can mutate `events`, mutate `events_anchor`,
and leave a self-consistent forgery. L3 closes that gap by
publishing the Merkle roots to an **external append-only log**
controlled by a different trust root, so a tamper attempt requires
also tampering with the external log (typically impossible).

This recipe is **operator-shaped**, not framework-shaped: the trust
root, log shape, and witness protocol vary per organization and
regulatory regime. The patterns below are the three that show up
most often.

## What it shows

Three concrete patterns, in order of increasing strength and
operational cost:

| Witness                                | Trust root                                | When to use                              |
| -------------------------------------- | ----------------------------------------- | ---------------------------------------- |
| **Internal append-only ledger**        | A separate service / database owned by a different team (audit, security, compliance). Often write-only via API. | Mid-trust environments. The "different team has the keys" model. |
| **Sigstore Rekor / public transparency log** | The Sigstore project, signed by a multi-party ceremony; tamper requires colluding with multiple operators. | Compliance with supply-chain attestation standards (SLSA, in-toto). |
| **Public blockchain (Bitcoin / Ethereum)** | Proof-of-work / proof-of-stake economics. Tamper requires rewriting public history. | Regulator-facing "this existed at time T" claims; legal-discovery-grade evidence. |

In every pattern the framework's contribution is the same: publish
the L2 Merkle root (from recipe 19) to the witness, store the
witness's receipt back in the audit table.

## What it deliberately does NOT show

- **Real-time anchoring.** External witnesses are batched —
  Rekor accepts each request, but public chains are too expensive
  per-event. Anchor on a cadence (every N minutes / hourly /
  daily). The batch lag is the tamper window for L3.
- **Per-event signatures.** Different threat model entirely
  (nonrepudiation against the writer, not detection by the
  auditor). Would be a separate ADR if pursued.
- **A specific blockchain integration.** Too many options
  (Bitcoin via OpenTimestamps, Ethereum via OPENTIMESTAMP /
  custom contract, Cosmos / private chains, etc.). Pick what
  your compliance team accepts; the framework stays neutral.

## Pattern 1 — Internal append-only ledger

The simplest L3. A separate service writes anchors to its own
storage, governed by a different team. Example shape:

```go
type LedgerWitness struct {
    HTTPClient *http.Client
    URL        string // POST endpoint of the ledger service
    APIKey     string // your service's identity for write-attestation
}

type AnchorReceipt struct {
    LedgerID    string    // ledger's own id for this attestation
    AnchoredAt  time.Time // ledger's timestamp (NOT our anchored_at)
    Signature   []byte    // ledger's signature over root + LedgerID + AnchoredAt
}

func (w *LedgerWitness) Witness(ctx context.Context, anchor Anchor) (AnchorReceipt, error) {
    body, _ := json.Marshal(map[string]any{
        "tenant_id":      anchor.TenantID,
        "stream_id":      anchor.StreamID,
        "up_to_version":  anchor.UpToVersion,
        "root":           hex.EncodeToString(anchor.Root),
        "submitted_at":   time.Now().UTC(),
    })
    req, _ := http.NewRequestWithContext(ctx, "POST", w.URL,
        bytes.NewReader(body))
    req.Header.Set("Authorization", "Bearer "+w.APIKey)
    req.Header.Set("Content-Type", "application/json")
    resp, err := w.HTTPClient.Do(req)
    if err != nil { return AnchorReceipt{}, err }
    defer resp.Body.Close()
    if resp.StatusCode != http.StatusCreated {
        return AnchorReceipt{}, fmt.Errorf("witness: %s", resp.Status)
    }
    var rec AnchorReceipt
    if err := json.NewDecoder(resp.Body).Decode(&rec); err != nil {
        return AnchorReceipt{}, err
    }
    return rec, nil
}
```

Extend the `events_anchor` table from recipe 19 with the receipt:

```sql
ALTER TABLE events_anchor ADD COLUMN ledger_id    TEXT;
ALTER TABLE events_anchor ADD COLUMN witnessed_at TIMESTAMPTZ;
ALTER TABLE events_anchor ADD COLUMN witness_sig  BYTEA;
```

The L2 anchoring job, after the local insert, calls `Witness(...)`
and updates these columns. **Failure to witness should not fail the
anchor** — keep the local row, retry the witness call separately.

Verification (operator workflow):
1. For each `events_anchor` row, fetch the ledger entry by
   `ledger_id`.
2. Verify the ledger's signature over `(root, up_to_version,
   witnessed_at)`.
3. Compare the local `root` to the ledger's `root`.
4. Replay the chain and rebuild the Merkle root locally; compare
   to both.

## Pattern 2 — Sigstore Rekor

Rekor is a public, append-only transparency log signed by the
Sigstore community. Append is free; queries are free; integrity is
guaranteed by the log's own consistency proofs.

```go
import "github.com/sigstore/rekor/pkg/client"
import "github.com/sigstore/rekor/pkg/generated/client/entries"
import "github.com/sigstore/rekor/pkg/types/hashedrekord"

// One witness call per anchor.
func witnessToRekor(ctx context.Context, root []byte, hint string) (logEntry, error) {
    rk, _ := client.GetRekorClient("https://rekor.sigstore.dev")
    // Submit the Merkle root as a HashedRekord entry with our
    // service's signing key as the attestation identity.
    entry, err := rk.Entries.CreateLogEntry(...)
    if err != nil { return logEntry{}, err }
    return entry, nil
}
```

Rekor returns a log index and an inclusion proof. Store both in
`events_anchor`:

```sql
ALTER TABLE events_anchor ADD COLUMN rekor_log_index BIGINT;
ALTER TABLE events_anchor ADD COLUMN rekor_proof     BYTEA;
```

Verifying later: fetch the entry from Rekor by log index, verify
inclusion against the log's signed tree head, then verify the
Merkle root matches your recomputed local root.

**Note** Rekor entries are public. If your Merkle roots reveal
information (they don't — they're SHA-256 hashes of envelope hashes),
no leak risk. But the *cadence* and *count* of anchors per stream
are observable. If that's sensitive metadata, use pattern 1
(internal ledger) instead.

## Pattern 3 — Public blockchain

The strongest witness but the slowest and most expensive. Used in
regulated industries where "this state existed at this block height"
is a legally-meaningful claim.

The simplest path is **OpenTimestamps** — a free service that
aggregates submissions and writes a single Bitcoin OP_RETURN per
batch (typically hourly). You get a Bitcoin-anchored proof for
each submission, verifiable independently against the Bitcoin
chain.

```go
import "github.com/opentimestamps/javascript-opentimestamps" // (or equivalent Go client)

func witnessToOTS(ctx context.Context, root []byte) (otsProof []byte, err error) {
    // POST root to a calendar server; receive a partial proof
    // immediately, upgraded to a full Bitcoin proof when the next
    // batch is anchored (within ~1 hour).
    return otsClient.Stamp(ctx, root)
}
```

Store the partial proof immediately; run an upgrader job that
checks for the full Bitcoin attestation hourly and replaces the
partial proof. Operators verify by feeding the proof to any
OpenTimestamps verifier — no trust in your service required.

## Failure modes

| Failure                                                | What happens / what to do                                                                                                                              |
| ------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Witness service down                                   | Local anchor still committed. Retry witness on the next anchoring tick. Verification flags rows missing a witness as "not yet attested."                |
| Witness signature can't be verified later              | Either the witness was compromised at signing time, or the verifying party has the wrong public key. Investigate; until resolved, treat as L2-only.    |
| Rekor returns inconsistent inclusion proof             | Either Rekor has misbehaved (newsworthy) or a man-in-the-middle is forging proofs. Fail loud.                                                          |
| Bitcoin block reorganization                           | OpenTimestamps proofs include the block height; reorg moves it. Re-upgrade the proof against the new chain head.                                       |
| Anchor lag exceeds compliance window                   | Add monitoring: alert if `(now - witnessed_at) > X`. The L3 tamper window equals your anchoring cadence.                                                |
| Hostile DBA edits both `events` + `events_anchor`      | **Detected** — local anchor doesn't match the witnessed copy. The witness's copy is canonical; auditor compares local recompute against the witness.   |
| Hostile DBA edits `events`, `events_anchor`, AND your witness service's storage | If the witness service is internal-with-the-same-DBA (pattern 1, no key separation), still possible. Move to pattern 2 / 3 for cross-trust-root assurance. |

## When NOT to use this recipe

- **No external auditor or regulator in your threat model.** If
  the only consumer of tamper-evidence is your own engineering org,
  L2 (in-DB Merkle anchors) is enough. L3 buys cross-trust-root
  assurance you might not need.
- **Latency budget is sub-minute.** External witnesses run on
  minutes-to-hours cadence. If your audit story requires "tamper
  detectable within seconds," you need a different approach
  (e.g., per-event signatures + a streaming witness; outside the
  framework's scope).
- **Privacy regulation forbids publishing hashes externally.** Some
  privacy regimes treat even cryptographic hashes as potentially
  identifying (rainbow tables over known small populations). For
  these workloads use pattern 1 with a contracted internal
  witness, not patterns 2 or 3.

## Reference

- [ADR 0028](../adr/0028-tamper-evident-chain.md) — the L1 chain.
- [Recipe 19 — L2 Merkle anchors](./19-merkle-anchors.md) — the
  Merkle roots this recipe witnesses externally.
- Sigstore Rekor — https://docs.sigstore.dev/logging/overview
- OpenTimestamps — https://opentimestamps.org/
- RFC 6962 — Certificate Transparency, the design pattern that
  Rekor follows for transparency logs.
