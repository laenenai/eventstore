# 21: Schema Evolution — Tier-B Upcaster (Unit Conversion)

Some schema changes don't move a single byte on disk but quietly change
what those bytes *mean*. A latency that used to be in milliseconds is
now reported in microseconds. A price that used to be cents is now
millicents. The wire encoding (`int64`, `int32`, an enum value) doesn't
change. Field tags don't change. Only the **interpretation** changes —
and proto3 has no machinery to notice.

This recipe shows the smallest working pattern for a Tier-B migration
per [ADR 0030 § Tier B](../adr/0030-schema-migration-discipline.md):
bump `(es.v1.schema_version)` and register a pure-function upcaster
that translates old values into the new shape at read time. Events on
disk stay byte-stable; reads through the upcaster produce events in
the current shape.

The runnable fixture lives at
[`gen/test/unitsmigration/v1/`](../../gen/test/unitsmigration/v1) and
ships as part of the framework so adopters can copy the pattern wholesale.

## When to use this

- A numeric field changed units (ms → µs, cents → millicents, bytes →
  KiB).
- An enum value's interpretation shifted ("PENDING" used to mean "not
  yet seen", now means "seen but not yet processed").
- A boolean's polarity flipped without renaming the field (rare;
  usually a bad idea, but Tier B is the mechanism if you do it).

The hallmark of Tier B: the wire bytes for an old event are still
parseable under the new schema definition — they just produce a *wrong*
in-memory value unless the upcaster intervenes.

## When *not* to use this

- **Wire shape changed** (e.g. `bytes` → `string`, raw → base64). That
  is Tier D (PII shape) or, in its non-PII form, also handled by a
  Tier-B-like upcaster *but* against a frozen legacy proto. See recipe
  11 § "PII shape migration" for the canonical Tier-D shape.
- **State proto changed** (the aggregate's `State` message). That is
  Tier C — bump `aggregate.Runtime.StateSchemaVersion` and let
  `state_cache` repopulate. No event upcaster involved.
- **Field tag or wire type changed.** That breaks protobuf wire
  compatibility — Tier F. You need a one-off data migration or a
  rename via a different `type_url`.

## The pattern

Two proto files cooperate:

```
proto/test/unitsmigration/v1/unitsmigration.proto        # current shape (schema_version = 2)
proto/test/unitsmigration/v1/legacy/legacy.proto         # frozen v1 shape
```

The current shape carries `(es.v1.schema_version) = 2`:

```proto
package test.unitsmigration.v1;

message MeasurementRecorded {
  option (es.v1.schema_version) = 2;

  string measurement_id = 1 [(es.v1.subject_field) = true];
  int64  latency_us     = 2;   // microseconds
}
```

The legacy shape is a hand-written, **frozen** sibling proto in a child
package (`...v1.legacy`) so the codegen plugin compiles it as a
separate unit:

```proto
package test.unitsmigration.v1.legacy;

message MeasurementRecordedV1 {
  string measurement_id = 1;
  int64  latency_ms     = 2;   // milliseconds — same wire tag, same wire type
}
```

Notice: field tag 2 is `int64` in both protos. Same wire bytes. Only
the name and meaning differ.

The `MigratingCodec` wraps the codegen-emitted `EventCodec` and
dispatches on `schemaVersion`:

```go
func (c MigratingCodec) Decode(typeURL string, schemaVersion uint32, payload []byte) (Event, error) {
    if schemaVersion >= 2 {
        return c.Inner.Decode(typeURL, schemaVersion, payload) // current path
    }
    switch typeURL {
    case "test.unitsmigration.v1.MeasurementRecorded":
        var old legacyv1.MeasurementRecordedV1
        if err := proto.Unmarshal(payload, &old); err != nil {
            return nil, fmt.Errorf("upcast MeasurementRecorded v1: %w", err)
        }
        return &MeasurementRecorded{
            MeasurementId: old.MeasurementId,
            LatencyUs:     old.LatencyMs * 1000, // 1 ms = 1000 µs
        }, nil
    }
    return c.Inner.Decode(typeURL, schemaVersion, payload) // unknown → inner's error
}
```

That's the whole pattern. Encode always emits the current shape; the
upcaster is read-only.

## Wiring into the runtime

`aggregate.Runtime` accepts any value that satisfies
`aggregate.Codec[E]`. Wire the `MigratingCodec` instead of the bare
`EventCodec`:

```go
rt := &aggregate.Runtime[State, Command, Event]{
    Store:   store,
    Decider: measurement.Decider,
    Codec:   unitsmigrationv1.MigratingCodec{Inner: unitsmigrationv1.EventCodec{}},
}
```

From here on, the runtime calls `Decode(typeURL, schemaVersion, payload)`
on every event it loads. Schema-version-1 rows on disk produce
current-shape events in memory; schema-version-2 rows pass through.

## Field rename: optional but supported

The fixture renames `latency_ms` → `latency_us` to make the unit shift
obvious to anyone reading code six months later. A rename is **not
required** for Tier B — you could leave the field name as
`latency` and just shift its meaning. The frozen legacy proto then
keeps the old name in its (separate) namespace. Pick whichever
communicates the change more clearly to future readers; the wire
behavior is identical either way.

## What NOT to do

- **Do not mutate the legacy proto post-freeze.** Once `legacy.proto`
  ships, treat it as immutable. Adding fields, changing tags, or
  re-running it through current `es.v1` options will silently shift the
  upcaster's input shape and break replays of old data.
- **Do not perform I/O in the upcaster body.** ADR 0013 mandates pure
  functions: no clocks, no random sources, no network/disk reads, no
  cache lookups. Same input → same output, forever. The framework will
  ship a determinism linter (ADR 0013 § Shipped lint suite) that flags
  `time.Now()`, `math/rand`, and I/O calls inside upcaster bodies;
  don't write code that depends on the linter being lax.
- **Do not skip the v2-passthrough test.** It's the canary that proves
  the upcaster only runs for old data. A bug in the switch (e.g.
  forgetting `if schemaVersion >= 2 { ... }`) silently double-converts
  the unit on every read — visible in production as latencies that
  grow by 1000× on every redeploy.
- **Do not rewrite stored bytes.** ADR 0013 alternatives §
  "In-place rewrite on read" explains why: it breaks signing, hash
  chains (ADR 0028), and audit trails. The events table is immutable.
- **Do not chain multiple unit conversions in one upcaster.** One
  upcaster handles one hop (v1 → v2). When you later go v2 → v3, add
  a second upcaster; the runtime composes them. Mixing the hops in
  one function defeats the lint suite and complicates reasoning.

## Failure modes

- **Forgot to bump `schema_version`.** New writes go out tagged as v1,
  the upcaster reads them, multiplies by 1000 again — latencies look
  1000× too big. Symptom shows up immediately in any read that round-
  trips a freshly written event. Mitigation: the codegen plugin reads
  `(es.v1.schema_version)` from the proto; the schema-version
  monotonicity linter (ADR 0013) fails the build on a missing bump.
- **Forgot to register `MigratingCodec`.** The runtime falls back to
  the bare `EventCodec`, which decodes v1 bytes under the v2 struct
  layout — proto3 doesn't error, it just gives you a microsecond
  reading that's actually milliseconds (1000× too small). Symptom:
  old data reads correctly through projections that bypass the
  runtime, but the aggregate sees stale-meaning values. Mitigation:
  the compile-time assertion `var _ aggregate.Codec[Event] =
  MigratingCodec{}` catches type-shape regressions; integration tests
  that load a known v1 fixture catch the wiring slip.
- **Edited the legacy proto.** Same hazard as above, harder to debug
  — the upcaster's input shape silently drifts. Mitigation: review
  discipline. The legacy proto's top comment names this explicitly
  ("FROZEN — do not modify").
- **Upcaster math overflows int64.** `latency_ms * 1000` overflows
  somewhere past `9.2 × 10^15` (~292,000 years of latency). Real
  systems won't hit this. If you migrate a field that does (atomic
  clock readings? cosmic-scale physics?), validate the input range
  in the upcaster and return an explicit error rather than producing
  a silently corrupted value.

## Tests in the fixture

The fixture ships four tests at
[`gen/test/unitsmigration/v1/migration_test.go`](../../gen/test/unitsmigration/v1/migration_test.go):

1. **Happy path** — write a v1 payload with `latency_ms=5`, decode at
   `schemaVersion=1`, assert `latency_us=5000`.
2. **v2 passthrough** — a current-shape payload at `schemaVersion=2`
   survives unchanged.
3. **Zero value** — `latency_ms=0` produces `latency_us=0` (no
   spurious fabricated reading).
4. **Unknown type URL** — falls through to the inner codec's
   canonical "unknown type_url" error at both schema versions.

That's the minimum bar for shipping a Tier-B upcaster. Copy the shape
to your own aggregate, swap the proto names, and you're done.

## References

- [ADR 0013](../adr/0013-schema-evolution-upcasters.md) — upcaster
  contract (pure-fn, one-hop, schema_version dispatch).
- [ADR 0030](../adr/0030-schema-migration-discipline.md) — migration
  tiers and the PR-template checkbox.
- Recipe 11 § "PII shape migration" — the Tier-D sibling pattern,
  same upcaster shape against a frozen legacy proto carrying
  encrypted bytes.
