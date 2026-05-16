<!--
PR template. Lean — only the load-bearing bits. If you're submitting a
trivial change (typo, docs nit), feel free to delete sections that
don't apply.
-->

## Summary

<!-- 1-3 sentences: what + why. Skip the "how" — that's the diff. -->

## Schema migration tier

Per [ADR 0030](docs/adr/0030-schema-migration-discipline.md), declare
the tier of any schema-touching change:

- [ ] **Tier A** — pure additive (no migration required)
- [ ] **Tier B** — `schema_version` bump + upcaster registered (ADR 0013)
- [ ] **Tier C** — `StateSchemaVersion` bump; rebuild `state_cache` after deploy (ADR 0023)
- [ ] **Tier D** — classification migration upcaster registered (ADR 0027 / 0010)
- [ ] **Tier E** — envelope hash subset change — **NEW ADR REQUIRED** (ADR 0028)
- [ ] **Tier F** — breaking change; explicit migration script included and tested
- [ ] **Not applicable** — no schema touched

If Tier B / C / D / F: link to the upcaster / migration / script below.
If Tier E: link to the new ADR.

> Reviewers: refuse "Not applicable" if the diff touches `proto/**/*.proto`,
> `adapters/storage/*/migrations/*.sql`, `aggregate/runtime.go`,
> `es/envelope.go`, or `proto-gen/emit_*.go`.

## Test plan

<!--
Bulleted markdown checklist. Reviewers and CI both look at this.
For schema changes (Tier B-F): include a test that exercises the
migration path.
-->

- [ ]
