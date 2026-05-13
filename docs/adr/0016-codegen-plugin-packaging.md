# ADR 0016: Codegen Plugin Packaging — buf only

- **Status:** Accepted
- **Date:** 2026-05-13

## Context

The framework's code-generation plugin emits a substantial surface per
`.proto`:

- Event, command, state, and `oneof`-variant Go structs (ADR 0004).
- Sealed interfaces and exhaustiveness analyzer hooks (ADR 0004).
- Typed `StreamID` wrappers per aggregate (ADR 0008).
- Decider scaffolding signatures (ADR 0003, ADR 0015).
- Type-URL ↔ Go-type registry entries.
- Upcaster registration stubs from `schema_version` annotations
  (ADR 0013).
- PII manifest (`pii_manifest.json`) from field annotations (ADR 0010).
- Restate service definitions for projectors and any handlers wired via
  the Restate publisher (ADR 0012).

How this plugin is distributed and invoked shapes daily developer
workflow. Three options were considered: vanilla `protoc-gen-es-go`,
`buf`-based invocation, or a hybrid supporting both.

`buf` is now the dominant toolchain for serious Go protobuf work and
provides a number of capabilities the framework would otherwise have to
reinvent — most importantly **`buf breaking`** (proto compatibility
enforcement, directly relevant to ADR 0013) and **`buf lint`** (naming
and structural rules).

## Decision

**Ship the codegen as a buf-only plugin.** No vanilla `protoc`
invocation path is supported.

### Plugin distribution

- The plugin is a single Go binary named `protoc-gen-es-go`.
- Distributed via `go install
  github.com/<org>/eventstore/cmd/protoc-gen-es-go@<version>` for local
  development.
- Configured in consumer projects as a local plugin in `buf.gen.yaml`:

```yaml
version: v2
plugins:
  - local: protoc-gen-es-go
    out: gen
    opt:
      - paths=source_relative
```

- A future hosted version on the Buf Schema Registry remains an open
  option but is not v1 scope.

### Recommended buf configuration shipped with the framework

The framework ships example configurations that consumers copy:

**`buf.yaml`** — base lint and breaking rules:

```yaml
version: v2
modules:
  - path: proto
breaking:
  use:
    - FILE             # all file-level breaking-change checks
lint:
  use:
    - DEFAULT
    - PACKAGE_VERSION_SUFFIX  # forces vN suffix on package
  except:
    - PACKAGE_DIRECTORY_MATCH # if your layout doesn't match buf's expectation
```

**`buf.gen.yaml`** — codegen invocation:

```yaml
version: v2
plugins:
  - local: protoc-gen-es-go
    out: gen
    opt:
      - paths=source_relative
  - remote: buf.build/protocolbuffers/go
    out: gen
    opt:
      - paths=source_relative
```

### Compatibility enforcement via `buf breaking`

ADR 0013 requires:

- No reuse of field numbers.
- No removal of fields without `reserved`.
- No renumbering of enum values.
- Monotonically increasing `schema_version` annotations.

The first three are enforced directly by `buf breaking --against
'.git#branch=main'`. Recommended CI gate:

```bash
buf breaking --against '.git#branch=main'
buf lint
buf generate
```

The fourth (`schema_version` monotonicity) and the framework-specific
lints (subject-field uniqueness, PII-vs-non-PII annotation discipline,
upcaster completeness) run inside the codegen plugin and fail
generation if violated.

### Custom framework lints live in the plugin, not in buf

The framework's domain-specific rules — at-most-one `(es.subject_field)`
per message, `(es.non_pii)` not redundant with subject-field, every
`schema_version` bump has a corresponding upcaster registration, no
`time.Now`/`rand`/IO inside upcaster bodies, etc. — are implemented
inside the codegen plugin. They run on every `buf generate` and fail
the generation step if violated. No separate buf lint plugin is
shipped; one binary, one set of rules.

## Consequences

### Positive

- **Schema-compatibility enforcement comes from `buf breaking`** rather
  than handwritten lint. Mature, fast, well-understood.
- **Reproducible builds.** `buf.gen.yaml` pins plugin versions; teams
  upgrade explicitly.
- **Modern, low-friction developer experience.** `buf generate` is one
  command. `buf format` keeps `.proto` files canonical.
- **One invocation path, one set of behaviors.** No "the protoc build
  works differently than the buf build" failure mode.
- **Custom framework rules** (PII annotations, upcaster completeness,
  determinism) ship in the plugin alongside the codegen — single
  binary, single distribution.

### Negative

- **Teams without `buf` must adopt it.** Adding a single CLI to the
  toolchain is small, but for some organizations it is a procurement
  or platform-team conversation.
- **Marginally less universal** than `protoc`. The framework is no
  longer usable in environments that can run `protoc` but cannot install
  `buf` — an edge case, but it exists.
- **The plugin is coupled to the buf protoc-plugin invocation
  protocol.** This is the same wire protocol as vanilla protoc, so the
  coupling is theoretical, but the framework will not test the vanilla
  `protoc` path.

## Alternatives Considered

### Vanilla `protoc-gen-es-go` only

Rejected. Loses `buf breaking` and `buf lint`, requires the framework
to ship its own compatibility-enforcement tool, and aligns the
framework with a less-current toolchain. The maintenance burden of
re-implementing `buf breaking` would be substantial and the result
would be worse than buf's.

### Hybrid (both protoc and buf)

Rejected. Doubles the tested invocation surface, doubles the docs,
adds a "which mode are you in?" failure category to every issue. The
small universality win does not justify the long-term maintenance cost.
If a team truly cannot adopt buf, the same proto can still be compiled
with `protoc` for the generic Go message types; only the framework's
codegen output is unavailable.

### Ship as a hosted plugin on the Buf Schema Registry

Deferred. Hosted plugins on the BSR are convenient (no `go install`
needed) but they push compute to Buf's infrastructure and create a hard
dependency on the BSR for the build. v1 ships as a local plugin
distributed via `go install`. Hosting on BSR remains a future option
once the plugin's API and emitted code are stable.
