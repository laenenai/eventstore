# eventstore

An event sourcing framework for Go with first-class uniqueness, multi-
tenancy, and crypto-shredding. Targets scale-to-zero databases
(Neon, Turso) with delivery via an external durable runtime.

## Status

In development. The architectural design is complete; implementation is
in early stages.

## Design

- [Architecture Decision Records](docs/adr/) — 19 ADRs covering the
  framework's spine. Start with the [index](docs/adr/README.md).
- [Cookbook](docs/cookbook/) — patterns for application code that the
  framework deliberately does not provide (sagas, process managers,
  workflows, timeouts).

## Repository layout

```
es/               Core API (Decider, Envelope, StreamID, ...)
aggregate/        Aggregate runtime
projection/       Projector runtime
outbox/           Outbox primitives + drain helpers
snapshot/         Snapshot primitives
shred/            Crypto-shredding logic
publisher/        EventPublisher interface (+ inproc adapter)
kms/              KeyStore interface (+ inproc adapter)
estest/           Test harness (given/when/then)
proto/            Framework's own .proto files
gen/              Generated Go from framework protos
cmd/              CLI tools
  protoc-gen-es-go/   Codegen plugin (Phase 2)
  esctl/              Operational CLI
adapters/
  storage/
    postgres/     Postgres adapter (pgx)
    sqlite/       SQLite adapter (driver-agnostic; modernc / libsql)
  publisher/      External publisher adapters (Phase 2+)
  kms/            External KMS adapters (Phase 2+)
docs/
  adr/            Architecture Decision Records
  cookbook/       Application-pattern recipes
```

## Defining an aggregate

Aggregates are defined in `.proto` files. The codegen plugin
(`protoc-gen-es-go`, invoked by `buf generate`) reads them and
produces sealed Go interfaces for the command and event sum types,
marker methods on each variant, and `Codec` implementations that
marshal each variant under its full proto type URL.

What stays hand-written per aggregate: error sentinels and the
`Decider{Initial, Decide, Evolve}` — these carry the business rules
codegen cannot guess. The State message defined in the .proto is
used directly as the aggregate's folded state (see
[examples/party/](examples/party/) for a complex real example).

### File layout

One `.proto` file per aggregate, under a versioned package path.
Buf's `STANDARD` lint set enforces `package_version_suffix` and
`package_directory_match`, so the package and path must agree:

```
proto/myapp/user/v1/user.proto          package myapp.user.v1
proto/myapp/order/v1/order.proto        package myapp.order.v1
```

### Walked-through example: Counter

The simplest possible aggregate — a bounded counter — lives at
[`proto/test/counter/v1/counter.proto`](proto/test/counter/v1/counter.proto)
and validates the end-to-end pipeline:

```protobuf
syntax = "proto3";
package test.counter.v1;

import "es/v1/options.proto";

option go_package = "github.com/<you>/myapp/gen/test/counter/v1;counterv1";

// 1. State — the aggregate's folded state shape. Annotate with
//    (es.v1.aggregate) = "name". The name will become the typed
//    StreamID wrapper's Go package in a future codegen iteration.
message Counter {
  option (es.v1.aggregate) = "counter";

  bool  initialized = 1;
  int64 count       = 2;
  int64 min         = 3;
  int64 max         = 4;
}

// 2. Each command is its own top-level message. Plain protos.
message Init {
  int64 min     = 1;
  int64 max     = 2;
  int64 initial = 3;
}

message Increment { int64 by = 1; }
message Decrement { int64 by = 1; }

// 3. The Commands container declares the sum type. The
//    (es.v1.sum_type) option names the Go interface to generate.
//    Convention: container = "Commands", interface = "Command".
message Commands {
  option (es.v1.sum_type) = "Command";

  oneof variant {
    Init      init      = 1;
    Increment increment = 2;
    Decrement decrement = 3;
  }
}

// 4. Each event is its own top-level message.
message Initialized {
  int64 min   = 1;
  int64 max   = 2;
  int64 value = 3;
}

message Incremented { int64 by = 1; }
message Decremented { int64 by = 1; }

// 5. The Events container, same pattern as Commands.
message Events {
  option (es.v1.sum_type) = "Event";

  oneof variant {
    Initialized initialized = 1;
    Incremented incremented = 2;
    Decremented decremented = 3;
  }
}
```

### What codegen produces

`task generate` invokes both `protoc-gen-go` (standard) and
`protoc-gen-es-go` (framework-specific). Two output files per .proto:

```
gen/test/counter/v1/counter.pb.go      standard protobuf Go types
gen/test/counter/v1/counter_es.pb.go   sealed interfaces + Codecs
```

The framework-specific file gives you:

```go
// Sealed interfaces. The unexported marker method keeps the variant
// set closed to additions from outside this generated file.
type Command interface { isCommand() }
type Event   interface { isEvent() }

// Marker methods on each variant — pointer receivers because protoc-
// gen-go emits message types intended to be used as pointers.
func (*Init)      isCommand() {}
func (*Increment) isCommand() {}
func (*Decrement) isCommand() {}

func (*Initialized) isEvent() {}
func (*Incremented) isEvent() {}
func (*Decremented) isEvent() {}

// Codecs implementing aggregate.Codec[T]. Compile-time assertions
// catch interface drift.
type CommandCodec struct{}
type EventCodec   struct{}

var _ aggregate.Codec[Command] = CommandCodec{}
var _ aggregate.Codec[Event]   = EventCodec{}
```

`Encode`/`Decode` marshal each variant as canonical proto bytes and
tag the `EncodedEvent` with the variant's full proto type name
(e.g., `test.counter.v1.Init`).

### Wiring it up

The hand-written piece per aggregate is small. The recommended
pattern uses the proto-defined State message directly:

```go
// State is the proto-defined message — no parallel Go struct needed.
// proto.Marshal(state) gives you free snapshots (ADR 0011) and the
// schema lives in exactly one place.
type State = counterv1.Counter

var Decider = es.Decider[*State, counterv1.Command, counterv1.Event]{
    Initial: func() *State { return &State{} },

    Decide: func(s *State, c counterv1.Command) ([]counterv1.Event, []es.ConstraintOp, error) {
        switch cmd := c.(type) {
        case *counterv1.Init:
            if s.GetInitialized() {
                return nil, nil, errAlreadyInit
            }
            return []counterv1.Event{
                &counterv1.Initialized{Min: cmd.Min, Max: cmd.Max, Value: cmd.Initial},
            }, nil, nil
        case *counterv1.Increment:
            // ... business rules
        }
    },

    Evolve: func(s *State, e counterv1.Event) *State {
        switch evt := e.(type) {
        case *counterv1.Initialized:
            s.Initialized = true
            s.Min, s.Max, s.Count = evt.Min, evt.Max, evt.Value
        // ...
        }
        return s
    },
}
```

Then wire the runtime against any `es.Store`:

```go
runtime := &aggregate.Runtime[*State, counterv1.Command, counterv1.Event]{
    Store:   store,                  // postgres or sqlite adapter
    Decider: Decider,
    Codec:   counterv1.EventCodec{}, // generated
}

result, err := runtime.Handle(ctx, streamID, &counterv1.Init{
    Min: 0, Max: 100, Initial: 5,
})
```

### Hand-written state struct — when it's the right choice

For aggregates where the state has derived fields, requires
specialized data structures (e.g., a map for O(1) lookup), or is
much smaller than the wire representation, a hand-written Go struct
is also defensible:

```go
type State struct {
    Initialized bool
    Count       int64
    Min, Max    int64
}

var Decider = es.Decider[State, counterv1.Command, counterv1.Event]{...}
```

The framework supports either — `aggregate.Runtime[S, C, E]` is
generic over the state type. The test fixture at
[`adapters/storage/sqlite/aggregate_test.go`](adapters/storage/sqlite/aggregate_test.go)
uses this pattern for the toy Counter. The Party example uses proto
state for production-shape illustration.

### Conventions

- **One `.proto` file per aggregate.** Mixing aggregates dilutes the
  sum types — a single Commands oneof should hold one aggregate's
  commands, not many.
- **Container/interface names** are `Commands`/`Command` and
  `Events`/`Event` by convention. Not enforced; useful for
  readability and tooling.
- **Variants are top-level messages**, not nested inside the
  container. Nesting works but produces uglier Go names
  (`Commands_Init` vs `Init`).
- **Event names are past-tense domain verbs** (Initialized,
  Incremented); **command names are imperatives** (Init, Increment).

### PII annotations (forthcoming)

The options below are already defined in `proto/es/v1/options.proto`
and will be consumed by codegen in a later iteration. Add them now
and the resulting protos remain forward-compatible.

```protobuf
message UserRegistered {
  // Subject identifier — auto-exempt from encryption (you'd need
  // the key to find the key). See ADR 0010.
  string user_id = 1 [(es.v1.subject_field) = true];

  // PERSONAL or stricter: codegen emits EncryptPII/DecryptPII so
  // the field is sealed under the subject's DEK on the wire.
  string email     = 2 [(es.v1.data_classification) = DATA_CLASSIFICATION_PERSONAL];
  string full_name = 3 [(es.v1.data_classification) = DATA_CLASSIFICATION_PERSONAL];

  // INTERNAL / PUBLIC / unset: stays plaintext on disk.
  string status = 4 [(es.v1.data_classification) = DATA_CLASSIFICATION_INTERNAL];

  // Financial / cardholder / credential — each has its own
  // retention + DSAR + audit rules (see ADR 0027 + cookbook 11).
  int64 salary_cents = 5 [(es.v1.data_classification) = DATA_CLASSIFICATION_FINANCIAL];
}
```

Multi-subject events (transfers, shared resources) declare per field:

```protobuf
message TransferRecorded {
  string from_user = 1 [(es.v1.subject) = "from_user"];
  string to_user   = 2 [(es.v1.subject) = "to_user"];
  int64  amount    = 3 [(es.v1.data_classification) = DATA_CLASSIFICATION_FINANCIAL];
}
```

### Schema versioning

Bump an event's `schema_version` when its semantics change in a way
proto's wire-format compatibility cannot detect (units shift, enum
meaning changes, etc.) — see ADR 0013.

```protobuf
message Incremented {
  option (es.v1.schema_version) = 2;  // bumped from default 1

  int64 by_millicents = 1;             // was `by_cents` in v1
}
```

The upcaster registry (planned codegen work) will scaffold a stub
when the version bumps and fail the build until the body is filled in.

See [`docs/adr/`](docs/adr/) for the full architectural rationale
behind each option.

## Toolchain

Versions are pinned in [`.mise.toml`](.mise.toml). Install
[mise](https://mise.jdx.dev) and run:

```sh
mise install     # installs go, buf, sqlc, task at the pinned versions
```

Common workflows are defined in [`Taskfile.yml`](Taskfile.yml):

```sh
task            # list tasks
task test       # run tests across all modules
task generate   # buf + sqlc codegen
task lint:proto # buf lint + buf breaking
task vet
task build
```

## License

TBD.
