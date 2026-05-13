---
name: define-aggregate
description: Walk through defining a new event-sourced aggregate for the eventstore framework — proto file, codegen, decider implementation, runtime wiring. Use when the user wants to create a new aggregate, define a new domain entity, or implement an event-sourced model.
---

# define-aggregate

Step-by-step guide for creating a new aggregate in this framework.

The full reference lives in [README.md → Defining an aggregate](../../../README.md#defining-an-aggregate). Treat this skill as the interactive companion: it asks the user the questions needed to materialize a working aggregate from a blank slate.

## When this skill applies

The user wants to:
- Create a new event-sourced aggregate
- Define a domain entity (User, Order, Account, etc.) using the framework
- Translate a hand-written decider into proto + codegen
- Scaffold proto files and decider code for a new domain

If the user is asking about the framework's *design* (why deciders? why proto?), point at the ADRs in `docs/adr/` instead.

## What information to gather first

Before writing anything, get from the user:

1. **Aggregate name.** Lowercase, single word, used as the proto package suffix and the Go package name. Examples: `user`, `order`, `account`, `subscription`.
2. **Domain package prefix.** The fully-qualified package root, e.g. `myapp` (gives `myapp.user.v1`). Default to whatever the existing `proto/myapp/` or similar shows.
3. **State shape.** The fields the aggregate carries: their names, types, whether they're PII.
4. **Commands.** The actions a caller can request: their names (imperative verbs) and arguments.
5. **Events.** The facts that get recorded: their names (past-tense verbs) and payloads.
6. **Invariants / business rules.** What the decider should reject and why.
7. **Uniqueness needs.** Does this aggregate own any first-class uniqueness constraints (e.g., one email per user)?

If any of these are unclear, ask one question at a time rather than dumping a checklist.

## Step 1 — Write the .proto file

Create `proto/<prefix>/<aggregate>/v1/<aggregate>.proto`. Structure follows the README's walked-through example.

Skeleton:

```protobuf
syntax = "proto3";

package <prefix>.<aggregate>.v1;

import "es/v1/options.proto";

option go_package = "github.com/<owner>/<repo>/gen/<prefix>/<aggregate>/v1;<aggregate>v1";

// ---- State -------------------------------------------------------------

message <Aggregate> {
  option (es.v1.aggregate) = "<aggregate>";

  // Subject id field — auto-exempt from PII encryption.
  string <aggregate>_id = 1 [(es.v1.subject_field) = true];

  // Domain fields. Default: encrypted. Mark non-PII fields explicitly.
  // ...
}

// ---- Commands (variants) ------------------------------------------------

message <CommandName1> {
  // fields
}

message <CommandName2> {
  // fields
}

message Commands {
  option (es.v1.sum_type) = "Command";
  oneof variant {
    <CommandName1> <command_name_1> = 1;
    <CommandName2> <command_name_2> = 2;
  }
}

// ---- Events (variants) --------------------------------------------------

message <EventName1> {
  // fields
}

message <EventName2> {
  // fields
}

message Events {
  option (es.v1.sum_type) = "Event";
  oneof variant {
    <EventName1> <event_name_1> = 1;
    <EventName2> <event_name_2> = 2;
  }
}
```

Apply naming conventions:
- Commands: imperative (Register, ChangeEmail, AssignRole)
- Events: past-tense (Registered, EmailChanged, RoleAssigned)
- State fields: lowercase snake_case for proto, will become PascalCase in Go
- PII fields default to encrypted; mark non-PII with `[(es.v1.non_pii) = true]`

## Step 2 — Generate code

Run `task generate` (or `cd proto && buf generate` directly).

This produces:
- `gen/<prefix>/<aggregate>/v1/<aggregate>.pb.go` — standard protobuf types
- `gen/<prefix>/<aggregate>/v1/<aggregate>_es.pb.go` — sealed Command/Event interfaces + Codecs

Verify with `go build ./...`. If buf reports lint errors, the most common are:
- Missing `v1` suffix on the package
- Package path not matching the directory structure
- Reusing a field number

## Step 3 — Write the decider

Create a Go file alongside the gen output, typically in a new package per aggregate. Conventional location depends on the consumer's repo — for a test fixture in this framework it sits beside the test that uses it; for a production app, place it under your domain layer.

Skeleton:

```go
package <aggregate>

import (
    "errors"

    "github.com/laenenai/eventstore/es"
    <aggregate>v1 "github.com/<owner>/<repo>/gen/<prefix>/<aggregate>/v1"
)

// State is the folded representation of the aggregate. Plain Go struct;
// does not have to mirror the proto <Aggregate> message exactly.
type State struct {
    // ...
}

// Error sentinels for rejected commands.
var (
    Err<BusinessRule1> = errors.New("<aggregate>: <description>")
    // ...
)

var Decider = es.Decider[State, <aggregate>v1.Command, <aggregate>v1.Event]{
    Initial: func() State { return State{} },

    Decide: func(s State, c <aggregate>v1.Command) ([]<aggregate>v1.Event, []es.ConstraintOp, error) {
        switch cmd := c.(type) {
        case *<aggregate>v1.<CommandName1>:
            // 1. Validate against current state.
            // 2. Return events to append.
            // 3. Optionally return constraint operations (Claim/Release).
            return []<aggregate>v1.Event{
                &<aggregate>v1.<EventName1>{/* fields */},
            }, nil, nil

        default:
            return nil, nil, errors.New("unknown command")
        }
    },

    Evolve: func(s State, e <aggregate>v1.Event) State {
        switch evt := e.(type) {
        case *<aggregate>v1.<EventName1>:
            // Mutate the state in response to this event.
            // Keep this function pure: no clocks, no I/O, no randomness.
        }
        return s
    },
}
```

Critical reminders to surface to the user:
- **Decide may not have side effects.** It returns events; the runtime commits them.
- **Evolve must be pure.** No `time.Now()`, no `uuid.New()` — those go on the event from the command (ADR 0003).
- **Uniqueness constraints** are declared by returning `[]es.ConstraintOp{Claim/Release, scope, value}` from Decide. The framework commits them atomically with the events (ADR 0010).

## Step 4 — Wire the runtime

In the consumer's main.go or wiring layer:

```go
runtime := &aggregate.Runtime[<aggregate>.State, <aggregate>v1.Command, <aggregate>v1.Event]{
    Store:   store, // any es.Store implementation
    Decider: <aggregate>.Decider,
    Codec:   <aggregate>v1.EventCodec{},
}

// Handle a command:
streamID, _ := es.NewStreamID(tenant, "<aggregate>", "<id>")
result, err := runtime.Handle(ctx, streamID, &<aggregate>v1.<CommandName1>{/* ... */})
```

## Step 5 — Write tests

Tests typically live near the decider. The common shape:

```go
func TestDecider_<Scenario>(t *testing.T) {
    // Arrange: setup state by replaying a few events
    state := <aggregate>.Decider.Initial()
    for _, evt := range []es.Event{...} {
        state = <aggregate>.Decider.Evolve(state, evt)
    }

    // Act: run a command
    events, _, err := <aggregate>.Decider.Decide(state, &<aggregate>v1.<CommandName>{/* ... */})

    // Assert: check events / error
    // ...
}
```

For integration tests, use SQLite `:memory:` directly per the pattern in `adapters/storage/sqlite/aggregate_test.go`.

## Common patterns and pitfalls

**Pattern: one aggregate emits a command on another via a saga.**
Not a framework primitive (ADR 0015). Write a subscriber that reads the source aggregate's events from the bus and calls the target aggregate's `Handle`. See `docs/cookbook/02-stateful-saga.md`.

**Pitfall: putting `time.Now()` in Evolve.** Replay determinism (ADR 0003). Time goes on the event via `OccurredAt`, captured at command-handle time.

**Pitfall: skipping the subject_field annotation.** Future crypto-shredding codegen needs to know which field identifies the subject. Add it now even if shredding isn't enabled yet.

**Pitfall: mixing aggregates in one proto file.** Each `.proto` should define exactly one aggregate. If two aggregates need to share types (e.g., a common `Money` message), put the shared message in a separate proto file and import it.

## Reference

- README → [Defining an aggregate](../../../README.md#defining-an-aggregate) — the full reference.
- [ADR 0003](../../../docs/adr/0003-decider-aggregate-model.md) — Why the decider model.
- [ADR 0004](../../../docs/adr/0004-sum-type-representation.md) — Why sum types via proto oneof + codegen.
- [ADR 0008](../../../docs/adr/0008-stream-identity.md) — Stream identity and the `(es.v1.aggregate)` option.
- [ADR 0010](../../../docs/adr/0010-crypto-shredding.md) — PII annotations.
- [ADR 0013](../../../docs/adr/0013-schema-evolution-upcasters.md) — Schema versioning.
- [ADR 0016](../../../docs/adr/0016-codegen-plugin-packaging.md) — How codegen is invoked.
- Example: [`proto/test/counter/v1/counter.proto`](../../../proto/test/counter/v1/counter.proto) — the framework's own test aggregate.
- Example: [`adapters/storage/sqlite/aggregate_test.go`](../../../adapters/storage/sqlite/aggregate_test.go) — the Counter wired up against SQLite.
