# Party (individual) — worked example

A complex aggregate demonstrating most of the framework's features
end-to-end on a realistic domain. If the Counter test fixture taught
you the framework's shape, this is the next step: a domain you'd
actually ship.

## What this example exercises

| Feature                          | Demonstrated by                                                                   |
| -------------------------------- | --------------------------------------------------------------------------------- |
| First-class uniqueness           | Email is unique per tenant; claimed on Register, swapped atomically on email change |
| PII annotations                  | name, email, phone, address default-encrypted; `party_id` is the subject identifier |
| Maker-checker workflow           | name, email, date_of_birth go through ProposeX → Approve / Reject / Withdraw      |
| Auto-apply workflow              | phone, address mutate immediately via UpdateX commands                            |
| Status state machine             | active ⇄ suspended → closed; transitions guard mutations                          |
| Generic pending-changes shape    | one slice + oneof inside; decider enforces "at most one per kind"                 |
| Self-approval rejection          | The decider refuses Approve when `approved_by == proposed_by`                     |
| Constraint release on close      | Closing a party frees its email for reuse                                         |

## Domain at a glance

```
                       ┌──────────────────────────┐
                       │       Register           │
                       │  - claims email scope    │
                       └────────────┬─────────────┘
                                    │
                                    ▼
        ┌───────────────────── ACTIVE ───────────────────┐
        │                                                │
        │   Auto-apply (immediate):                      │
        │     UpdatePhone   → PhoneUpdated               │
        │     UpdateAddress → AddressUpdated             │
        │                                                │
        │   Maker-checker (propose → approve):           │
        │     ProposeName   ──┐                          │
        │     ProposeEmail  ──┼─► pending_changes        │
        │     ProposeDOB    ──┘                          │
        │                                                │
        │     Approve  → *ChangeApplied                  │
        │     Reject   → ChangeRejected                  │
        │     Withdraw → ChangeWithdrawn                 │
        │                                                │
        └────────────┬─────────────────┬─────────────────┘
                     │                 │
              Suspend│       Reactivate│
                     ▼                 │
                SUSPENDED ─────────────┘
                     │
                Close│
                     ▼
                  CLOSED
              (email released)
```

## State shape

State **is** the proto-defined `Party` message — used directly as the
aggregate's folded state, not duplicated as a parallel Go struct. The
decider is generic over `(S, C, E)`; using `*partyv1.Party` as `S`
means the schema lives in exactly one place.

```go
// state.go
type State = partyv1.Party
```

That's it. No conversion helpers, no `Name`/`Address` Go types
shadowing the proto, no `PendingChangeKind` discriminator enum —
the proto's oneof inside `PendingChange` already discriminates.

The decider's `Decide` reads state via the generated proto accessors
(`s.GetEmail()`, `s.GetStatus()`, `s.GetPendingChanges()`); `Evolve`
mutates the proto pointer in place:

```go
case *partyv1.Registered:
    s.Name = evt.GetName()
    s.Email = evt.GetEmail()
    s.Status = partyv1.Status_STATUS_ACTIVE
    // ...
```

The "at most one pending per change kind" invariant is enforced by
the decider (via the generic `hasPending[T]` helper in `state.go`),
not the schema. The proto `repeated PendingChange` allows N entries;
the decider rejects propose commands that would create a second
pending of the same oneof variant.

### Why proto state and not a hand-written Go struct?

- **Single source of truth.** The schema lives in the .proto.
- **Snapshots are free.** When ADR 0011 snapshots get wired up,
  `proto.Marshal(state)` is the implementation; no separate
  serialization layer.
- **Less code.** State is one type alias plus three helpers; no
  parallel struct, no field-by-field conversion.
- **Aligns with "proto is the schema"** — the same principle that
  drives the payload format choice in ADR 0006.

The trade-off is using proto's pointer-based message types (`*Name`,
`*Address`) and the slightly noisier oneof wrapper syntax inside
`Evolve` (`&partyv1.PendingChange_Name{Name: x}`). Both are
contained to the decider's *internal* mutation code, never leaking
into the caller-facing command API.

For a smaller / less proto-shaped aggregate, a hand-written state
struct is also defensible — the framework supports either.

## Business rules (Decide)

| Command                | Requires                                             | Constraints                                                  |
| ---------------------- | ---------------------------------------------------- | ------------------------------------------------------------ |
| Register               | Fresh stream; valid email                            | Claim email                                                  |
| ProposeName / ProposeEmail / ProposeDateOfBirth | status=ACTIVE; no pending of same kind | —                                                            |
| Approve                | status=ACTIVE; change exists; approved_by ≠ proposed_by | Email change: release old + claim new                    |
| Reject                 | change exists; rejected_by ≠ proposed_by             | —                                                            |
| Withdraw               | change exists; withdrawn_by == proposed_by           | —                                                            |
| UpdatePhone / UpdateAddress | status=ACTIVE                                  | —                                                            |
| Suspend                | status=ACTIVE                                        | —                                                            |
| Reactivate             | status=SUSPENDED                                     | —                                                            |
| Close                  | Registered; not already closed                       | Release email                                                |

All structural rules (self-approval, proposer-only-withdraw,
at-most-one-pending) live in the decider. They are independent of any
authz policy — even a fully-privileged admin cannot self-approve.

## Running the tests

```sh
# In-memory SQLite (default — fast, isolated per test)
go test ./examples/party/...

# Disk-backed SQLite (debugging — keeps DB files around)
EVENTSTORE_TEST_DB=/tmp/party-test go test ./examples/party/...
ls /tmp/party-test/        # one DB file per test case
```

13 tests run in ~1.3s on `:memory:`.

## Where to look in the code

```
proto/myapp/party/v1/party.proto       # state, commands, events
examples/party/state.go                # Go state struct + proto conversions
examples/party/decider.go              # the entire business-rule core
examples/party/errors.go               # domain error sentinels
examples/party/party_test.go           # 13 tests covering the workflow
```

The decider is ~250 lines and reads top-to-bottom as the domain's
rule book. Everything else is wiring.

## How authorization would layer on top

The framework intentionally stays out of authorization. The decider
enforces *structural* rules (self-approval is forbidden) but does not
know about *roles* (which user can approve, who can re-activate).

Authz is application-layer: a thin wrapper around the runtime that
checks a policy before dispatching. Sketch:

```go
type AuthzRuntime[S, C, E any] struct {
    Inner  *aggregate.Runtime[S, C, E]
    Policy Policy   // e.g., Cedar, OPA, RBAC, custom
}

func (a *AuthzRuntime[S, C, E]) Handle(ctx context.Context, sid es.StreamID, cmd C, opts ...aggregate.HandleOption) (es.AppendResult, error) {
    principal, _ := principalFrom(ctx)
    if err := a.Policy.Authorize(ctx, principal, actionOf(cmd), sid); err != nil {
        return es.AppendResult{}, err
    }
    return a.Inner.Handle(ctx, sid, cmd, opts...)
}
```

A Cedar policy for the Party domain might say:

```cedar
// Anyone in the compliance group can approve any change,
// except a change they themselves proposed.
permit (
  principal in Group::"compliance",
  action == Action::"myapp.party.v1.Approve",
  resource is Party
)
when {
  context.pending_change.proposed_by != principal.id
};

// Auto-apply phone/address only for the party themselves
// or operators with the kyc.update role.
permit (
  principal,
  action in [Action::"myapp.party.v1.UpdatePhone",
             Action::"myapp.party.v1.UpdateAddress"],
  resource is Party
)
when {
  principal.id == resource.created_by ||
  principal in Group::"kyc_operators"
};
```

The framework's codegen plugin will (in a later commit) emit
`Action() string` methods on each command variant, returning the full
proto type URL — that's the stable action name for the policy.

For now, this example deliberately omits authz to keep the focus on
the domain. A separate `examples/party/authz/` may follow once the
Action()-method codegen and a Cedar adapter land.

## Reference

- [README → Defining an aggregate](../../README.md#defining-an-aggregate)
- [`.claude/skills/define-aggregate/`](../../.claude/skills/define-aggregate/SKILL.md) — interactive walkthrough
- [ADR 0003 — Decider model](../../docs/adr/0003-decider-aggregate-model.md)
- [ADR 0010 — Crypto-shredding](../../docs/adr/0010-crypto-shredding.md)
- [ADR 0015 — Why sagas aren't a framework word](../../docs/adr/0015-decider-output-and-saga-scope.md) (the same logic applies to authz)
