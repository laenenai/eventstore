# 02: Stateful Saga

When you need to react after multiple events have happened in some
combination, the coordinator needs its own state. In this framework,
that coordinator is just **a regular aggregate** with its own stream.

## When to use this

- You wait for multiple events before deciding what to do
  ("after both A and B").
- The order of events doesn't matter, only that the set is complete.
- The coordinator must survive process restarts and remember what it
  has already seen.
- Examples: onboarding flows ("registered + email verified + payment
  authorized → grant access"), order fulfillment ("paid + shipped +
  delivered → mark complete"), KYC pipelines.

## The pattern

The saga is an aggregate. Its commands are envelope-driven — the
subscriber wraps each incoming event in an `OnEvent` command and
dispatches it to the saga aggregate. The aggregate's `Decide` looks at
its current state and the event, decides what saga-internal events to
emit, and optionally emits a "ready" event that other handlers act on.

State lives in the saga's stream — same persistence, same snapshots,
same crypto-shredding as any other aggregate. The framework does not
need to know it's a "saga".

## Example: onboarding

Grant access only when the user is registered AND email is verified
AND payment is authorized. Order doesn't matter.

```protobuf
// myapp/onboarding/v1/saga.proto
syntax = "proto3";
package myapp.onboarding.v1;

message Commands {
  oneof variant {
    OnEvent on_event = 1;
  }
}

message OnEvent {
  bytes envelope = 1; // serialized es.Envelope
}

message Events {
  oneof variant {
    UserRegisteredRecorded    user_registered    = 1;
    EmailVerifiedRecorded     email_verified     = 2;
    PaymentAuthorizedRecorded payment_authorized = 3;
    AccessGranted             access_granted     = 4;
  }
}

message State {
  string user_id              = 1 [(es.subject_field) = true];
  bool   user_registered      = 2 [(es.non_pii) = true];
  bool   email_verified       = 3 [(es.non_pii) = true];
  bool   payment_authorized   = 4 [(es.non_pii) = true];
  bool   access_granted       = 5 [(es.non_pii) = true];
}
```

Decider in Go:

```go
package onboarding

import (
    "github.com/<org>/eventstore/es"
    "github.com/<org>/myapp/userpb"
    "github.com/<org>/myapp/paymentpb"
)

var Decider = es.Decider[State, OnEvent, Event]{
    Initial: func() State { return State{} },

    Decide: func(s State, c OnEvent) ([]Event, []es.ConstraintOp, error) {
        env, err := es.DecodeEnvelope(c.Envelope)
        if err != nil {
            return nil, nil, err
        }

        switch payload := env.Payload.(type) {
        case *userpb.UserRegistered:
            if s.UserRegistered {
                return nil, nil, nil // idempotent — already recorded
            }
            return appendIfComplete(s, UserRegisteredRecorded{UserID: payload.UserID}), nil, nil

        case *userpb.EmailVerified:
            if s.EmailVerified {
                return nil, nil, nil
            }
            return appendIfComplete(s, EmailVerifiedRecorded{UserID: payload.UserID}), nil, nil

        case *paymentpb.PaymentAuthorized:
            if s.PaymentAuthorized {
                return nil, nil, nil
            }
            return appendIfComplete(s, PaymentAuthorizedRecorded{UserID: payload.UserID}), nil, nil
        }

        return nil, nil, nil // event we don't care about
    },

    Evolve: func(s State, e Event) State {
        switch e := e.(type) {
        case UserRegisteredRecorded:    s.UserRegistered = true; s.UserID = e.UserID
        case EmailVerifiedRecorded:     s.EmailVerified = true
        case PaymentAuthorizedRecorded: s.PaymentAuthorized = true
        case AccessGranted:             s.AccessGranted = true
        }
        return s
    },
}

// appendIfComplete also emits AccessGranted when all three are recorded.
func appendIfComplete(prev State, recorded Event) []Event {
    next := Decider.Evolve(prev, recorded)
    if next.UserRegistered && next.EmailVerified && next.PaymentAuthorized && !next.AccessGranted {
        return []Event{recorded, AccessGranted{UserID: next.UserID}}
    }
    return []Event{recorded}
}
```

Subscriber wiring — one subscriber feeds the saga, one acts on its
"ready" output:

```go
// In main.go

// Feed the saga from all the events it needs to observe.
pub.Subscribe(
    []es.EventTypeURL{
        "myapp.user.v1.UserRegistered",
        "myapp.user.v1.EmailVerified",
        "myapp.payment.v1.PaymentAuthorized",
    },
    func(ctx context.Context, env es.Envelope) error {
        // Extract user ID from the event to find the saga instance.
        userID, ok := userIDFromEnvelope(env)
        if !ok { return nil }

        sid, err := onboarding.NewStreamID(ctx, userID)
        if err != nil { return err }

        cmdID := es.DeriveCommandID("onboarding-feeder", env.EventID, 0)
        return onboardingRuntime.Handle(ctx, sid,
            &onboarding.OnEvent{Envelope: env.MustMarshal()},
            es.WithCommandID(cmdID),
        )
    },
)

// React to the saga's "I'm ready" output.
pub.Subscribe(
    []es.EventTypeURL{"myapp.onboarding.v1.AccessGranted"},
    func(ctx context.Context, env es.Envelope) error {
        granted := env.Payload.(*onboardingpb.AccessGranted)
        sid, _ := user.NewStreamID(ctx, granted.UserID)
        cmdID := es.DeriveCommandID("access-granter", env.EventID, 0)
        return userRuntime.Handle(ctx, sid,
            &userpb.GrantAccess{},
            es.WithCommandID(cmdID),
        )
    },
)
```

## Why this works

- The saga aggregate uses the framework's normal optimistic-concurrency,
  snapshot, and persistence machinery. Nothing new.
- Events arriving out of order are fine: `Decide` checks the current
  state and only emits `AccessGranted` when the full set is recorded.
- Duplicate bus deliveries: `Decide` is idempotent (returns no events
  if the flag is already set). The framework's `command_id` dedup is
  the outer safety net.
- The saga's "I'm ready" event (`AccessGranted`) is itself a normal
  event in the saga's stream — auditable, queryable, projectable.
- A separate subscriber turns `AccessGranted` into a concrete command
  on the user aggregate. Same pattern as recipe 01.

## Key design choices to absorb

- **The saga emits a publishable signal event** (`AccessGranted`) rather
  than dispatching the command itself. This keeps `Decide` pure and
  testable, and makes the workflow visible in the event log.
- **One subscriber feeds many event types into the saga.** All three
  observed events become `OnEvent` commands on the same saga instance.
- **Saga instance ID is derived from the domain key** (user ID here).
  The subscriber resolves the saga stream ID from the event's payload.

## Snapshot and crypto-shredding interaction

The saga's state contains the `user_id` (a subject field, auto-exempt
from encryption per ADR 0010) and a few booleans (non-PII). If a user
asks for deletion, dropping their DEK shreds any PII fields across all
events — including events in the saga's stream that referenced PII —
without disturbing the saga's structural state.

Snapshots invalidate automatically if the saga's `State` shape changes
(`state_schema_version` bump) — see ADR 0011.

## When this pattern stops scaling

Stateful sagas work well up to "tens of state fields, dozens of
transitions". Beyond that, consider splitting the workflow into
multiple smaller sagas, each owning one phase. Or model the workflow
explicitly as a Restate workflow handler (durable execution outside the
eventstore) and keep the eventstore for the canonical domain log.
