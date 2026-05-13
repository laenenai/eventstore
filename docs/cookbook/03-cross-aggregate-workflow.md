# 03: Cross-Aggregate Workflow with Compensation

The canonical hard case in event sourcing: a domain operation spans two
or more aggregates, must be all-or-nothing in user-visible terms, but
cannot be a single database transaction across aggregate boundaries.

The answer is an explicit workflow with explicit compensation. The
framework provides the primitives; the application writes the
coordinator.

## When to use this

- Multiple aggregates must end up in a consistent state.
- If a later step fails, earlier steps must be reversed.
- Examples: money transfer (debit one account, credit another), order
  fulfillment with reservation rollback, distributed sign-up that
  spans identity and billing.

## The pattern

A coordinator aggregate (a stateful saga, recipe 02) tracks the
workflow's progress. Each step is a subscriber that translates a
workflow event into a command on the participating aggregate; failures
emit a compensating command for the preceding successful step.

Crucially: **the coordinator's state is the source of truth for "where
is this workflow"**. No reliance on bus ordering or magical retries to
recover from partial failure.

## Example: money transfer

`Account` aggregate (existing) supports `Debit`, `Credit`, `Refund`.

`Transfer` aggregate (new, the coordinator) tracks one transfer
through its lifecycle:

```
Requested → SourceDebited → DestCredited → Completed
                         ↘ Failed (compensation: Refund source)
```

### Transfer aggregate

```protobuf
// myapp/transfer/v1/transfer.proto
syntax = "proto3";
package myapp.transfer.v1;

message Commands {
  oneof variant {
    Request               request                = 1;
    RecordSourceDebited   record_source_debited  = 2;
    RecordDestCredited    record_dest_credited   = 3;
    RecordDestCreditFailed record_dest_credit_failed = 4;
    RecordCompensated     record_compensated     = 5;
  }
}

message Events {
  oneof variant {
    Requested             requested              = 1;
    SourceDebited         source_debited         = 2;
    DestCredited          dest_credited          = 3;
    DestCreditFailed      dest_credit_failed     = 4;
    Compensated           compensated            = 5;
    Completed             completed              = 6;
    Failed                failed                 = 7;
  }
}

message State {
  string source_account_id  = 1 [(es.non_pii) = true];
  string dest_account_id    = 2 [(es.non_pii) = true];
  int64  amount             = 3 [(es.non_pii) = true];
  Phase  phase              = 4 [(es.non_pii) = true];
  string fail_reason        = 5 [(es.non_pii) = true];
}

enum Phase {
  UNSTARTED       = 0;
  REQUESTED       = 1;
  SOURCE_DEBITED  = 2;
  COMPLETED       = 3;
  COMPENSATING    = 4;
  FAILED          = 5;
}
```

Decider logic (abbreviated):

```go
var Decider = es.Decider[State, Command, Event]{
    Initial: func() State { return State{} },

    Decide: func(s State, c Command) ([]Event, []es.ConstraintOp, error) {
        switch c := c.(type) {

        case Request:
            if s.Phase != Unstarted { return nil, nil, ErrAlreadyStarted }
            if c.Amount <= 0        { return nil, nil, ErrInvalidAmount }
            return []Event{Requested{
                SourceAccountID: c.SourceID,
                DestAccountID:   c.DestID,
                Amount:          c.Amount,
            }}, nil, nil

        case RecordSourceDebited:
            if s.Phase != Requested { return nil, nil, nil } // idempotent
            return []Event{SourceDebited{}}, nil, nil

        case RecordDestCredited:
            if s.Phase != SourceDebited { return nil, nil, nil }
            return []Event{DestCredited{}, Completed{}}, nil, nil

        case RecordDestCreditFailed:
            if s.Phase != SourceDebited { return nil, nil, nil }
            // Move into compensating phase — a subscriber will refund the source.
            return []Event{DestCreditFailed{Reason: c.Reason}}, nil, nil

        case RecordCompensated:
            if s.Phase != Compensating { return nil, nil, nil }
            return []Event{Compensated{}, Failed{Reason: s.FailReason}}, nil, nil
        }
        return nil, nil, nil
    },

    Evolve: func(s State, e Event) State { /* update Phase based on event */ ; return s },
}
```

### Step subscribers

One subscriber per workflow step. Each:
1. Reads its trigger event.
2. Calls the target aggregate.
3. Feeds the result back into the transfer coordinator.

```go
// Step 1: on Requested → Debit source account
pub.Subscribe(
    []es.EventTypeURL{"myapp.transfer.v1.Requested"},
    func(ctx context.Context, env es.Envelope) error {
        req := env.Payload.(*transferpb.Requested)
        transferID := transferIDFromStream(env.StreamID)

        sourceSID, _ := account.NewStreamID(ctx, req.SourceAccountID)
        cmdID := es.DeriveCommandID("transfer-debit", env.EventID, 0)

        debitErr := accountRuntime.Handle(ctx, sourceSID,
            &accountpb.Debit{
                TransferID: transferID,
                Amount:     req.Amount,
            },
            es.WithCommandID(cmdID),
        )

        transferSID, _ := transfer.NewStreamID(ctx, transferID)
        feedbackCmdID := es.DeriveCommandID("transfer-debit-feedback", env.EventID, 0)

        if debitErr != nil {
            // Source debit failed — mark whole transfer failed immediately.
            return transferRuntime.Handle(ctx, transferSID,
                &transfer.RecordFailedAtSource{Reason: debitErr.Error()},
                es.WithCommandID(feedbackCmdID),
            )
        }

        return transferRuntime.Handle(ctx, transferSID,
            &transfer.RecordSourceDebited{},
            es.WithCommandID(feedbackCmdID),
        )
    },
)

// Step 2: on SourceDebited → Credit destination account
pub.Subscribe(
    []es.EventTypeURL{"myapp.transfer.v1.SourceDebited"},
    func(ctx context.Context, env es.Envelope) error {
        transferID := transferIDFromStream(env.StreamID)

        // Reload transfer state to know dest + amount.
        // In practice, the coordinator's events carry the data;
        // here we keep the example minimal.

        destSID, _ := account.NewStreamID(ctx, /* dest */)
        cmdID := es.DeriveCommandID("transfer-credit", env.EventID, 0)

        creditErr := accountRuntime.Handle(ctx, destSID,
            &accountpb.Credit{
                TransferID: transferID,
                Amount:     /* amount */,
            },
            es.WithCommandID(cmdID),
        )

        transferSID, _ := transfer.NewStreamID(ctx, transferID)
        feedbackCmdID := es.DeriveCommandID("transfer-credit-feedback", env.EventID, 0)

        if creditErr != nil {
            return transferRuntime.Handle(ctx, transferSID,
                &transfer.RecordDestCreditFailed{Reason: creditErr.Error()},
                es.WithCommandID(feedbackCmdID),
            )
        }

        return transferRuntime.Handle(ctx, transferSID,
            &transfer.RecordDestCredited{},
            es.WithCommandID(feedbackCmdID),
        )
    },
)

// Compensation: on DestCreditFailed → Refund the source account
pub.Subscribe(
    []es.EventTypeURL{"myapp.transfer.v1.DestCreditFailed"},
    func(ctx context.Context, env es.Envelope) error {
        transferID := transferIDFromStream(env.StreamID)
        // (load source+amount from the transfer's state or from earlier events)

        sourceSID, _ := account.NewStreamID(ctx, /* source */)
        cmdID := es.DeriveCommandID("transfer-compensate", env.EventID, 0)

        refundErr := accountRuntime.Handle(ctx, sourceSID,
            &accountpb.Refund{
                TransferID: transferID,
                Amount:     /* amount */,
            },
            es.WithCommandID(cmdID),
        )

        if refundErr != nil {
            // Refund failed — the workflow is now in a bad state.
            // Alert. This is a manual-intervention case.
            return refundErr
        }

        transferSID, _ := transfer.NewStreamID(ctx, transferID)
        feedbackCmdID := es.DeriveCommandID("transfer-compensate-feedback", env.EventID, 0)
        return transferRuntime.Handle(ctx, transferSID,
            &transfer.RecordCompensated{},
            es.WithCommandID(feedbackCmdID),
        )
    },
)
```

## Why this works

- **The transfer coordinator's stream is the source of truth.** Looking
  at any transfer's events tells you exactly where it is: requested,
  debited, credited (and completed), or failed-and-compensated.
- **Every step is independently retryable.** Subscribers may run more
  than once; aggregates dedup by `command_id`; the coordinator's
  `Decide` is idempotent (no-op when phase doesn't match).
- **Compensation is an explicit step, not magic.** A failure in step 2
  produces a `DestCreditFailed` event; a dedicated subscriber observes
  it and runs the compensation. No exception unwinding, no rollback
  middleware.
- **Failure modes are observable.** Each transfer's event stream is a
  complete audit trail. Stuck transfers (e.g., compensation that
  fails) are queryable.

## Failure modes to think about

- **Compensation itself fails.** Move the transfer into a
  `NEEDS_ATTENTION` phase and alert. Don't silently retry forever; the
  underlying account aggregate may be in a state that cannot accept a
  refund.
- **The coordinator's events arrive at subscribers out of order.** Bus
  delivery is at-least-once but not strictly ordered across event
  types. Each subscriber must guard against acting on a stale phase
  (the coordinator's `Decide` is the guard).
- **Two workflows operating on the same source account collide.** Use
  the account aggregate's optimistic-concurrency (ADR 0009) — the
  losing one retries.

## When this pattern doesn't fit

If the workflow has many steps, complex branching, or long sleeps,
consider expressing the coordinator as a Restate workflow instead. The
domain events still flow through the eventstore for audit; Restate
handles the orchestration. The eventstore framework is not in the
business of being a workflow engine.

The cookbook will grow a recipe for that hybrid pattern as soon as the
first project ships it.
