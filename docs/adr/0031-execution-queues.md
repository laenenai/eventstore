# ADR 0031: Execution Queues — Backend-Neutral Routing Hint

- **Status:** Accepted (amends ADR 0025, ADR 0026)
- **Date:** 2026-05-16
- **Pairs with:** ADR 0025 (workflow-orchestrated command bus), ADR 0026
  (workflow adapters — Restate and DBOS).

## Context

Production adopters running the workflow-orchestrated command bus
(ADR 0025) eventually want differential treatment of commands:
priority lanes for end-user-facing flows, dedicated capacity for
heavy batch operations, rate limits on calls that hit external APIs.
DBOS exposes named `dbos.WorkflowQueue` values for exactly this:
declare a queue with concurrency caps, rate limits, and priority;
enqueue workflows onto it by name. Restate takes a different shape —
its virtual-objects model already serializes per object key, so
"queue" isn't a primitive; isolation comes from key partitioning.
The inproc adapter has no scheduling model at all.

The framework has two competing pressures. One: adopters need a way
to tell the runtime "this command goes on the high-priority lane"
without writing adapter-specific glue at every call site. Two: the
framework promises portability across adapters — code that runs on
inproc in tests must run on Restate or DBOS in production, with the
same logical behavior. A queue concept that's first-class in DBOS,
non-existent in Restate, and meaningless in inproc cannot be modeled
as a hard contract.

The naive approach — add a `WithQueue` option to the bus that the
DBOS adapter honors and the others reject — produces a portability
trap. Adopters write code that depends on queues for correctness,
deploy to Restate, and silently lose the guarantee.

## Decision

`cmdworkflow.WithQueue(ctx, name)` attaches an **advisory** routing
hint to ctx. The framework defines the surface; each adapter
interprets the hint per its native model. No adapter is required to
enforce the hint; adopters who depend on queues for correctness
(rather than performance / isolation) violate the contract and will
degrade silently across adapters. This is an explicit, documented
trade-off — the alternative (hard contract) would force the framework
to either drop Restate support or build a queue abstraction that
duplicates virtual-objects in a strictly inferior shape.

### Adapter behavior

**DBOS** maps the hint to a declared `*dbossdk.WorkflowQueue`. The
adapter exposes two constructor options:

- `WithQueues(map[string]*dbos.WorkflowQueue)` — adopter declares the
  queues; the runtime stores the mapping.
- `WithStrictQueues(bool)` — controls unknown-queue policy. Default
  false: unrecognized names fall back to the "default" queue with a
  one-time slog WARN per unique name. True: `ResolveQueue` returns
  `ErrUnknownQueue` which `HandleCmd` surfaces to the caller.

Resolution lives on `(*Runtime).ResolveQueue` and the convenience
wrapper `(*Runtime).QueueOption`. Codegen emits a `sendAsync` that
applies the resolved `dbos.WithQueue(name)` to the `RunWorkflow` call
that dispatches `AsyncDispatch` — so async subscribers inherit the
queue routing from the command context.

The "default" queue is implicit: if no `*dbos.WorkflowQueue` is
declared for the name `"default"`, resolution returns a nil queue
handle and codegen falls through to no-queue (immediate) execution.
Adopters who want the default name to map to an actual queue with
concurrency caps include it in the `WithQueues` map explicitly.

**Restate** logs the requested queue name at DEBUG once per unique
name (sync.Map-based dedup) and otherwise no-ops. Restate's
virtual-objects model already serializes per object key — the same
isolation guarantee DBOS uses queues for is provided by key
partitioning instead. Adopters whose design depends on queue-style
routing should partition aggregates across virtual-object keys to
achieve the same effect.

**inproc** logs at DEBUG once per unique name and otherwise no-ops.
inproc executes synchronously on the caller goroutine; there is no
scheduling decision to make. The log is purely a traceability
breadcrumb for tests that exercise the routing path before swapping
in a real adapter.

### Default queue name

`cmdworkflow.DefaultQueue = "default"`. A literal constant rather
than the empty-string-means-default convention. Two reasons:

1. Adapter wiring stays symmetric — the same name appears in adopter
   config maps and in routing-decision branches.
2. The defensive resolution `WithQueue(ctx, "")` collapsing to
   `DefaultQueue` removes the silent-fall-off-the-queue-map footgun
   for adopters who derive the queue name from a config lookup that
   came back blank.

`QueueFromContext` is guaranteed to never return `""`. Empty context,
empty value, unset, all collapse to `DefaultQueue`. Adapter code can
key a `map[string]*Queue` lookup against the result without a "" guard.

### Scope (v1)

Command execution only. Subscribers spawned by `HandleCmd` inherit
the queue via context — Sync subscribers run inline as steps inside
the parent workflow (DBOS step granularity has no queue concept;
queues apply at workflow invocation boundaries only), Async
subscribers go through the codegen `sendAsync` which applies the
queue option on the `RunWorkflow` call.

Projection runtime queue routing and outbox-drain queue routing are
out of scope for this ADR. Those subsystems take a queue at runtime
construction (not via context), which is a smaller and different
design problem to be addressed in a follow-up ADR.

## Alternatives considered

**Per-call option on `HandleCmd`** — `HandleCmd(ctx, sid, cmd,
cmdworkflow.WithQueue("high"))`. Rejected: pollutes the generic
command API with a backend-specific concern. The advisory contract
reads better as a context value because that's exactly its semantic
weight — propagating-but-non-binding metadata. Putting it on the
options struct implies a hard contract the framework can't deliver.

**Queue-bound `Workflow` constructor** — `cmdworkflow.New(...,
WithQueue("high"))` binding the entire bus to one queue. Rejected:
less flexible at runtime; the common case is "most commands on
default, a few high-priority ones on a separate lane," which a
static-per-bus design forces into either multiple bus instances or
manual dispatch trees.

**"Lane" or "shard" naming** — both rejected in favor of "queue" for
industry alignment. "Lane" is the more semantically honest term (a
routing dimension, not necessarily a queue), but every adopter who's
worked with DBOS, Sidekiq, BullMQ, RabbitMQ, AWS SQS, or any
priority-execution framework will reach for "queue" first.
The technical inaccuracy on Restate is mitigated through per-adapter
docs and this ADR.

**Hard contract — every adapter must implement queue routing** —
rejected as a Restate non-starter. The Restate team has documented
that virtual objects are the intended isolation primitive; building
a separate queue abstraction inside Restate would either duplicate
existing functionality or fight the runtime. The advisory contract
keeps the door open for adopters who specifically need DBOS-shape
queueing without forcing Restate adopters to absorb conceptual cost.

## Consequences

The contract is advisory. Code that runs on DBOS with priority
queues will get vanilla scheduling on Restate. This is graceful
degradation by design — but it is degradation, and adopters who
choose a multi-backend deployment posture must understand it.
The mitigation is documentation: the doc.go for each adapter, the
README for each adapter, and the cookbook recipe for command-bus
wiring (cookbook 14, to be updated) all call out the queue model
explicitly.

The DBOS adapter gains a small wire-up surface: `WithQueues`,
`WithStrictQueues`, `ResolveQueue`, `QueueOption`,
`ErrUnknownQueue`. The codegen `sendAsync` emission grows by ~10
lines to apply the resolved queue option. The interface surface
contract stays narrow — adopters who don't use queues don't see them.

`(*cmdworkflow.Workflow).Runtime()` is now part of the framework's
public surface so codegen can type-assert against `*cwdbos.Runtime`
to call `QueueOption`. This is an awkward but unavoidable concession:
the WorkflowRuntime interface stays narrow (Run / RunAsync / Spawn);
adapter-specific knobs like queue routing have to come through the
concrete type. The accessor is documented as adapter-integration
machinery, not for general adopter use.

Future work, deliberately out of scope for this ADR:

- Outbox-drain queue routing — likely a runtime-construction option
  (`outbox.New(WithQueue("outbox"))`) rather than per-message context
  propagation.
- Projection runtime queue routing — same shape; queue per projection
  worker.
- A `cwdbos.RunWorkflow` adopter-facing helper that auto-applies the
  queue option from context, so adopters can wrap their outer
  `RunWorkflow` calls without writing the resolution boilerplate.
  Trivial to add once the contract has been exercised in production.
