# 18: Clock Injection and Time Acceleration in Tests

Aggregates and workflows that care about time — KYC refresh due dates,
permission overrides with `expires_at`, retention windows, last-seen
bucket boundaries — are painful to test if every assertion has to wait
wall-clock time. The framework ships a `Clock` abstraction so tests
can drive time forward in milliseconds; production keeps using real
time without ceremony.

This recipe shows how to wire `Clock` into a `Runtime`, the patterns
for testing expiry / bucket-boundary logic, and the anti-patterns that
break replay determinism.

## Why Clock injection matters

Four reasons the framework owns this rather than letting each
aggregate roll its own:

1. **Test determinism.** "Advance 30 days, assert the override
   expired" is a one-liner with `ManualClock.Advance(30*24*time.Hour)`;
   without it, the only options are sleeping (slow, flaky) or threading
   bespoke clock parameters through every domain helper.

2. **Time acceleration.** Multi-day workflows (review cadence,
   retention drains, last-seen bucket transitions) become fast tests.
   Centcom's KYC pipeline exercises a 12-month refresh window in
   sub-second test time by advancing the runtime's clock.

3. **Replay determinism.** Replaying old events through a Decider must
   produce the same state regardless of when replay happens. If
   anything on the framework's hot path calls `time.Now()` directly,
   replay is non-deterministic. The Clock funnel keeps the entire
   framework time-honest.

4. **Server-side authoritative timestamps.** The framework stamps
   `OccurredAt` from its own clock by default — clients can't lie
   about when their commands happened. Test clocks fold into the same
   discipline; production clocks are simply `RealClock`.

## The primitive

```go
// es/clock.go
type Clock interface {
    Now() time.Time
}

var RealClock Clock = realClock{}      // production default
type ManualClock struct { /* ... */ }  // test-only, deterministic
```

`ManualClock` is safe for concurrent use, returns UTC, and notifies
registered waiters on every `Set` / `Advance` — see "Workflow sleeps"
below.

## Wiring a Clock into a Runtime

Production:

```go
rt := &aggregate.Runtime[*pb.Employee, pb.Command, pb.Event]{
    Store:   store,
    Decider: employee.Decider,
    Codec:   employeev1.EventCodec{},
    // Clock omitted — defaults to es.RealClock.
}
```

Test:

```go
clock := es.NewManualClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
rt := &aggregate.Runtime[*pb.Employee, pb.Command, pb.Event]{
    Store:   store,
    Decider: employee.Decider,
    Codec:   employeev1.EventCodec{},
    Clock:   clock,
}

// Hire on day 1.
rt.Handle(ctx, sid, &pb.Hire{...})

// Fast-forward 30 days, then assert.
clock.Advance(30 * 24 * time.Hour)
rt.Handle(ctx, sid, &pb.RunRefreshCheck{})

state, _, _ := rt.Load(ctx, sid)
if !state.NeedsRefresh { t.Errorf("...") }
```

Envelope `OccurredAt` reflects `Clock.Now()` — both the initial Hire
and the post-Advance refresh-check carry the clock's view of "now".

## Pattern: testing expiry windows

KYC refresh-due, override `expires_at`, document retention — anything
that says "after N units of time, transition state":

```go
func TestKYC_RefreshDueAfter12Months(t *testing.T) {
    start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
    clock := es.NewManualClock(start)
    rt := newKYCRuntime(t, clock)

    // Onboard the customer.
    rt.Handle(ctx, sid, &kyc.Onboard{...})

    // Refresh-check exactly at the 12-month boundary: still ok.
    clock.Set(start.AddDate(1, 0, 0).Add(-time.Second))
    state, _, _ := rt.Load(ctx, sid)
    if state.RefreshStatus != kyc.RefreshCurrent {
        t.Errorf("expected current at 12mo-1s, got %v", state.RefreshStatus)
    }

    // One second past the anniversary: refresh now due.
    clock.Advance(2 * time.Second)
    rt.Handle(ctx, sid, &kyc.EvaluateRefresh{})
    state, _, _ = rt.Load(ctx, sid)
    if state.RefreshStatus != kyc.RefreshDue {
        t.Errorf("expected due at 12mo+1s, got %v", state.RefreshStatus)
    }
}
```

The Decider does the bucket comparison; the test drives the clock.
Wall-clock independent, sub-millisecond runtime.

## Pattern: testing time-bucketed events

Recipe 17 shows last-seen / heartbeat dedup by truncating the command's
timestamp to a calendar-day bucket. To test that the bucket boundary
genuinely produces exactly one event per day, drive the clock and use
the runtime's clock as the command's seen_at:

```go
clock := es.NewManualClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
rt := newIdentityRuntime(t, clock)

// 10 ticks within the same day → 1 event.
for i := 0; i < 10; i++ {
    rt.Handle(ctx, sid, &identity.MarkLastSeen{SeenAt: clock.Now()})
    clock.Advance(2 * time.Hour)
}
// First 10 calls span 0h..18h — same day.

// One more after midnight → second event.
clock.Set(time.Date(2025, 1, 2, 0, 0, 1, 0, time.UTC))
rt.Handle(ctx, sid, &identity.MarkLastSeen{SeenAt: clock.Now()})

envs, _ := store.ReadStream(ctx, sid, 0)
if len(envs) != 2 {
    t.Errorf("expected 2 events (one per day), got %d", len(envs))
}
```

The Decider's bucket math is the unit under test; the clock just
supplies deterministic boundary inputs.

## Pattern: workflow sleeps that don't actually sleep

`ManualClock.NotifyOnTick()` returns a channel that closes on the next
`Set`/`Advance` call. A test harness can substitute a real workflow
sleep with "wait on the next clock tick":

```go
// Inside a test-only workflow shim:
func (s *testSleeper) Sleep(d time.Duration) {
    tick := s.clock.NotifyOnTick()
    target := s.clock.Now().Add(d)
    for s.clock.Now().Before(target) {
        <-tick
        tick = s.clock.NotifyOnTick()
    }
}
```

The test drives the clock; the workflow's "sleep" unblocks as soon as
the clock catches up to the target instant. No wall-clock waits, no
goroutine timers leaking across tests.

**Caveat for DBOS / Restate adapters**: those runtimes own their
durable sleep semantics — the framework deliberately doesn't try to
hijack them. Use `ManualClock.NotifyOnTick()` only inside test shims
of `WorkflowRuntime`, not against a real Restate or DBOS server.

## Anti-pattern: calling `time.Now()` in domain code

The framework's contract says: aggregates are pure functions over
(state, command) and (state, event). `time.Now()` in `Decide` or
`Evolve` violates that:

```go
// DON'T:
func Decide(s State, c Command) ([]Event, error) {
    switch cmd := c.(type) {
    case *Renew:
        return []Event{&Renewed{
            ExpiresAt: time.Now().Add(365 * 24 * time.Hour),
        }}, nil
    }
}
```

Replay non-determinism follows immediately: replaying this event a
year later through a freshly-deployed service produces a different
`ExpiresAt` than the original execution — the resulting state diverges
between writers.

Two fixes, in decreasing order of preference:

1. **Pass the timestamp on the command.** The caller (HTTP edge,
   cmdworkflow bus) stamps `now` once at the boundary; the Decider
   reads it from the command. This is the canonical pattern.

2. **Use the runtime's clock at the boundary, not in the Decider.**
   The HTTP / cmdworkflow / cron-trigger layer reads `clock.Now()` and
   threads it into the command before calling `Handle`. The Decider
   stays pure.

Either way, the Decider never asks "what time is it?"; it only
*receives* a time.

## Anti-pattern: `time.Now()` directly inside framework hot paths

Same issue at the framework boundary — replay-time vs. write-time
divergence. The framework's own callsites (`aggregate/runtime.go`,
`cmdworkflow/bus.go` DLQ row, etc.) all go through the runtime's
`Now()` for exactly this reason. Adapters' framework-managed
timestamps (storage adapters' `recorded_at`) belong to the *write
path* and intentionally use real wall-clock — they're commit-time
audit columns, not replay inputs.

If you're authoring a new framework primitive that needs "now",
thread a `Clock` parameter through it. If you're authoring an
application-layer subscriber that needs "now", read it off the
runtime's clock or get it from the envelope.

## Failure modes + edge cases

### Test clock skew at bucket boundaries

`ManualClock.Set` accepts any instant; nothing stops a test from
rewinding the clock. The framework does not assume monotonicity. Most
Deciders that compare timestamps to state will produce surprising
results if the clock rewinds mid-test — pick `Advance` over `Set` for
forward motion to avoid the footgun.

### Wait-once channel semantics

`NotifyOnTick()` returns a channel that's closed on the very next
`Set` / `Advance`. Subsequent ticks need a new subscription. Worker
loops that want continuous wakeups must re-subscribe after each
observed tick — see the workflow-sleep example above.

### Production with a non-default Clock

Nothing in the framework prevents wiring a non-`RealClock` Clock in
production. One legitimate use case: a `frozenClock` for replay-tools
that backfill events at archive timestamps. Another: a
`monotonicClock` wrapping `time.Now()` to harden against NTP step
adjustments. Both are user-land; the framework's only requirement is
that whatever clock is wired returns a sensible time.Time.

### Concurrent test goroutines vs ManualClock

Multiple test goroutines reading `clock.Now()` while one drives
`Advance` is safe by design — `ManualClock` is read-mostly with a
single writer. Multiple writers (two goroutines advancing the same
clock) is undefined in practice; if a test needs that pattern,
serialize through a single owning goroutine.

## See also

- ADR 0003 — Decider pattern (where the "pure function over (state,
  command)" rule originates)
- Recipe 17 — high-frequency bucketed events (the canonical time-
  bucket dedup pattern that this recipe enables testing of)
- Recipe 04 — time-based triggers (Restate / DBOS durable sleeps;
  what NOT to try and replace with ManualClock)
- Recipe 14 — cmdworkflow deployment (the bus uses the runtime's
  clock for DLQ rows; production wiring needs nothing extra)
