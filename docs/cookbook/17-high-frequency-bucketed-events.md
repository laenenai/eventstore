# 17: High-Frequency Bucketed Events

When a domain command fires very frequently — login, heartbeat,
last-seen tick, online-status update, cursor position — naive
event emission blows up the event stream. ES says "every state change
is an event", but some "state changes" are really continuous-update
signals that don't warrant per-occurrence durability.

This recipe captures the canonical patterns.

## Problem

Take a login-tracking field like `last_seen_at` on an `Identity`
aggregate. Naive Decider:

```go
case *MarkLastSeen:
    return []Event{&LastSeenMarked{SeenAt: c.SeenAt}}, nil
```

For 1M users with 50% daily active over 5 years: **~900M events** for
one field. Storage, replay time, projection cost, and query-plan
overhead all degrade. The events are also low-value individually —
"customer logged in at 14:03:21" rarely matters; "customer was active
on this date" usually does.

## Pattern 1: Decider-side bucket dedup (recommended starting point)

The Decider truncates the new timestamp to a bucket (calendar day,
hour, week — chosen by domain need) and emits at most one event per
bucket per stream:

```go
func (d Decider) Decide(s *Identity, cmd Command) ([]Event, error) {
    switch c := cmd.(type) {
    case *MarkLastSeen:
        newBucket := c.SeenAt.AsTime().UTC().Truncate(24 * time.Hour)
        if !s.LastSeenAt.AsTime().IsZero() {
            prevBucket := s.LastSeenAt.AsTime().UTC().Truncate(24 * time.Hour)
            if newBucket.Equal(prevBucket) {
                return nil, nil // same day → no event
            }
        }
        return []Event{&LastSeenMarked{
            SeenAt: c.SeenAt,
            Actor:  c.Actor,
        }}, nil
    }
}
```

**Caller contract**: invoke the command on every signal. No
client-side coordination. The Decider is the single source of
bucketing truth.

**Bucket policy is server-controlled**: changing daily → weekly is a
Decider-only change — no API change, no client deployment.

**Volume bound**: O(active-buckets) per stream. For daily on a 5-year
active customer, ~1825 events.

### What the timestamp on the event represents

The emitted event's `SeenAt` is the **command's `seen_at`** — the
first observation in that bucket. Subsequent same-bucket calls don't
move the timestamp forward. This is usually what you want
("first-seen-today") because:

- It's deterministic on replay — given the same command sequence, the
  same events emit with the same timestamps.
- Downstream projections that care about "first activity per day"
  get it directly.

If you instead want "most-recent-time-in-this-bucket" semantics, the
Decider has to emit a second event when the within-bucket time
advances past some threshold — at which point you're back to nearly
unbounded volume. Pick first-observation; it's the right call ~95%
of the time.

### Wall-clock determinism caveat

The Decider compares `c.SeenAt` (caller-supplied) against
`s.LastSeenAt` (from event replay). If the caller's clock is wrong
or skewed, bucket boundaries shift. Mitigations:

- **Server-clock the timestamp**: the framework's command bus can
  inject a server-side `time.Now()` into the command before the
  Decider sees it. Some teams prefer this over trusting client
  clocks for any timestamp.
- **Trust client clock but cap drift**: reject commands where
  `c.SeenAt` is more than N minutes in the future (likely clock skew).

For login signals from a trusted auth IDP (Clerk, Auth0): trust the
IDP's claim timestamp. For client-emitted signals: server-clock.

## Pattern 2: Separate short-retention stream

For genuinely audit-required firehoses (every login with IP, device,
geolocation), keep the main aggregate's stream clean and put the
firehose on a **separate aggregate with short retention**.

```protobuf
message LoginAudit {
  option (es.v1.aggregate) = "login_audit";
  string login_session_id = 1 [(es.v1.subject_field) = true];

  // The identity this login is for — QUASI_IDENTIFIER, encrypted
  // under the LoginAudit's own DEK so retention on the audit stream
  // doesn't pollute the Identity stream.
  string identity_id = 2 [(es.v1.data_classification) = DATA_CLASSIFICATION_QUASI_IDENTIFIER];

  google.protobuf.Timestamp at = 3;
  string ip          = 4 [(es.v1.data_classification) = DATA_CLASSIFICATION_PERSONAL];
  string device_id   = 5 [(es.v1.data_classification) = DATA_CLASSIFICATION_INTERNAL];
  string user_agent  = 6 [(es.v1.data_classification) = DATA_CLASSIFICATION_INTERNAL];
  // ... etc
}
```

Retention worker auto-shreds `LoginAudit` streams after the policy
window (30-90 days for typical fraud-investigation needs).

Pattern 1 still runs on the main `Identity` stream — daily-bucketed
"customer was active this day" — for long-term lifecycle queries.
Pattern 2 covers short-term forensic queries.

Don't combine the two on the same stream: mixing high-frequency +
low-frequency events ruins replay performance and retention
boundaries.

## Pattern 3: Out-of-ES

When the signal is purely operational and has no audit / retention /
replay requirement:

- **Online-status** ("is the user currently active in the app?") →
  Redis with TTL. Not a state mutation; not durable.
- **Cursor position in a real-time editor** → CRDT in memory; persist
  periodic checkpoints.
- **Per-request rate-limit counter** → Redis INCR + sliding window.

The test: does losing this signal on a power loss matter to the
domain? If no, don't put it in ES.

## Pattern 4 (anti-pattern): direct state mutation

```go
// DON'T:
case *MarkLastSeen:
    s.LastSeenAt = c.SeenAt
    return nil, nil // no event, but state mutated
```

Violates the framework's contract: state MUST be reconstructable from
event replay. `state_cache` is a mirror; Decider tests rely on
event-based determinism. Don't take this shortcut even when tempting.

## Choosing between patterns

| Pattern | When to pick it |
| ------- | --------------- |
| **1 (bucketed dedup)** | Default. The signal has long-term audit value at coarse granularity (last seen, last refresh, recent activity). Bucket size matches the question being answered. |
| **2 (short-retention firehose)** | The signal has short-term forensic value at full granularity (login attempts, payment authorizations, fraud signals). |
| **1 + 2 combined** | Identity's bucketed "active this day" on the main stream, plus a parallel `LoginAudit` stream with 90-day retention. Common for fintech auth. |
| **3 (out-of-ES)** | The signal is purely volatile operational state — no audit, no replay, can be reconstructed from logs or just lost. |

## Failure modes + edge cases

### Clock skew at bucket boundaries

A login at 23:59:59 UTC + a login at 00:00:01 UTC = two events
(two different days). If the customer was active across midnight,
you get a bucket boundary blip. This is **correct** — they were
active on two calendar days. The "always exactly one event per
active period" interpretation breaks at any bucket boundary you
choose; pick the bucket size that matches the question being
answered.

### Bucket policy migration

Changing daily → weekly: historical events stay daily-bucketed; new
events use the new bucket. State queries (`state.LastSeenAt`) return
the most recent emission timestamp, which is monotonic — no replay
break. Just document the policy change in the aggregate's README so
future developers know when the bucket changed.

### Replay vs live behavior

Replaying old events through the new Decider must produce the same
state. This works because:

- The Decider's `Evolve` (apply event to state) doesn't know about
  bucketing — it just sets `s.LastSeenAt = e.SeenAt`.
- `Decide`'s bucketing only affects FUTURE emissions, not the
  interpretation of existing events.

So a bucket-policy change is forward-only and safe for replay.

### Concurrent writes within a bucket

Two `MarkLastSeen` commands for the same identity within milliseconds
of each other in the same bucket: the framework's optimistic
concurrency on the stream serialises them. The second one's Decider
sees the first one's emitted event already in state, deduplicates,
returns zero events. The bus may retry the second one with the new
version; the retry sees the dedup and is also a no-op. Idempotent
under at-least-once delivery.

### What if `seen_at` arrives out of order

A `MarkLastSeen` with `seen_at` in the past (e.g. delayed webhook).
Decider sees `c.SeenAt < s.LastSeenAt`. Options:

- **Reject the command** as a clock skew / out-of-order indicator
  (returns an error to the caller).
- **Bucket-compare regardless of order**: if the old timestamp's
  bucket isn't already represented, emit. This is more permissive.
- **Always emit if bucket isn't covered, regardless of order**.

For login: the order matters only for "what's the most recent
seen?". Pick option 1 (reject out-of-order) for simplicity; the auth
layer shouldn't be sending delayed signals.

## When to upgrade Pattern 1 → 2

Triggers:

- Forensic teams ask for IP + device per login (not just date).
- Fraud-detection model needs per-login features (geolocation pattern,
  device fingerprint history).
- Regulator asks for the firehose with retention >7 days.

Add a parallel `LoginAudit` aggregate at that point. Pattern 1 stays
in place — they serve different questions.

## See also

- ADR 0003 — Decider pattern (where bucketing lives)
- ADR 0010 / ADR 0027 — crypto-shredding + data classification (apply
  to the audit firehose's PII fields)
- Recipe 06 — outbox drain (events still flow through the outbox; the
  bucketing just changes how many events get there)
- Recipe 13 — state_stream coalesced delivery (related coalescing
  pattern at the projection layer)
