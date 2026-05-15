package es

import (
	"sync"
	"time"
)

// Clock is the framework's abstraction for "now". Runtime carries one;
// tests inject ManualClock for deterministic time-travel.
//
// All framework code that needs a wall-clock timestamp MUST go through
// the Runtime's Clock — never time.Now() directly. This keeps tests
// deterministic and makes time-acceleration possible:
//
//   - Domain expiry windows (KYC refresh-due, override expires_at,
//     retention) can be exercised in milliseconds by advancing a
//     ManualClock rather than waiting wall-clock days.
//   - Replay determinism: events stamped with the runtime's Now()
//     produce the same envelopes regardless of when replay runs.
//   - Server-side authoritative timestamps: the framework — not the
//     caller — decides what "now" means for envelope metadata.
//
// Production wiring uses RealClock; tests inject NewManualClock(...).
type Clock interface {
	Now() time.Time
}

// RealClock returns time.Now().UTC(). The default for production
// Runtimes when no Clock is wired explicitly.
//
// UTC normalization is deliberate — envelope timestamps must be
// monotone-comparable across processes regardless of host timezone.
var RealClock Clock = realClock{}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now().UTC() }

// ManualClock is a test-only Clock with explicit Now() control. Safe
// for concurrent use: many readers can call Now() while a single test
// goroutine drives Set/Advance.
//
// Set and Advance notify any waiters registered via NotifyOnTick.
// Workflows that would otherwise block on real-time Sleep can subscribe
// and unblock as soon as the test moves the clock forward.
type ManualClock struct {
	mu      sync.RWMutex
	now     time.Time
	waiters []chan struct{}
}

// NewManualClock constructs a ManualClock with the given starting
// instant. Tests that don't care about the absolute value typically
// pass time.Now().UTC(); tests that want a fixed seed for golden-file
// comparison should pass a stable value (e.g. a known epoch).
//
// The stored time is normalized to UTC; ManualClock.Now() always
// returns UTC, matching RealClock's behavior.
func NewManualClock(start time.Time) *ManualClock {
	return &ManualClock{now: start.UTC()}
}

// Now returns the current instant under the manual clock.
func (c *ManualClock) Now() time.Time {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.now
}

// Set replaces the clock's current instant. Notifies all waiters
// registered via NotifyOnTick.
//
// Set going backwards is allowed (tests sometimes need to rewind to
// re-exercise a transition); the framework imposes no monotonicity
// invariant.
func (c *ManualClock) Set(t time.Time) {
	c.mu.Lock()
	c.now = t.UTC()
	waiters := c.waiters
	c.waiters = nil
	c.mu.Unlock()
	notifyWaiters(waiters)
}

// Advance moves the clock forward by d. Notifies all waiters.
//
// Negative durations are accepted (and rewind the clock); they're
// occasionally useful for testing out-of-order delivery.
func (c *ManualClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	waiters := c.waiters
	c.waiters = nil
	c.mu.Unlock()
	notifyWaiters(waiters)
}

// NotifyOnTick returns a channel that's closed on the next Set or
// Advance call. Each call returns a fresh channel — closing-once is
// the broadcast mechanism, so the receiving goroutine should
// re-subscribe after observing a tick if it wants to wait for the
// following one.
//
// Used by DBOS/Restate workflow tests where the workflow's Sleep would
// otherwise wait on wall-clock time. Sleep-replacement glue:
//
//	tick := mc.NotifyOnTick()
//	mc.Advance(24 * time.Hour)
//	<-tick
//
// Returns an already-closed channel if the caller never advances —
// no leaks, no goroutine pile-up.
func (c *ManualClock) NotifyOnTick() <-chan struct{} {
	ch := make(chan struct{})
	c.mu.Lock()
	c.waiters = append(c.waiters, ch)
	c.mu.Unlock()
	return ch
}

// notifyWaiters closes every channel in the slice. Close-once is the
// broadcast primitive — every reader observes the tick exactly once
// and re-subscribes if it wants the next one.
func notifyWaiters(waiters []chan struct{}) {
	for _, ch := range waiters {
		close(ch)
	}
}
