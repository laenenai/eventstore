package es_test

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/laenenai/eventstore/es"
)

func TestRealClock_Now(t *testing.T) {
	// RealClock just wraps time.Now().UTC(); we sanity-check
	// monotonicity within a single goroutine and the UTC invariant.
	t1 := es.RealClock.Now()
	if t1.Location() != time.UTC {
		t.Errorf("RealClock.Now() must return UTC, got %s", t1.Location())
	}
	t2 := es.RealClock.Now()
	if t2.Before(t1) {
		t.Errorf("RealClock.Now() not monotonic: %v then %v", t1, t2)
	}
}

func TestManualClock_BasicSetAdvance(t *testing.T) {
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	mc := es.NewManualClock(start)

	if got := mc.Now(); !got.Equal(start) {
		t.Errorf("initial Now: got %v want %v", got, start)
	}

	mc.Advance(24 * time.Hour)
	want := start.Add(24 * time.Hour)
	if got := mc.Now(); !got.Equal(want) {
		t.Errorf("after Advance: got %v want %v", got, want)
	}

	target := time.Date(2026, 6, 15, 12, 30, 0, 0, time.UTC)
	mc.Set(target)
	if got := mc.Now(); !got.Equal(target) {
		t.Errorf("after Set: got %v want %v", got, target)
	}

	// Negative advance (rewind) is allowed.
	mc.Advance(-time.Hour)
	want = target.Add(-time.Hour)
	if got := mc.Now(); !got.Equal(want) {
		t.Errorf("after negative Advance: got %v want %v", got, want)
	}
}

func TestManualClock_UTCNormalization(t *testing.T) {
	// Inputs in non-UTC zones must be normalized — envelope
	// timestamps cross process boundaries.
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skipf("America/New_York not available: %v", err)
	}
	start := time.Date(2025, 1, 1, 12, 0, 0, 0, loc)
	mc := es.NewManualClock(start)
	if got := mc.Now(); got.Location() != time.UTC {
		t.Errorf("ManualClock.Now() must be UTC, got %s", got.Location())
	}
	if !mc.Now().Equal(start) {
		t.Errorf("UTC normalization must preserve instant; got %v want %v",
			mc.Now(), start)
	}

	tNow := time.Date(2025, 6, 15, 9, 0, 0, 0, loc)
	mc.Set(tNow)
	if mc.Now().Location() != time.UTC {
		t.Errorf("Now() after non-UTC Set must be UTC")
	}
	if !mc.Now().Equal(tNow) {
		t.Errorf("Set must preserve instant across timezone normalization")
	}
}

func TestManualClock_ConcurrentSafe(t *testing.T) {
	// Many readers + one writer. Race detector catches torn reads or
	// missing locks; this also asserts that no goroutine deadlocks.
	mc := es.NewManualClock(time.Now().UTC())

	const readers = 32
	const iters = 1000

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Writer.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < iters; i++ {
			mc.Advance(time.Millisecond)
		}
		close(stop)
	}()

	// Readers.
	var readsObserved int64
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					_ = mc.Now()
					atomic.AddInt64(&readsObserved, 1)
				}
			}
		}()
	}

	wg.Wait()
	if atomic.LoadInt64(&readsObserved) == 0 {
		t.Error("no concurrent reads observed — race coverage is suspect")
	}
}

func TestManualClock_NotifyOnTick(t *testing.T) {
	mc := es.NewManualClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))

	tick1 := mc.NotifyOnTick()
	tick2 := mc.NotifyOnTick()

	// Both waiters wake on Advance.
	mc.Advance(time.Hour)

	select {
	case <-tick1:
	case <-time.After(time.Second):
		t.Fatal("tick1 not closed after Advance")
	}
	select {
	case <-tick2:
	case <-time.After(time.Second):
		t.Fatal("tick2 not closed after Advance")
	}

	// Subsequent advances do not re-close already-closed channels —
	// a new waiter is needed for the next tick.
	tick3 := mc.NotifyOnTick()
	mc.Set(mc.Now().Add(48 * time.Hour))
	select {
	case <-tick3:
	case <-time.After(time.Second):
		t.Fatal("tick3 not closed after Set")
	}

	// No waiters registered ⇒ Advance is a no-op for notifications
	// (and must not panic).
	mc.Advance(time.Minute)
}

func TestManualClock_NotifyOnTick_WorkflowSleepPattern(t *testing.T) {
	// The intended usage: a workflow waiting on a Sleep substitute.
	// Test goroutine drives the clock; worker goroutine observes the
	// tick and proceeds.
	mc := es.NewManualClock(time.Now().UTC())

	tick := mc.NotifyOnTick()
	done := make(chan struct{})

	go func() {
		<-tick
		close(done)
	}()

	// Worker should still be waiting.
	select {
	case <-done:
		t.Fatal("worker fired before Advance")
	case <-time.After(10 * time.Millisecond):
	}

	mc.Advance(30 * 24 * time.Hour) // 30 days

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("worker did not observe tick after Advance")
	}
}

func TestClock_InterfaceSatisfaction(t *testing.T) {
	// Compile-time assertions live in the file; this just exercises
	// the assignment as a runtime sanity check.
	var c es.Clock = es.RealClock
	if c.Now().Location() != time.UTC {
		t.Errorf("RealClock via interface must return UTC")
	}
	c = es.NewManualClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	if c.Now().Year() != 2025 {
		t.Errorf("ManualClock via interface mis-set")
	}
}
