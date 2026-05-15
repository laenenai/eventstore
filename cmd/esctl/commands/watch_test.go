package commands

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestWatch_RunsInitialTickThenCadence(t *testing.T) {
	var ticks atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		// First tick + ~3 more then stop. Refresh well above minRefresh
		// to keep the test reliable on slow CI.
		_ = Watch(ctx, 50*time.Millisecond, func(_ context.Context) error {
			ticks.Add(1)
			return nil
		})
	}()
	// Even with minRefresh clamping (50→100ms), 350ms is enough for
	// the immediate tick + 2-3 more.
	time.Sleep(350 * time.Millisecond)
	cancel()
	time.Sleep(20 * time.Millisecond)

	got := ticks.Load()
	if got < 2 {
		t.Errorf("expected at least 2 ticks, got %d", got)
	}
}

func TestWatch_PropagatesError(t *testing.T) {
	wantErr := errors.New("tick boom")
	err := Watch(context.Background(), time.Second, func(_ context.Context) error {
		return wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Errorf("got %v want %v", err, wantErr)
	}
}

func TestWatch_RespectsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancel; initial tick still runs once (per contract)

	calls := 0
	err := Watch(ctx, time.Second, func(_ context.Context) error {
		calls++
		return nil
	})
	if err != nil {
		t.Errorf("watch returned %v on cancelled ctx", err)
	}
	if calls != 1 {
		t.Errorf("expected exactly 1 initial tick, got %d", calls)
	}
}

func TestWatch_ClampsBelowMinRefresh(t *testing.T) {
	start := time.Now()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var ticks atomic.Int32
	done := make(chan struct{})
	go func() {
		_ = Watch(ctx, 1*time.Millisecond, func(_ context.Context) error {
			if ticks.Add(1) == 3 {
				cancel()
			}
			return nil
		})
		close(done)
	}()
	<-done
	elapsed := time.Since(start)
	// 1 initial tick + 2 more at clamped 100ms ≈ 200ms+; far more than
	// the requested 1ms would have produced.
	if elapsed < 150*time.Millisecond {
		t.Errorf("clamp not honoured: 3 ticks in %v (expected ≥150ms)", elapsed)
	}
}
