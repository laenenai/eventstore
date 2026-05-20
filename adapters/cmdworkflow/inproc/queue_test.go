package inproc_test

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/laenenai/eventstore/adapters/cmdworkflow/inproc"
	"github.com/laenenai/eventstore/cmdworkflow"
)

// captureDebug installs a slog handler that captures every record at
// LevelDebug (the inproc adapter logs queue traceability at DEBUG so
// production deployments don't pay log-volume cost). Restoring the
// previous Default at cleanup avoids leaking the capture buffer into
// sibling tests run in the same process.
func captureDebug(t *testing.T) *bytes.Buffer {
	t.Helper()
	buf := &bytes.Buffer{}
	h := slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	prev := slog.Default()
	slog.SetDefault(slog.New(h))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return buf
}

// countOccurrences counts non-overlapping occurrences of substr in s.
// Useful for asserting log-dedup invariants — "this message was
// emitted exactly once" — without parsing the structured log format.
func countOccurrences(s, substr string) int {
	if substr == "" {
		return 0
	}
	return strings.Count(s, substr)
}

// TestInproc_QueueLoggedOncePerName verifies the dedup contract:
// repeated dispatches under the same queue name produce exactly one
// DEBUG log. Multiple distinct queue names each produce their own
// single log entry. The framework propagates the queue across Run /
// RunAsync / Spawn entry points; all three should share the same
// dedup table.
func TestInproc_QueueLoggedOncePerName(t *testing.T) {
	buf := captureDebug(t)

	rt := inproc.New()
	ctx := cmdworkflow.WithQueue(context.Background(), "high")

	// Hit every entry point with the same queue — only one log.
	if _, err := rt.Run(ctx, "step1", func(context.Context) ([]byte, error) { return nil, nil }); err != nil {
		t.Fatalf("Run: %v", err)
	}
	f := rt.RunAsync(ctx, "step2", func(context.Context) ([]byte, error) { return nil, nil })
	if _, err := f.Wait(); err != nil {
		t.Fatalf("RunAsync Wait: %v", err)
	}
	if err := rt.Spawn(ctx, "step3", func(context.Context) error { return nil }); err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	rt.Wait()

	logOut := buf.String()
	if got := countOccurrences(logOut, `queue=high`); got != 1 {
		t.Errorf("DEBUG log for queue=high emitted %d times, want 1\nlog:\n%s", got, logOut)
	}

	// Switch queues — second name logs once.
	otherCtx := cmdworkflow.WithQueue(context.Background(), "low")
	if _, err := rt.Run(otherCtx, "step4", func(context.Context) ([]byte, error) { return nil, nil }); err != nil {
		t.Fatalf("Run other: %v", err)
	}
	if _, err := rt.Run(otherCtx, "step5", func(context.Context) ([]byte, error) { return nil, nil }); err != nil {
		t.Fatalf("Run other again: %v", err)
	}
	logOut = buf.String()
	if got := countOccurrences(logOut, `queue=low`); got != 1 {
		t.Errorf("DEBUG log for queue=low emitted %d times, want 1\nlog:\n%s", got, logOut)
	}
	// Original name still logged exactly once after the second batch.
	if got := countOccurrences(logOut, `queue=high`); got != 1 {
		t.Errorf("DEBUG log for queue=high re-emitted; got %d total occurrences\nlog:\n%s", got, logOut)
	}
}

// TestInproc_QueueIsNoOp confirms the behavioral contract: queue
// routing on inproc never changes execution semantics. fn runs once
// per call regardless of queue.
func TestInproc_QueueIsNoOp(t *testing.T) {
	rt := inproc.New()
	var calls int
	ctx := cmdworkflow.WithQueue(context.Background(), "ignored")

	for i := 0; i < 3; i++ {
		if _, err := rt.Run(ctx, "step", func(context.Context) ([]byte, error) {
			calls++
			return nil, nil
		}); err != nil {
			t.Fatalf("Run: %v", err)
		}
	}
	if calls != 3 {
		t.Errorf("calls = %d, want 3 (queue must not affect execution)", calls)
	}
}

// TestInproc_DefaultQueueLogged covers the no-WithQueue path: the
// bare-context dispatch resolves to DefaultQueue and logs once under
// that name. Confirms QueueFromContext's never-empty invariant lands
// in the adapter as the literal "default".
func TestInproc_DefaultQueueLogged(t *testing.T) {
	buf := captureDebug(t)
	rt := inproc.New()
	if _, err := rt.Run(context.Background(), "step", func(context.Context) ([]byte, error) { return nil, nil }); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := countOccurrences(buf.String(), `queue=default`); got != 1 {
		t.Errorf("DEBUG log for queue=default emitted %d times, want 1\nlog:\n%s", got, buf.String())
	}
}
