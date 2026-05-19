package restate_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	cwrestate "github.com/laenenai/eventstore/adapters/cmdworkflow/restate"
	"github.com/laenenai/eventstore/cmdworkflow"
)

// captureDebug installs a slog handler that captures DEBUG records.
// Restored at cleanup so concurrent sibling tests don't see the
// capture buffer take over the process Default.
func captureDebug(t *testing.T) *bytes.Buffer {
	t.Helper()
	buf := &bytes.Buffer{}
	h := slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	prev := slog.Default()
	slog.SetDefault(slog.New(h))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return buf
}

func countOccurrences(s, substr string) int {
	if substr == "" {
		return 0
	}
	return strings.Count(s, substr)
}

// TestRestate_QueueDebugLoggedOncePerName exercises the dedup
// invariant directly against the Runtime, without standing up a
// Restate testcontainer. Run/RunAsync/Spawn return ErrNoRestateContext
// here (no restate.Context stashed), but noteQueue runs first — that
// pre-error log is what we're after.
//
// Lightweight test path is the intentional contract for this scenario:
// the WARN/DEBUG dedup is a pure-Go state machine, independent of any
// SDK behavior.
func TestRestate_QueueDebugLoggedOncePerName(t *testing.T) {
	buf := captureDebug(t)

	rt := cwrestate.New()
	high := cmdworkflow.WithQueue(context.Background(), "high")

	// Multiple dispatches under "high" — only one log entry.
	for i := 0; i < 3; i++ {
		_, err := rt.Run(high, "step", func(context.Context) ([]byte, error) { return nil, nil })
		if !errors.Is(err, cwrestate.ErrNoRestateContext) {
			t.Fatalf("expected ErrNoRestateContext, got %v", err)
		}
	}
	rt.RunAsync(high, "step", func(context.Context) ([]byte, error) { return nil, nil })
	_ = rt.Spawn(high, "step", func(context.Context) error { return nil })

	logOut := buf.String()
	if got := countOccurrences(logOut, `queue=high`); got != 1 {
		t.Errorf("DEBUG log for queue=high emitted %d times, want 1\nlog:\n%s", got, logOut)
	}

	// Switch queues — second name logs once. First name's count stays
	// at 1 (the dedup table is process-scoped, not call-scoped).
	low := cmdworkflow.WithQueue(context.Background(), "low")
	for i := 0; i < 2; i++ {
		_, _ = rt.Run(low, "step", func(context.Context) ([]byte, error) { return nil, nil })
	}
	logOut = buf.String()
	if got := countOccurrences(logOut, `queue=low`); got != 1 {
		t.Errorf("DEBUG log for queue=low emitted %d times, want 1\nlog:\n%s", got, logOut)
	}
	if got := countOccurrences(logOut, `queue=high`); got != 1 {
		t.Errorf("queue=high count drifted to %d after switching queues\nlog:\n%s", got, logOut)
	}
}

// TestRestate_DefaultQueueLogged covers the bare-context path: no
// WithQueue, expect a single "queue=default" DEBUG entry — confirms
// the framework's QueueFromContext default lands as the literal
// "default" string at the adapter boundary.
func TestRestate_DefaultQueueLogged(t *testing.T) {
	buf := captureDebug(t)
	rt := cwrestate.New()
	_, _ = rt.Run(context.Background(), "step", func(context.Context) ([]byte, error) { return nil, nil })

	if got := countOccurrences(buf.String(), `queue=default`); got != 1 {
		t.Errorf("DEBUG log for queue=default emitted %d times, want 1\nlog:\n%s", got, buf.String())
	}
}
