package dbos_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	dbossdk "github.com/dbos-inc/dbos-transact-golang/dbos"

	cwdbos "github.com/laenenai/eventstore/adapters/cmdworkflow/dbos"
	"github.com/laenenai/eventstore/cmdworkflow"
)

// Unit tests for ResolveQueue / QueueOption — the pure-Go state
// machine that decides which queue a dispatch routes to. No DBOS
// testcontainer needed: this is the resolution logic in isolation,
// independent of whether a workflow is actually running.
//
// The container-backed scenarios in dbos_test.go (build tag `dbos`)
// exercise the end-to-end path; this file covers the decision logic
// every container test relies on.

func captureLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	buf := &bytes.Buffer{}
	h := slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	prev := slog.Default()
	slog.SetDefault(slog.New(h))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return buf
}

// TestResolveQueue_NoQueuesDeclared covers the zero-config path:
// constructing a Runtime with no WithQueues option behaves as the
// pre-queue adapter did. Default resolves to nil queue handle (run
// immediately, no queue option). Non-default resolves to a WARN +
// default fallback.
func TestResolveQueue_NoQueuesDeclared(t *testing.T) {
	buf := captureLogs(t)
	rt := cwdbos.New()

	name, q, err := rt.ResolveQueue(context.Background())
	if err != nil {
		t.Fatalf("default resolution: %v", err)
	}
	if name != cmdworkflow.DefaultQueue {
		t.Errorf("name = %q, want %q", name, cmdworkflow.DefaultQueue)
	}
	if q != nil {
		t.Errorf("queue = %v, want nil (no declared queue)", q)
	}
	if got := strings.Count(buf.String(), "queue not declared"); got != 0 {
		t.Errorf("WARN for default emitted %d times, want 0 (no declared queues = no warning expected)", got)
	}

	// Non-default name on a zero-queue runtime degrades gracefully.
	ctx := cmdworkflow.WithQueue(context.Background(), "high")
	name, q, err = rt.ResolveQueue(ctx)
	if err != nil {
		t.Fatalf("unknown resolution: %v", err)
	}
	if name != cmdworkflow.DefaultQueue {
		t.Errorf("fallback name = %q, want %q", name, cmdworkflow.DefaultQueue)
	}
	if q != nil {
		t.Errorf("fallback queue = %v, want nil", q)
	}
	if got := strings.Count(buf.String(), `queue=high`); got != 1 {
		t.Errorf("WARN for queue=high emitted %d times, want 1\nlog:\n%s", got, buf.String())
	}
}

// TestResolveQueue_DeclaredQueueRoundTrips — adopter declares a named
// queue; resolution returns the *WorkflowQueue handle. The handle is
// the same pointer the adopter passed in, so callers can apply
// dbos.WithQueue(name) confidently.
//
// NOTE: We pass a placeholder *dbossdk.WorkflowQueue created via the
// struct literal rather than dbossdk.NewWorkflowQueue, since the
// latter requires a real DBOSContext. ResolveQueue only inspects
// presence in the map — the SDK calls happen later, in container
// tests.
func TestResolveQueue_DeclaredQueueRoundTrips(t *testing.T) {
	highQ := &dbossdk.WorkflowQueue{Name: "high"}
	defaultQ := &dbossdk.WorkflowQueue{Name: "default"}
	rt := cwdbos.New(cwdbos.WithQueues(map[string]*dbossdk.WorkflowQueue{
		"high":    highQ,
		"default": defaultQ,
	}))

	// Default name resolves to the declared default queue.
	name, q, err := rt.ResolveQueue(context.Background())
	if err != nil {
		t.Fatalf("default: %v", err)
	}
	if name != "default" || q != defaultQ {
		t.Errorf("default resolution: name=%q q=%v, want default", name, q)
	}

	// Explicit queue name resolves to its handle.
	ctx := cmdworkflow.WithQueue(context.Background(), "high")
	name, q, err = rt.ResolveQueue(ctx)
	if err != nil {
		t.Fatalf("high: %v", err)
	}
	if name != "high" || q != highQ {
		t.Errorf("high resolution: name=%q q=%v, want high", name, q)
	}
}

// TestResolveQueue_UnknownFallsBackToDeclaredDefault — when the
// adopter declares "default" + others, an unknown name falls back to
// the declared default queue (not nil). Verifies the explicit-default-
// override path: adopters who want bounded default-queue concurrency
// declare it, and unknown-name fallback then inherits those bounds.
func TestResolveQueue_UnknownFallsBackToDeclaredDefault(t *testing.T) {
	buf := captureLogs(t)
	defaultQ := &dbossdk.WorkflowQueue{Name: "default"}
	rt := cwdbos.New(cwdbos.WithQueues(map[string]*dbossdk.WorkflowQueue{
		"default": defaultQ,
	}))

	ctx := cmdworkflow.WithQueue(context.Background(), "ghost")
	name, q, err := rt.ResolveQueue(ctx)
	if err != nil {
		t.Fatalf("ghost: %v", err)
	}
	if name != cmdworkflow.DefaultQueue || q != defaultQ {
		t.Errorf("ghost resolution: name=%q q=%v, want default→declared", name, q)
	}
	if got := strings.Count(buf.String(), `queue=ghost`); got != 1 {
		t.Errorf("WARN for queue=ghost emitted %d times, want 1\nlog:\n%s", got, buf.String())
	}

	// Same unknown name a second time — no additional WARN.
	_, _, _ = rt.ResolveQueue(ctx)
	if got := strings.Count(buf.String(), `queue=ghost`); got != 1 {
		t.Errorf("WARN for queue=ghost re-emitted; want 1 total, got %d", got)
	}

	// A different unknown name emits its own WARN.
	other := cmdworkflow.WithQueue(context.Background(), "spectre")
	_, _, _ = rt.ResolveQueue(other)
	if got := strings.Count(buf.String(), `queue=spectre`); got != 1 {
		t.Errorf("WARN for queue=spectre emitted %d times, want 1", got)
	}
}

// TestResolveQueue_StrictMode — strict mode returns
// ErrUnknownQueue rather than logging + degrading. HandleCmd's caller
// can then act on the configuration error directly.
func TestResolveQueue_StrictMode(t *testing.T) {
	highQ := &dbossdk.WorkflowQueue{Name: "high"}
	rt := cwdbos.New(
		cwdbos.WithQueues(map[string]*dbossdk.WorkflowQueue{"high": highQ}),
		cwdbos.WithStrictQueues(true),
	)

	ctx := cmdworkflow.WithQueue(context.Background(), "ghost")
	_, _, err := rt.ResolveQueue(ctx)
	if !errors.Is(err, cwdbos.ErrUnknownQueue) {
		t.Errorf("strict ghost: err=%v, want ErrUnknownQueue", err)
	}

	// Default name with no declared default returns nil queue, no
	// error — strict applies to non-default unknowns, not to the
	// implicit "no declared default = run immediately" path.
	name, q, err := rt.ResolveQueue(context.Background())
	if err != nil {
		t.Errorf("strict default w/o declared default: err=%v, want nil", err)
	}
	if name != cmdworkflow.DefaultQueue || q != nil {
		t.Errorf("strict default w/o declared default: name=%q q=%v, want default/nil", name, q)
	}
}

// TestQueueOption_StrictModePropagatesError — QueueOption is the
// thin wrapper that codegen sendAsync uses. Verifies strict-mode
// errors propagate through it (rather than being swallowed).
func TestQueueOption_StrictModePropagatesError(t *testing.T) {
	rt := cwdbos.New(
		cwdbos.WithQueues(map[string]*dbossdk.WorkflowQueue{"high": {Name: "high"}}),
		cwdbos.WithStrictQueues(true),
	)
	ctx := cmdworkflow.WithQueue(context.Background(), "ghost")
	opt, err := rt.QueueOption(ctx)
	if !errors.Is(err, cwdbos.ErrUnknownQueue) {
		t.Errorf("QueueOption strict: err=%v, want ErrUnknownQueue", err)
	}
	if opt != nil {
		t.Errorf("QueueOption strict: opt=%v, want nil", opt)
	}
}

// TestQueueOption_NilRuntime — sendAsync codegen type-asserts the
// framework's WorkflowRuntime against *cwdbos.Runtime; tests using
// the inproc runtime won't pass the assertion. Calling QueueOption on
// a nil receiver must safely return (nil, nil) so the codegen
// compiles without a defensive nil check.
func TestQueueOption_NilRuntime(t *testing.T) {
	var rt *cwdbos.Runtime // nil
	opt, err := rt.QueueOption(context.Background())
	if err != nil {
		t.Errorf("nil runtime QueueOption: err=%v, want nil", err)
	}
	if opt != nil {
		t.Errorf("nil runtime QueueOption: opt=%v, want nil", opt)
	}
}
