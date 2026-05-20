package cmdworkflow_test

import (
	"context"
	"testing"

	"github.com/laenenai/eventstore/cmdworkflow"
)

// TestQueue_RoundTrip exercises the WithQueue/QueueFromContext
// contract. The invariant under test is that QueueFromContext never
// returns "" — every code path (no value, empty value, empty-string
// override) resolves to DefaultQueue. Adapters rely on this to skip a
// "" check when looking up a Queue in a map.
func TestQueue_RoundTrip(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		ctxFn  func() context.Context
		expect string
	}{
		{
			name:   "explicit name round-trips",
			ctxFn:  func() context.Context { return cmdworkflow.WithQueue(context.Background(), "high-priority") },
			expect: "high-priority",
		},
		{
			name:   "empty string resolves to default",
			ctxFn:  func() context.Context { return cmdworkflow.WithQueue(context.Background(), "") },
			expect: cmdworkflow.DefaultQueue,
		},
		{
			name:   "bare context resolves to default",
			ctxFn:  context.Background,
			expect: cmdworkflow.DefaultQueue,
		},
		{
			name: "nested WithQueue overrides outer",
			ctxFn: func() context.Context {
				outer := cmdworkflow.WithQueue(context.Background(), "outer")
				return cmdworkflow.WithQueue(outer, "inner")
			},
			expect: "inner",
		},
		{
			name: "nested empty string falls back to default, not outer",
			ctxFn: func() context.Context {
				// Documented behavior: empty string is the same defensive
				// resolution at every layer — adopters shouldn't get
				// surprise inheritance when they explicitly pass "".
				outer := cmdworkflow.WithQueue(context.Background(), "outer")
				return cmdworkflow.WithQueue(outer, "")
			},
			expect: cmdworkflow.DefaultQueue,
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := cmdworkflow.QueueFromContext(tc.ctxFn())
			if got != tc.expect {
				t.Errorf("QueueFromContext = %q, want %q", got, tc.expect)
			}
		})
	}
}

// TestQueue_DefaultConstantValue locks the literal string. A renamed
// constant would break adopter config maps that key on the literal
// "default" — this guards against the rename slipping through.
func TestQueue_DefaultConstantValue(t *testing.T) {
	t.Parallel()
	if cmdworkflow.DefaultQueue != "default" {
		t.Fatalf("DefaultQueue = %q, want %q", cmdworkflow.DefaultQueue, "default")
	}
}

// TestQueue_NilContext — defensive: a nil context shouldn't panic.
// (No exported callsite should pass nil, but QueueFromContext is the
// adapter-side lookup and bugs upstream shouldn't crash dispatch.)
func TestQueue_NilContext(t *testing.T) {
	t.Parallel()
	//nolint:staticcheck // intentional nil ctx — guarding the adapter path
	got := cmdworkflow.QueueFromContext(nil)
	if got != cmdworkflow.DefaultQueue {
		t.Errorf("QueueFromContext(nil) = %q, want %q", got, cmdworkflow.DefaultQueue)
	}
}
