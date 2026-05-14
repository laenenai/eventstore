package cmdworkflow

import "context"

// WorkflowRuntime is the abstraction over a durable workflow engine
// (Restate, DBOS, inproc test runner, …). The framework requires
// three operations:
//
//   - Run executes a function as a single durable step. On replay,
//     if the journal already contains a result for name, the cached
//     result is returned without re-running fn.
//   - RunAsync starts fn as a journaled step and returns a Future.
//     The caller awaits via Future.Wait. Multiple RunAsync calls
//     from the same context execute concurrently — used by HandleCmd
//     to fan out Sync subscribers in parallel.
//   - Spawn fires fn as an independent child workflow. The parent
//     does not block; the child has its own retry/exhaustion state.
//
// Adapters MUST honor caller-supplied ctx for cancellation and
// deadlines. The framework wraps each Run call in a child context
// when a Subscriber.AttemptTimeout is set.
//
// Saga primitives (Sleep, Wait, Awakeable, Cancel) intentionally are
// NOT part of this interface — they land in a future ADR alongside
// the saga API.
type WorkflowRuntime interface {
	Run(ctx context.Context, name string,
		fn func(ctx context.Context) ([]byte, error)) ([]byte, error)

	RunAsync(ctx context.Context, name string,
		fn func(ctx context.Context) ([]byte, error)) Future

	Spawn(ctx context.Context, name string,
		fn func(ctx context.Context) error) error
}

// Future represents the eventual result of a RunAsync call. Adapters
// implement this against their native concurrent-step primitive
// (Restate's RunAsyncFuture, a goroutine-backed channel for inproc,
// DBOS's StartChildStep handle, etc.).
//
// Wait blocks until the underlying step completes and returns its
// result. Safe to call exactly once — implementations may not support
// multiple Wait calls.
type Future interface {
	Wait() ([]byte, error)
}
