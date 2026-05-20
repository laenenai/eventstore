package inproc

import (
	"context"
	"log/slog"
	"sync"

	"github.com/laenenai/eventstore/cmdworkflow"
)

// Runtime is a non-durable WorkflowRuntime. Each Run executes fn
// synchronously and returns the result; RunAsync launches a
// goroutine and returns a Future; Spawn fires fn fire-and-forget on
// its own goroutine.
//
// Tests can opt into the optional Wait() helper which blocks until
// every outstanding Spawn'd workflow has finished — useful for
// asserting on Async-subscriber side effects without time-based
// sleeps. (RunAsync futures are awaited directly via Future.Wait.)
type Runtime struct {
	wg sync.WaitGroup

	// OnSpawnError is invoked when a Spawned workflow's fn returns
	// an error. The framework's Workflow already routes per-
	// subscriber exhaustion through DLQ / Compensate / Drop — by the
	// time a Spawn'd fn returns an error, those policies have been
	// applied. This hook is for test observability only.
	OnSpawnError func(name string, err error)

	// loggedQueues dedupes the one-time DEBUG log per unique queue
	// name observed in HandleCmd dispatch. inproc doesn't model
	// queues — it executes inline, no scheduling — so the log is
	// purely a traceability breadcrumb (ADR 0031). sync.Map keeps
	// the hot path lock-free.
	loggedQueues sync.Map
}

// New returns a fresh in-process runtime.
func New() *Runtime { return &Runtime{} }

// noteQueue emits a one-time DEBUG log per unique queue name observed
// in ctx. The inproc adapter has no scheduling model — every step runs
// inline — so the log is purely a traceability breadcrumb (ADR 0031):
// tests that exercise cmdworkflow.WithQueue can confirm the wiring
// without changing inproc semantics.
func (r *Runtime) noteQueue(ctx context.Context) {
	q := cmdworkflow.QueueFromContext(ctx)
	if _, loaded := r.loggedQueues.LoadOrStore(q, struct{}{}); loaded {
		return
	}
	slog.Debug(
		"cmdworkflow/inproc: queue routing is a no-op; steps execute inline",
		"queue", q,
	)
}

// Run executes fn synchronously and returns its result. No journal,
// no replay.
func (r *Runtime) Run(ctx context.Context, _ string,
	fn func(ctx context.Context) ([]byte, error)) ([]byte, error) {
	r.noteQueue(ctx)
	return fn(ctx)
}

// RunAsync launches fn on a goroutine and returns a Future. The
// goroutine runs concurrently with other RunAsync calls — multiple
// Sync subscribers fan out via this path.
//
// Each Future returns its result on Wait. Wait may be called exactly
// once per Future; subsequent calls return zero values.
func (r *Runtime) RunAsync(ctx context.Context, _ string,
	fn func(ctx context.Context) ([]byte, error)) cmdworkflow.Future {
	r.noteQueue(ctx)
	f := &future{done: make(chan struct{})}
	go func() {
		out, err := fn(ctx)
		f.out, f.err = out, err
		close(f.done)
	}()
	return f
}

// Spawn fires fn on a fresh goroutine and returns immediately. The
// returned error is always nil — failures inside fn are reported via
// OnSpawnError.
//
// The spawned fn receives a context detached from cancellation
// (context.WithoutCancel). Spawned workflows are independent by
// design; if the caller's request context is cancelled, the work the
// caller already scheduled continues to completion.
func (r *Runtime) Spawn(ctx context.Context, name string,
	fn func(ctx context.Context) error) error {
	r.noteQueue(ctx)
	r.wg.Add(1)
	detached := context.WithoutCancel(ctx)
	go func() {
		defer r.wg.Done()
		if err := fn(detached); err != nil && r.OnSpawnError != nil {
			r.OnSpawnError(name, err)
		}
	}()
	return nil
}

// Wait blocks until every outstanding Spawn'd workflow has finished.
// Test-only helper; production Restate / DBOS adapters do not expose
// an equivalent.
func (r *Runtime) Wait() { r.wg.Wait() }

// future is the inproc Future implementation: goroutine + channel.
type future struct {
	done chan struct{}
	out  []byte
	err  error
}

func (f *future) Wait() ([]byte, error) {
	<-f.done
	return f.out, f.err
}

// Compile-time check.
var _ cmdworkflow.WorkflowRuntime = (*Runtime)(nil)
