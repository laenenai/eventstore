package dbos

import (
	"context"
	"errors"

	dbossdk "github.com/dbos-inc/dbos-transact-golang/dbos"

	"github.com/laenenai/eventstore/cmdworkflow"
)

// ctxKey is the typed key used to stash a DBOSContext inside a
// stdlib context.Context. Unexported so callers must go through
// WithContext.
type ctxKey struct{}

// WithContext stashes a DBOSContext inside parent so the framework's
// Runtime can extract it later when invoking dbos.RunAsStep /
// dbos.Go. Call this once at the entry point of every DBOS workflow
// handler before invoking cmdworkflow.Workflow.HandleCmd.
//
//	func (s *DBOSService) Create(ctx dbossdk.DBOSContext, cmd *invoicev1.Create) (*invoicev1.Invoice, error) {
//	    stdCtx := cwdbos.WithContext(context.Background(), ctx)
//	    return s.workflow.HandleCmd(stdCtx, sid, cmd)
//	}
//
// Returns a derived stdlib context. The DBOSContext embeds
// context.Context so Deadline / Done / Err / Value propagate via
// the standard inheritance — the wrapper exists only so the
// framework can recover the DBOSContext-specific interface for
// step / async-step / message-passing primitives.
func WithContext(parent context.Context, dctx dbossdk.DBOSContext) context.Context {
	return context.WithValue(parent, ctxKey{}, dctx)
}

// FromContext extracts the DBOSContext previously stashed by
// WithContext. Returns ok=false if the context was not wrapped —
// almost always means the caller forgot the wrap line at the top
// of the DBOS workflow handler.
func FromContext(ctx context.Context) (dbossdk.DBOSContext, bool) {
	dctx, ok := ctx.Value(ctxKey{}).(dbossdk.DBOSContext)
	return dctx, ok
}

// ErrNoDBOSContext is returned when Run, RunAsync, or Spawn is
// called with a context that hasn't been wrapped via WithContext.
var ErrNoDBOSContext = errors.New("cmdworkflow/dbos: no DBOSContext in context — call WithContext from your handler")

// Runtime implements cmdworkflow.WorkflowRuntime against the DBOS
// Go SDK. One per process; pass to cmdworkflow.New(...) as the
// third argument.
//
// All operations require a DBOSContext stashed in the caller's
// context.Context via WithContext. Calling Run/RunAsync/Spawn
// without a wrapped context returns ErrNoDBOSContext.
type Runtime struct{}

// New returns a Runtime. Stateless — the DBOSContext carries all the
// runtime state.
func New() *Runtime { return &Runtime{} }

// Run implements cmdworkflow.WorkflowRuntime.Run by delegating to
// dbos.RunAsStep[[]byte] with the supplied step name.
//
// Per the SDK contract, the fn closure receives a plain
// context.Context (not a DBOSContext) — DBOS forbids nested step
// calls from inside a step. The framework respects this: HandleCmd
// is structured so that nested Run only happens from the outer
// HandleCmd context, never from within a Run/RunAsync closure
// (ADR 0026 § 5c).
func (r *Runtime) Run(
	ctx context.Context,
	name string,
	fn func(ctx context.Context) ([]byte, error),
) ([]byte, error) {
	dctx, ok := FromContext(ctx)
	if !ok {
		return nil, ErrNoDBOSContext
	}
	return dbossdk.RunAsStep(dctx, func(stepCtx context.Context) ([]byte, error) {
		// Re-wrap the step's ctx so any callbacks that need stdlib
		// context.Value lookups still see the same chain. The
		// DBOSContext we stashed is NOT propagated into the step —
		// nested step calls aren't allowed.
		return fn(stepCtx)
	}, dbossdk.WithStepName(name))
}

// RunAsync implements cmdworkflow.WorkflowRuntime.RunAsync via
// dbos.Go which returns a channel of StepOutcome. The returned
// Future wraps the channel; Wait blocks until the outcome arrives.
//
// Multiple RunAsync calls from one workflow execute concurrently;
// DBOS assigns a deterministic step ID at call time so replay sees
// the same ordering.
func (r *Runtime) RunAsync(
	ctx context.Context,
	name string,
	fn func(ctx context.Context) ([]byte, error),
) cmdworkflow.Future {
	dctx, ok := FromContext(ctx)
	if !ok {
		return errFuture{err: ErrNoDBOSContext}
	}
	ch, err := dbossdk.Go(dctx, func(stepCtx context.Context) ([]byte, error) {
		return fn(stepCtx)
	}, dbossdk.WithStepName(name))
	if err != nil {
		return errFuture{err: err}
	}
	return &chanFuture{ch: ch}
}

// chanFuture wraps a dbos.Go StepOutcome channel in our Future
// interface.
type chanFuture struct {
	ch chan dbossdk.StepOutcome[[]byte]
}

func (f *chanFuture) Wait() ([]byte, error) {
	outcome := <-f.ch
	return outcome.Result, outcome.Err
}

// errFuture is a Future that always returns the same error on Wait.
// Used when RunAsync's preconditions fail; the framework collects
// futures and awaits them in a batch.
type errFuture struct{ err error }

func (f errFuture) Wait() ([]byte, error) { return nil, f.err }

// Spawn implements cmdworkflow.WorkflowRuntime.Spawn via an
// in-process goroutine. The spawned fn runs concurrently with the
// caller's workflow but is NOT journaled by DBOS at this layer.
// Durable Async fan-out is provided by the codegen-emitted
// AsyncDispatch handler in
// `adapters/cmdworkflow/dbos/gen/<aggregate>/...` (see
// `runtime=dbos` mode in `proto-gen/main.go`); the framework's
// `cmdworkflow.Workflow.SetAsyncSend` wires the codegen service's
// `sendAsync` to issue durable `dbos.RunWorkflow` calls.
//
// Why a goroutine and not dbos.RunWorkflow at THIS layer?
//
//   - dbos.RunWorkflow requires a registered workflow function
//     with a typed (Input, Output) signature, registered at
//     startup. The framework's Subscriber.Handle is a closure
//     dynamically dispatched by Filter — not registerable as a
//     named workflow at module init time.
//
//   - A "GenericStep" workflow that takes a closure-ID +
//     serialized envelope would let this layer call
//     dbos.RunWorkflow, but it adds a marshalling round-trip per
//     (subscriber, event) pair when the codegen path already
//     provides the durable route. Not worth the duplication.
//
// Async subscribers WITHOUT the codegen path are best-effort:
// they execute, but a process restart mid-Spawn loses in-flight
// work. Idempotency on env.EventID at the subscriber keeps
// duplicates safe under retry; the DLQ table receives permanent
// failures.
func (r *Runtime) Spawn(
	ctx context.Context,
	name string,
	fn func(ctx context.Context) error,
) error {
	if _, ok := FromContext(ctx); !ok {
		return ErrNoDBOSContext
	}
	detached := context.WithoutCancel(ctx)
	go func() {
		// Errors are framework-handled (DLQ / Compensate / Drop) by
		// the time fn returns. The goroutine just lets it run.
		_ = fn(detached)
	}()
	return nil
}

// Compile-time interface satisfaction check.
var _ cmdworkflow.WorkflowRuntime = (*Runtime)(nil)
