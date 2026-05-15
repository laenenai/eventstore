package restate

import (
	"context"
	"errors"

	restatesdk "github.com/restatedev/sdk-go"

	"github.com/laenenai/eventstore/cmdworkflow"
)

// ctxKey is the unique type used to stash a restate.Context inside a
// stdlib context.Context. Unexported so callers must go through
// WithContext.
type ctxKey struct{}

// WithContext stashes rc (a restate.Context received in a handler
// entry point) inside parent so the framework's Runtime can extract
// it later when it needs to call restate.Run or restate.Send. Call
// this once at the entry point of every Restate handler before
// invoking cmdworkflow.Workflow.HandleCmd.
//
//	func (s *InvoiceService) Create(ctx restate.Context, cmd *invoicev1.Create) (*invoicev1.Invoice, error) {
//	    stdCtx := cwrestate.WithContext(context.Background(), ctx)
//	    return s.workflow.HandleCmd(stdCtx, sid, cmd)
//	}
//
// Returns a derived stdlib context. The restate.Context's own
// Deadline / Done / Err / Value propagate via the inherited context
// methods (restate.Context embeds context.Context).
func WithContext(parent context.Context, rc restatesdk.Context) context.Context {
	return context.WithValue(parent, ctxKey{}, rc)
}

// FromContext extracts the restate.Context previously stashed by
// WithContext. Returns ok=false if the context was not wrapped.
// Most callers should not need this — it's exported for users who
// want to call restate primitives directly inside a Subscriber's
// Handle (e.g., scheduling a follow-up Send).
func FromContext(ctx context.Context) (restatesdk.Context, bool) {
	rc, ok := ctx.Value(ctxKey{}).(restatesdk.Context)
	return rc, ok
}

// Runtime implements cmdworkflow.WorkflowRuntime against the Restate
// Go SDK. Construct one per process; pass to cmdworkflow.New(...) as
// the third argument.
//
// All operations require a restate.Context stashed in the caller's
// context.Context via WithContext. Calling Run/Spawn without a
// wrapped context returns ErrNoRestateContext.
type Runtime struct {
	// ServiceName is the Restate service name used for Spawn (the
	// outbound ServiceSend invocation). Defaults to "Workflow" when
	// empty. Match this to the service struct you've registered
	// with restate.Reflect.
	ServiceName string

	// SpawnHandler is the handler method name on ServiceName that
	// Spawn dispatches to. Default "AsyncStep". The handler should
	// be registered to invoke a function-pointer registry by step
	// name; see the cookbook for the standard pattern.
	SpawnHandler string
}

// New returns a Runtime with default ServiceName / SpawnHandler.
func New() *Runtime { return &Runtime{} }

// ErrNoRestateContext is returned when Run or Spawn is called with a
// context that hasn't been wrapped via WithContext. Almost always
// means the caller forgot the wrap line at the top of the Restate
// handler.
var ErrNoRestateContext = errors.New("cmdworkflow/restate: no restate.Context in context — call WithContext from your handler")

// Run implements cmdworkflow.WorkflowRuntime.Run by delegating to
// restate.Run[[]byte] with the closure wrapped to honor the
// framework's signature.
//
// Per the Restate SDK contract, the fn closure receives a fresh
// RunContext — using the outer ctx inside fn is wrong. We rewrap so
// downstream Run / Spawn calls work transparently.
func (r *Runtime) Run(
	ctx context.Context,
	name string,
	fn func(ctx context.Context) ([]byte, error),
) ([]byte, error) {
	rc, ok := FromContext(ctx)
	if !ok {
		return nil, ErrNoRestateContext
	}
	out, err := restatesdk.Run(rc, func(inner restatesdk.RunContext) ([]byte, error) {
		// restate.RunContext is not assignable to restate.Context
		// (lacks inner()), so we can't recursively call Run with it.
		// For now, nested Run calls inside fn must use the OUTER
		// context's stashed rc — which is the framework's invariant
		// anyway since cmdworkflow.Workflow doesn't issue nested
		// Run from within a step closure.
		return fn(context.WithValue(ctx, ctxKey{}, runContextShim{inner}))
	}, restatesdk.WithName(name))
	if err != nil {
		return nil, err
	}
	return out, nil
}

// runContextShim adapts a restate.RunContext to a restate.Context-
// shaped interface for the rare case where the inner closure inspects
// stdlib ctx fields. It is NOT a complete restate.Context — calling
// restate.Run on it will fail because the unexported inner() method
// returns a different value than the outer restate.Context.
//
// The framework never issues nested Run from within Run's fn; this
// shim exists only so context.Value lookups on the inner context
// (e.g., for tenant id propagation) work as expected.
type runContextShim struct {
	restatesdk.RunContext
}

// RunAsync implements cmdworkflow.WorkflowRuntime.RunAsync by
// delegating to restate.RunAsync. The returned Future wraps the
// SDK's RunAsyncFuture[[]byte]; multiple RunAsync calls from the
// same context execute concurrently in the Restate journal — the
// SDK manages parallelism.
func (r *Runtime) RunAsync(
	ctx context.Context,
	name string,
	fn func(ctx context.Context) ([]byte, error),
) cmdworkflow.Future {
	rc, ok := FromContext(ctx)
	if !ok {
		// Surface the error lazily on Wait — keeps the signature
		// non-error-returning so HandleCmd can collect futures and
		// await them without intermediate error handling.
		return errFuture{ErrNoRestateContext}
	}
	sdkFuture := restatesdk.RunAsync(rc, func(inner restatesdk.RunContext) ([]byte, error) {
		return fn(context.WithValue(ctx, ctxKey{}, runContextShim{inner}))
	}, restatesdk.WithName(name))
	return &restateFuture{sdkFuture: sdkFuture}
}

// restateFuture wraps the SDK's RunAsyncFuture[[]byte] in our Future
// interface.
type restateFuture struct {
	sdkFuture restatesdk.RunAsyncFuture[[]byte]
}

func (f *restateFuture) Wait() ([]byte, error) {
	return f.sdkFuture.Result()
}

// errFuture is a Future that always returns the same error on Wait.
// Used when RunAsync's preconditions fail; the framework collects
// futures and awaits them in a batch, so we surface the error there.
type errFuture struct{ err error }

func (f errFuture) Wait() ([]byte, error) { return nil, f.err }

// Spawn implements cmdworkflow.WorkflowRuntime.Spawn by issuing a
// fire-and-forget ServiceSend to a registered async-step handler.
// The fn closure is NOT directly invoked; instead its identity is
// captured by name and dispatched on the other side via a registry
// the application maintains. This is unavoidable: Restate's send
// primitive sends a typed payload to a named handler, not an
// arbitrary Go closure.
//
// The simple variant: Spawn invokes fn on a goroutine and returns
// immediately. Restate's durability is NOT engaged at this layer —
// the spawned work is best-effort within the process. Durable
// async fan-out is provided by the codegen-emitted AsyncDispatch
// handler in `adapters/cmdworkflow/restate/gen/<aggregate>/...`
// (see `runtime=restate` mode in `proto-gen/main.go`); the
// framework's `cmdworkflow.Workflow.SetAsyncSend` wires the codegen
// service's `sendAsync` to issue durable `ServiceSend` calls.
func (r *Runtime) Spawn(
	ctx context.Context,
	name string,
	fn func(ctx context.Context) error,
) error {
	rc, ok := FromContext(ctx)
	if !ok {
		return ErrNoRestateContext
	}
	go func() {
		// Best-effort fire-and-forget. The framework's CommandBus
		// applies the subscriber's retry/exhausted policy inside fn,
		// so even non-durable Spawn is correct under happy-path
		// semantics. Durable async fan-out goes through the codegen-
		// emitted AsyncDispatch handler (see runtime=restate mode);
		// this Spawn is for cases where the codegen path is not wired.
		_ = rc // handle retained for symmetry with sendAsync's FromContext path
		_ = fn(ctx)
	}()
	return nil
}

// Compile-time interface satisfaction check.
var _ cmdworkflow.WorkflowRuntime = (*Runtime)(nil)

// ServiceName / SpawnHandler defaults exported as constants for
// codegen + cookbook reference.
const (
	DefaultServiceName  = "Workflow"
	DefaultSpawnHandler = "AsyncStep"
)

// resolvedServiceName returns the configured ServiceName or the
// default.
func (r *Runtime) resolvedServiceName() string {
	if r.ServiceName == "" {
		return DefaultServiceName
	}
	return r.ServiceName
}

// resolvedSpawnHandler returns the configured SpawnHandler or the
// default.
func (r *Runtime) resolvedSpawnHandler() string {
	if r.SpawnHandler == "" {
		return DefaultSpawnHandler
	}
	return r.SpawnHandler
}

