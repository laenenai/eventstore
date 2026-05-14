package cmdworkflow

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/laenenai/eventstore/aggregate"
	"github.com/laenenai/eventstore/es"
)

// CommandRunner is the minimal aggregate-runtime surface the bus
// needs: append a command and load the post-Decide state. Concretely
// satisfied by *aggregate.Runtime[S, C, E] for any E — the bus
// deliberately does not surface E, since it never decodes events to
// typed form (subscribers receive opaque es.Envelope and decode as
// they choose).
type AggregateRunner[S, C any] interface {
	Handle(ctx context.Context, sid es.StreamID, cmd C, opts ...aggregate.HandleOption) (es.AppendResult, error)
	Load(ctx context.Context, sid es.StreamID) (S, uint64, error)
}

// Workflow is the generic command handler + subscriber registry
// described in ADR 0025. One bus per aggregate (S, C).
//
// Wiring:
//
//	bus := cmdworkflow.New[*invoicev1.Invoice, invoicev1.Command](
//	    runtime,    // aggregate.Runtime[S, C, E] — E hidden by interface
//	    store,      // es.Store (also reads state_cache + ReadStream)
//	    wfRuntime,  // WorkflowRuntime adapter (inproc, restate, …)
//	)
//	bus.Register(cmdworkflow.Subscriber[Command]{...})
//	state, err := bus.HandleCmd(ctx, streamID, cmd)
//
// HandleCmd contract:
//   - Returns the aggregate's own state S after Append (not a view
//     model — ADR 0025 § "S is the aggregate's state, not a view").
//   - Fans out to every Subscriber whose Filter matches each new
//     envelope. Sync subscribers settle before HandleCmd returns;
//     Async subscribers are scheduled and HandleCmd returns
//     immediately for those.
//   - Compensation runs on a fresh, non-cancellable context so a
//     caller timeout cannot leave the aggregate in a half-rolled-back
//     state (ADR 0025 § decision 8).
type Workflow[S, C any] struct {
	runner      AggregateRunner[S, C]
	store       es.Store
	wf          WorkflowRuntime
	subscribers []Subscriber[C]
	dlq         SubscriberDLQWriter

	// asyncSend, when set by the codegen-emitted Service constructor,
	// replaces the goroutine-based Spawn for durable Async fan-out.
	// Each Async subscriber × event becomes its own runtime workflow
	// invocation with a deterministic workflowID for dedup.
	asyncSend AsyncSend
}

// New constructs a CommandBus. The dlq parameter is wired separately
// via WithDLQ.
func New[S, C any](
	runner AggregateRunner[S, C],
	store es.Store,
	wf WorkflowRuntime,
) *Workflow[S, C] {
	return &Workflow[S, C]{
		runner: runner,
		store:  store,
		wf:     wf,
	}
}

// WithDLQ wires a SubscriberDLQWriter for OnExhausted=DLQ subscribers.
// Returns the bus for chaining.
func (b *Workflow[S, C]) WithDLQ(dlq SubscriberDLQWriter) *Workflow[S, C] {
	b.dlq = dlq
	return b
}

// Register appends a subscriber to the bus. Order matters only for
// deterministic step naming during replay; subscribers do not see
// one another's effects.
func (b *Workflow[S, C]) Register(s Subscriber[C]) {
	if s.Name == "" {
		panic("cmdworkflow: Subscriber.Name is required")
	}
	if s.Handle == nil {
		panic("cmdworkflow: Subscriber.Handle is required")
	}
	if s.OnExhausted == Compensate && s.Compensate == nil {
		panic("cmdworkflow: Subscriber " + s.Name + ": OnExhausted=Compensate requires Compensate fn")
	}
	if s.Mode == Async && s.OnExhausted == Compensate {
		// Saga-style compensation only makes sense when the original
		// caller is waiting on the result. Async subscribers fire
		// fire-and-forget; "command succeeded but the saga rollback
		// fired asynchronously" is a confusing semantic with no good
		// recovery story for the caller. Use Sync+Compensate for
		// saga steps; DLQ or Drop for async failures.
		panic("cmdworkflow: Subscriber " + s.Name + ": Async + Compensate is disallowed (saga compensation requires Sync)")
	}
	for _, existing := range b.subscribers {
		if existing.Name == s.Name {
			panic("cmdworkflow: duplicate Subscriber.Name: " + s.Name)
		}
	}
	b.subscribers = append(b.subscribers, s)
}

// AsyncPayload is the wire shape carried by durable Async fan-out.
// Codegen-emitted Service.AsyncDispatch handlers receive it as their
// input; Service.sendAsync constructs it when invoking the runtime's
// send primitive. Exported for codegen consumption — application
// code typically doesn't touch it.
type AsyncPayload struct {
	SubscriberName string `json:"subscriberName"`
	EnvBytes       []byte `json:"envBytes"`
}

// AsyncSend is the function shape an adapter's Service registers via
// SetAsyncSend to enable durable Async fan-out. When set, spawnAsync
// invokes it instead of using the goroutine-based Spawn — the
// adapter's Service handles the actual workflow invocation
// (ServiceSend for Restate, RunWorkflow for DBOS) targeting its own
// pre-registered AsyncDispatch method.
//
// The workflowID is the deterministic dedup key:
// "<streamType>:<subscriberName>:<eventID>". Two Spawn calls with
// the same id produce one child workflow.
type AsyncSend func(ctx context.Context, subscriberName string, envBytes []byte, workflowID string) error

// SetAsyncSend wires a durable Async fan-out function. Called by
// the codegen-emitted Service constructors (NewRestateService,
// NewDBOSService). If unset, spawnAsync falls back to the goroutine-
// based Spawn (Phase 2a inproc behavior).
func (b *Workflow[S, C]) SetAsyncSend(fn AsyncSend) {
	b.asyncSend = fn
}

// DispatchAsync is invoked by the codegen-emitted Service's
// AsyncDispatch workflow when a child invocation fires. Looks up the
// subscriber by name, decodes the envelope, runs the same
// retry+exhausted policy as Sync.
//
// Returns nil on success or successful policy application
// (DLQ-written, Drop, etc.). Returns an error only on framework
// failures (subscriber not found, encoding error). Subscriber
// Handle errors are absorbed by the retry+policy loop and never
// propagate up — the parent runtime should consider the child
// workflow successful regardless of subscriber Handle outcome.
func (b *Workflow[S, C]) DispatchAsync(ctx context.Context, subscriberName string, envBytes []byte) error {
	var sub Subscriber[C]
	found := false
	for _, s := range b.subscribers {
		if s.Name == subscriberName {
			sub = s
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("cmdworkflow: DispatchAsync: subscriber %q not registered", subscriberName)
	}

	envs, err := decodeEnvelopes(envBytes)
	if err != nil {
		return fmt.Errorf("cmdworkflow: DispatchAsync: decode envelope: %w", err)
	}
	if len(envs) != 1 {
		return fmt.Errorf("cmdworkflow: DispatchAsync: expected 1 envelope, got %d", len(envs))
	}
	env := envs[0]

	sid := env.StreamID
	if err := b.runRetries(ctx, env, sub); err != nil {
		return b.onExhausted(ctx, sid, env, sub, err)
	}
	return nil
}

// With is the fluent counterpart to Register. Returns the workflow so
// subscribers + DLQ can be wired in one expression:
//
//	wf := cmdworkflow.New[*Invoice, Command](rt, store, inproc.New()).
//	    WithDLQ(store).
//	    With(read.Subscriber(), search.Subscriber(), credit.Subscriber())
//
// Each variadic argument goes through the same validation as Register
// (panics on empty name, missing Handle, Compensate without fn,
// duplicate names).
func (b *Workflow[S, C]) With(subs ...Subscriber[C]) *Workflow[S, C] {
	for _, s := range subs {
		b.Register(s)
	}
	return b
}

// HandleCmd is the entry point. Appends events through aggregate.Runtime,
// reads them back with assigned global_position, fans out to matched
// subscribers per the Mode / MaxRetries / OnExhausted policy, and
// returns the aggregate's post-decide state.
//
// A no-op command (Decider returns zero events) returns the current
// state and nil error — no subscribers are notified.
func (b *Workflow[S, C]) HandleCmd(
	ctx context.Context,
	sid es.StreamID,
	cmd C,
	opts ...HandleCmdOption,
) (S, error) {
	var zero S

	cfg := handleCmdConfig{}
	for _, opt := range opts {
		opt(&cfg)
	}

	aggOpts := cfg.aggOpts
	if cfg.idempotencyKey != "" {
		// Layer 2 idempotency: deterministic command_id from key.
		// The workflow runtime adapter handles layer 1 separately
		// via the same key as the invocation name.
		aggOpts = append(aggOpts, aggregate.WithCommandID(
			deriveCommandIDFromKey(cfg.idempotencyKey)))
	}

	// Step 1: durable Append. The journaled result includes the
	// version range we need to read back.
	appendResult, err := b.appendStep(ctx, sid, cmd, aggOpts...)
	if err != nil {
		return zero, err
	}

	// No-op command (Decider returned no events): nothing to fan out.
	if appendResult.StartVersion == 0 && appendResult.EndVersion == 0 {
		state, _, err := b.runner.Load(ctx, sid)
		return state, err
	}

	// Step 2: read back the just-appended envelopes for fan-out.
	envs, err := b.readEnvelopesStep(ctx, sid, appendResult)
	if err != nil {
		return zero, err
	}

	// Step 3: fan out per event. Sync subscribers run concurrently
	// via RunAsync (retries inside one journal entry). HandleCmd
	// awaits all futures, then applies each subscriber's OnExhausted
	// policy from the OUTER context. The policy application MUST
	// happen here (not inside the RunAsync fn) because Restate forbids
	// nested Run from a RunContext closure — DLQ insert and
	// Compensate recursion both issue Run calls. Async subscribers
	// fire via Spawn into independent child workflows.
	for _, env := range envs {
		var (
			syncFutures []Future
			syncSubs    []Subscriber[C]
		)
		for _, sub := range b.subscribers {
			if !sub.Filter.Matches(env) {
				continue
			}
			if sub.Mode == Async {
				if err := b.spawnAsync(ctx, sid, env, sub); err != nil {
					return zero, err
				}
				continue
			}
			syncSubs = append(syncSubs, sub)
			syncFutures = append(syncFutures, b.runSyncSubscriber(ctx, sid, env, sub))
		}
		// Wait for every Sync subscriber, collect (subscriber,
		// exhaustedErr) pairs. Don't short-circuit — every
		// subscriber's policy runs even if a sibling failed.
		//
		// Future.Wait returns:
		//   - (nil,    nil)    on success (fn returned nil/nil)
		//   - (bytes,  nil)    on exhausted (fn returned bytes, nil)
		//   - (any,    err)    on Restate infrastructure error
		type exhausted struct {
			sub Subscriber[C]
			err error
		}
		var toApply []exhausted
		for i, f := range syncFutures {
			raw, err := f.Wait()
			if err != nil {
				return zero, fmt.Errorf("cmdworkflow: subscriber %s: %w", syncSubs[i].Name, err)
			}
			if len(raw) > 0 {
				toApply = append(toApply, exhausted{
					sub: syncSubs[i],
					err: errors.New(string(raw)),
				})
			}
		}
		// Apply OnExhausted policy from the outer context. All Run /
		// HandleCmd calls inside onExhausted use the parent ctx
		// directly (real restate.Context, not a RunContext shim).
		for _, e := range toApply {
			if err := b.onExhausted(ctx, sid, env, e.sub, e.err); err != nil {
				return zero, err
			}
		}
	}

	// Step 4: return post-Decide state. Loads from state_cache when
	// the runtime has a StateCodec (read-your-writes); falls back to
	// full event replay otherwise.
	state, _, err := b.runner.Load(ctx, sid)
	return state, err
}

// stepPrefixKey is the typed context-value key for namespacing step
// names during nested HandleCmd calls (Compensate recursion). The
// recursive HandleCmd issues "append", "read-envelopes" — but the
// outer HandleCmd has already done so. Same step name = journal
// collision in Restate. Prefix disambiguates.
type stepPrefixKey struct{}

func withStepPrefix(ctx context.Context, prefix string) context.Context {
	return context.WithValue(ctx, stepPrefixKey{}, prefix)
}

func stepPrefix(ctx context.Context) string {
	v, _ := ctx.Value(stepPrefixKey{}).(string)
	return v
}

// appendStep wraps the aggregate.Runtime.Handle call in a durable
// WorkflowRuntime step. On workflow replay, the journal entry for
// "append" already exists with the cached AppendResult — no second
// commit happens, version ranges stay consistent.
func (b *Workflow[S, C]) appendStep(
	ctx context.Context,
	sid es.StreamID,
	cmd C,
	opts ...aggregate.HandleOption,
) (es.AppendResult, error) {
	raw, err := b.wf.Run(ctx, stepPrefix(ctx)+"append", func(ctx context.Context) ([]byte, error) {
		ar, herr := b.runner.Handle(ctx, sid, cmd, opts...)
		if herr != nil {
			return nil, herr
		}
		return encodeAppendResult(ar), nil
	})
	if err != nil {
		return es.AppendResult{}, err
	}
	return decodeAppendResult(raw)
}

// readEnvelopesStep fetches the envelopes appended in step 1, with
// the global_position values assigned by the DB at commit. The read
// is itself journaled — on replay we don't hit the DB again.
func (b *Workflow[S, C]) readEnvelopesStep(
	ctx context.Context,
	sid es.StreamID,
	result es.AppendResult,
) ([]es.Envelope, error) {
	raw, err := b.wf.Run(ctx, stepPrefix(ctx)+"read-envelopes", func(ctx context.Context) ([]byte, error) {
		envs, rerr := b.store.ReadStream(ctx, sid, result.StartVersion-1)
		if rerr != nil {
			return nil, rerr
		}
		// Take only the envelopes we just produced (ReadStream may
		// return more if other writers raced — but they can't, OCC
		// prevents that on a contended stream; defensive slice anyway).
		want := int(result.EndVersion - result.StartVersion + 1)
		if len(envs) > want {
			envs = envs[:want]
		}
		return encodeEnvelopes(envs), nil
	})
	if err != nil {
		return nil, err
	}
	return decodeEnvelopes(raw)
}

// spawnAsync dispatches an Async subscriber. Two paths:
//
//   - If asyncSend is set (the codegen-emitted Service has wired it
//     in via SetAsyncSend), invoke the durable child-workflow path:
//     encode the envelope, generate a deterministic workflowID, and
//     hand off to the runtime's ServiceSend / RunWorkflow targeting
//     the Service's pre-registered AsyncDispatch method.
//
//   - Otherwise, fall back to the goroutine-based Spawn (Phase 2a
//     inproc behavior). Not durable, but correct for tests and
//     single-process apps.
//
// The workflowID format is "<streamType>:<subscriberName>:<eventID>"
// — deterministic across replays, prefixed with the aggregate so
// operator dashboards group related child workflows visually.
func (b *Workflow[S, C]) spawnAsync(
	ctx context.Context,
	sid es.StreamID,
	env es.Envelope,
	sub Subscriber[C],
) error {
	if b.asyncSend != nil {
		envBytes := encodeEnvelopes([]es.Envelope{env})
		workflowID := fmt.Sprintf("%s:%s:%s", env.StreamID.Type, sub.Name, env.EventID.String())
		return b.asyncSend(ctx, sub.Name, envBytes, workflowID)
	}

	// Fallback: goroutine-based fire-and-forget. Same retry+policy
	// logic as Sync, just running on a goroutine.
	spawnName := sub.Name + ":" + env.EventID.String()
	return b.wf.Spawn(ctx, spawnName, func(ctx context.Context) error {
		if err := b.runRetries(ctx, env, sub); err != nil {
			return b.onExhausted(ctx, sid, env, sub, err)
		}
		return nil
	})
}

// runSyncSubscriber dispatches a Sync subscriber as one journaled
// async step (RunAsync). Retries happen INSIDE the step's fn so the
// journal sees exactly one entry per (subscriber, event) regardless
// of attempt count.
//
// IMPORTANT: the fn ALWAYS returns nil error to Restate, even on
// exhaustion. Restate treats a non-nil fn error as step failure and
// retries the whole invocation — which is wrong for our retry budget
// semantics (we already retried inside the fn).
//
// The "exhausted, here's the lastErr" signal travels through the
// bytes return: nil bytes = success, non-empty bytes = exhausted
// error message. HandleCmd's outer loop decodes this and applies the
// OnExhausted policy. The actual DLQ insert / Compensate recursion
// happens from HandleCmd's main context (real restate.Context, not
// the RunContext shim).
func (b *Workflow[S, C]) runSyncSubscriber(
	ctx context.Context,
	sid es.StreamID,
	env es.Envelope,
	sub Subscriber[C],
) Future {
	stepName := stepPrefix(ctx) + fmt.Sprintf("%s:%s", sub.Name, env.EventID.String())
	return b.wf.RunAsync(ctx, stepName, func(ctx context.Context) ([]byte, error) {
		if err := b.runRetries(ctx, env, sub); err != nil {
			return []byte(err.Error()), nil
		}
		return nil, nil
	})
}

// runRetries runs the subscriber's Handle inside the retry budget.
// Returns nil on success, the last error on exhaustion. Does NOT
// apply OnExhausted policy — that's the caller's responsibility.
func (b *Workflow[S, C]) runRetries(
	ctx context.Context,
	env es.Envelope,
	sub Subscriber[C],
) error {
	maxAttempts := sub.MaxRetries + 1
	infinite := sub.MaxRetries < 0

	var lastErr error
	for attempt := 1; infinite || attempt <= maxAttempts; attempt++ {
		callCtx := ctx
		if sub.AttemptTimeout > 0 {
			var cancel context.CancelFunc
			callCtx, cancel = context.WithTimeout(ctx, sub.AttemptTimeout)
			err := sub.Handle(callCtx, env)
			cancel()
			if err == nil {
				return nil
			}
			lastErr = err
			continue
		}
		if err := sub.Handle(callCtx, env); err == nil {
			return nil
		} else {
			lastErr = err
		}
	}
	return lastErr
}

// onExhausted applies the subscriber's exhausted-policy. Compensate
// runs on a context detached from the caller's deadline (ADR 0025
// decision 8).
func (b *Workflow[S, C]) onExhausted(
	ctx context.Context,
	sid es.StreamID,
	env es.Envelope,
	sub Subscriber[C],
	lastErr error,
) error {
	switch sub.OnExhausted {
	case Drop:
		return nil

	case DLQ:
		if b.dlq == nil {
			// No DLQ wired — surface the error.
			return fmt.Errorf("cmdworkflow: subscriber %s exhausted (no DLQ wired): %w", sub.Name, lastErr)
		}
		stepName := stepPrefix(ctx) + fmt.Sprintf("%s:%s:dlq", sub.Name, env.EventID.String())
		_, err := b.wf.Run(ctx, stepName, func(ctx context.Context) ([]byte, error) {
			return nil, b.dlq.InsertSubscriberDLQ(ctx, SubscriberDLQRow{
				SubscriberName: sub.Name,
				TenantID:       env.TenantID,
				EventID:        env.EventID.String(),
				StreamID:       env.StreamID.Canonical(),
				TypeURL:        env.TypeURL,
				LastError:      lastErr.Error(),
				Attempts:       sub.MaxRetries + 1,
				EnqueuedAt:     time.Now().UTC(),
			})
		})
		return err

	case Compensate:
		if sub.Compensate == nil {
			return errors.New("cmdworkflow: OnExhausted=Compensate but Compensate fn is nil (should have been caught at Register)")
		}
		// Detach from caller's cancellation so compensation runs to
		// completion. Inherits values but ignores deadline + Done.
		detached := context.WithoutCancel(ctx)
		cmd, err := sub.Compensate(detached, env)
		if err != nil {
			return fmt.Errorf("cmdworkflow: subscriber %s compensate fn: %w", sub.Name, err)
		}
		// Recursive HandleCmd for the compensating command. The
		// nested invocation issues its own "append" / "read-envelopes"
		// steps — same names as the parent's, which would collide in
		// the Restate journal. Push a unique prefix on the context;
		// step names downstream all pick it up via stepPrefix(ctx).
		nestedPrefix := stepPrefix(detached) + fmt.Sprintf("compensate:%s:%s:", sub.Name, env.EventID.String())
		detached = withStepPrefix(detached, nestedPrefix)
		_, err = b.HandleCmd(detached, sid, cmd)
		return err
	}
	return fmt.Errorf("cmdworkflow: unknown OnExhausted policy: %d", sub.OnExhausted)
}
