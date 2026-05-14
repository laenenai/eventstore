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
	for _, existing := range b.subscribers {
		if existing.Name == s.Name {
			panic("cmdworkflow: duplicate Subscriber.Name: " + s.Name)
		}
	}
	b.subscribers = append(b.subscribers, s)
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
	// via RunAsync; HandleCmd waits for all of them to settle before
	// proceeding to the next event. Async subscribers are Spawned
	// into independent child workflows and keep running after
	// HandleCmd returns.
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
			// Sync: fan out as a journaled async step. Retries
			// happen inside fn — one journal entry per (sub, event)
			// regardless of attempt count.
			syncSubs = append(syncSubs, sub)
			syncFutures = append(syncFutures, b.runSyncSubscriber(ctx, sid, env, sub))
		}
		// Wait for every Sync subscriber to settle. We collect
		// errors but don't short-circuit — every subscriber's policy
		// (DLQ / Compensate / Drop) must run even if a sibling
		// failed.
		for i, f := range syncFutures {
			if _, err := f.Wait(); err != nil {
				return zero, fmt.Errorf("cmdworkflow: subscriber %s: %w", syncSubs[i].Name, err)
			}
		}
	}

	// Step 4: return post-Decide state. Loads from state_cache when
	// the runtime has a StateCodec (read-your-writes); falls back to
	// full event replay otherwise.
	state, _, err := b.runner.Load(ctx, sid)
	return state, err
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
	raw, err := b.wf.Run(ctx, "append", func(ctx context.Context) ([]byte, error) {
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
	raw, err := b.wf.Run(ctx, "read-envelopes", func(ctx context.Context) ([]byte, error) {
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

// spawnAsync dispatches an Async subscriber as an independent child
// workflow. The spawned fn runs the same retry-then-exhausted body
// as the Sync path; it just lives outside HandleCmd's wait set.
func (b *Workflow[S, C]) spawnAsync(
	ctx context.Context,
	sid es.StreamID,
	env es.Envelope,
	sub Subscriber[C],
) error {
	spawnName := sub.Name + ":" + env.EventID.String()
	return b.wf.Spawn(ctx, spawnName, func(ctx context.Context) error {
		return b.deliverWithRetriesInline(ctx, sid, env, sub)
	})
}

// runSyncSubscriber dispatches a Sync subscriber as one journaled
// async step (RunAsync). Retries happen INSIDE the step's fn so the
// journal sees exactly one entry per (subscriber, event) regardless
// of attempt count. Returns a Future the caller awaits.
func (b *Workflow[S, C]) runSyncSubscriber(
	ctx context.Context,
	sid es.StreamID,
	env es.Envelope,
	sub Subscriber[C],
) Future {
	stepName := fmt.Sprintf("%s:%s", sub.Name, env.EventID.String())
	return b.wf.RunAsync(ctx, stepName, func(ctx context.Context) ([]byte, error) {
		return nil, b.deliverWithRetriesInline(ctx, sid, env, sub)
	})
}

// deliverWithRetriesInline runs the full retry loop INSIDE one
// journaled step. Each attempt invokes sub.Handle directly — no
// nested Run call (the runtime contract forbids it inside a Run
// closure, and per-attempt journaling traded for parallelism per
// ADR 0026 § 7).
//
// On exhaustion the configured OnExhausted policy applies. DLQ /
// Compensate are journaled as their own steps issued from the
// HandleCmd context (via the outer ctx captured through closure
// when the inproc adapter is used; via separate steps registered by
// the parent invocation for Restate).
func (b *Workflow[S, C]) deliverWithRetriesInline(
	ctx context.Context,
	sid es.StreamID,
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

	// Exhausted.
	return b.onExhausted(ctx, sid, env, sub, lastErr)
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
			return fmt.Errorf("commandbus: subscriber %s exhausted (no DLQ wired): %w", sub.Name, lastErr)
		}
		stepName := fmt.Sprintf("%s:%s:dlq", sub.Name, env.EventID.String())
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
			return errors.New("commandbus: OnExhausted=Compensate but Compensate fn is nil (should have been caught at Register)")
		}
		// Detach from caller's cancellation so compensation runs to
		// completion. Inherits values but ignores deadline + Done.
		detached := context.WithoutCancel(ctx)
		cmd, err := sub.Compensate(detached, env)
		if err != nil {
			return fmt.Errorf("commandbus: subscriber %s compensate fn: %w", sub.Name, err)
		}
		// Recurse into HandleCmd with the compensating command.
		// The nested invocation produces its own events; the journal
		// nests naturally under the parent.
		stepName := fmt.Sprintf("%s:%s:compensate", sub.Name, env.EventID.String())
		_, err = b.wf.Run(detached, stepName, func(ctx context.Context) ([]byte, error) {
			_, herr := b.HandleCmd(ctx, sid, cmd)
			return nil, herr
		})
		return err
	}
	return fmt.Errorf("commandbus: unknown OnExhausted policy: %d", sub.OnExhausted)
}
