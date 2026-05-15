package cmdworkflow

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/laenenai/eventstore/aggregate"
	"github.com/laenenai/eventstore/es"
)

// AggregateRunner is the minimal aggregate-runtime surface the bus
// needs: append a command, load the post-Decide state, encode/decode
// that state for journaling, and read the runtime's clock.
// Concretely satisfied by *aggregate.Runtime[S, C, E].
//
// EncodeState / DecodeState bridge the bus's "journal the post-Decide
// state once per dispatch" step (ADR 0029) to the application's
// concrete state type. The bus doesn't know S's wire format —
// arbitrary type parameter — so the runner exposes the same codec
// used for state_cache writes.
//
// Now() exists on the interface so the bus stamps DLQ rows and other
// framework-managed timestamps through the SAME clock the aggregate
// runtime uses for envelopes. Tests injecting a ManualClock into the
// runtime get consistent timestamps across the whole pipeline.
type AggregateRunner[S, C any] interface {
	Handle(ctx context.Context, sid es.StreamID, cmd C, opts ...aggregate.HandleOption) (es.AppendResult, error)
	Load(ctx context.Context, sid es.StreamID) (S, uint64, error)
	EncodeState(state S) ([]byte, error)
	DecodeState(data []byte) (S, error)
	Now() time.Time
}

// Workflow is the generic command handler + subscriber registry
// described in ADR 0025, with per-command-batch subscriber delivery
// (ADR 0029).
//
// One bus per aggregate (S, C, E). The E parameter is required so the
// bus can decode envelopes into the typed event sum at the boundary
// and hand the batch to subscribers as []E.
//
// Wiring:
//
//	bus := cmdworkflow.New[*invoicev1.Invoice, invoicev1.Command, invoicev1.Event](
//	    runtime,                  // aggregate.Runtime[S, C, E]
//	    store,                    // es.Store
//	    wfRuntime,                // WorkflowRuntime adapter (inproc, restate, …)
//	    invoicev1.EventCodec{},   // aggregate.Codec[E]
//	)
//	bus.Register(cmdworkflow.Subscriber[*invoicev1.Invoice, invoicev1.Command, invoicev1.Event]{...})
//	state, err := bus.HandleCmd(ctx, streamID, cmd)
//
// HandleCmd contract:
//   - Returns the aggregate's own state S after Append (ADR 0025 §
//     "S is the aggregate's state, not a view"). The state is read
//     ONCE per HandleCmd, journaled as a dedicated step, and reused
//     for every subscriber call — replay sees the same value.
//   - Fans out per (subscriber, command). Each subscriber whose
//     Filter matches at least one envelope receives ONE Handle call
//     with the filtered batch, the typed state, and the typed events.
//   - Compensation runs on a fresh, non-cancellable context (ADR
//     0025 § decision 8) so a caller timeout cannot leave the
//     aggregate in a half-rolled-back state.
type Workflow[S, C, E any] struct {
	runner      AggregateRunner[S, C]
	store       es.Store
	wf          WorkflowRuntime
	codec       aggregate.Codec[E]
	subscribers []Subscriber[S, C, E]
	dlq         SubscriberDLQWriter

	// asyncSend, when set by the codegen-emitted Service constructor,
	// replaces the goroutine-based Spawn for durable Async fan-out.
	// Each (subscriber, command-batch) becomes its own runtime
	// workflow invocation with a deterministic workflowID for dedup.
	asyncSend AsyncSend
}

// New constructs a Workflow. The dlq writer is wired separately via
// WithDLQ. The codec decodes Envelope.Payload back into the typed
// event variant so subscribers see []E instead of raw bytes.
func New[S, C, E any](
	runner AggregateRunner[S, C],
	store es.Store,
	wf WorkflowRuntime,
	codec aggregate.Codec[E],
) *Workflow[S, C, E] {
	return &Workflow[S, C, E]{
		runner: runner,
		store:  store,
		wf:     wf,
		codec:  codec,
	}
}

// WithDLQ wires a SubscriberDLQWriter for OnExhausted=DLQ subscribers.
// Returns the bus for chaining.
func (b *Workflow[S, C, E]) WithDLQ(dlq SubscriberDLQWriter) *Workflow[S, C, E] {
	b.dlq = dlq
	return b
}

// Register appends a subscriber to the bus. Order matters only for
// deterministic step naming during replay; subscribers do not see
// one another's effects.
func (b *Workflow[S, C, E]) Register(s Subscriber[S, C, E]) {
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
// input; sendAsync constructs it when invoking the runtime's send
// primitive. Exported for codegen consumption — application code
// typically doesn't touch it.
//
// EnvBytes carries the WHOLE envelope batch the Async subscriber
// should observe. The dispatcher re-decodes envelopes into []E using
// the bus's codec, and re-Loads state via the AggregateRunner before
// invoking Handle. This keeps the wire shape narrow — no second
// state encoding contract to maintain — at the cost of one
// state_cache read per Async dispatch, which is the lookup the
// runtime would do anyway for the next command on the same stream.
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
// "<streamType>:<subscriberName>:<firstEventID>". Two Spawn calls
// with the same id produce one child workflow.
type AsyncSend func(ctx context.Context, subscriberName string, envBytes []byte, workflowID string) error

// SetAsyncSend wires a durable Async fan-out function. Called by
// the codegen-emitted Service constructors (NewRestateService,
// NewDBOSService). If unset, spawnAsync falls back to the goroutine-
// based Spawn (Phase 2a inproc behavior).
func (b *Workflow[S, C, E]) SetAsyncSend(fn AsyncSend) {
	b.asyncSend = fn
}

// DispatchAsync is invoked by the codegen-emitted Service's
// AsyncDispatch workflow when a child invocation fires. Looks up the
// subscriber by name, decodes the envelope batch, decodes typed
// events, re-Loads state, runs the same retry+exhausted policy as
// Sync.
//
// Returns nil on success or successful policy application
// (DLQ-written, Drop, etc.). Returns an error only on framework
// failures (subscriber not found, encoding error, state load error).
// Subscriber Handle errors are absorbed by the retry+policy loop and
// never propagate up — the parent runtime should consider the child
// workflow successful regardless of subscriber Handle outcome.
func (b *Workflow[S, C, E]) DispatchAsync(ctx context.Context, subscriberName string, envBytes []byte) error {
	var sub Subscriber[S, C, E]
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
		return fmt.Errorf("cmdworkflow: DispatchAsync: decode envelopes: %w", err)
	}
	if len(envs) == 0 {
		return fmt.Errorf("cmdworkflow: DispatchAsync: empty envelope batch")
	}

	events, err := b.decodeEvents(envs)
	if err != nil {
		return fmt.Errorf("cmdworkflow: DispatchAsync: decode events: %w", err)
	}

	sid := envs[0].StreamID
	// Re-load state on the consumer side. The async dispatch runs in
	// its own workflow, after the parent has committed — state_cache
	// is already populated with the post-Decide state at the version
	// that produced this batch.
	state, _, err := b.runner.Load(ctx, sid)
	if err != nil {
		return fmt.Errorf("cmdworkflow: DispatchAsync: load state: %w", err)
	}

	if err := b.runRetries(ctx, envs, state, events, sub); err != nil {
		return b.onExhausted(ctx, sid, envs, state, events, sub, err)
	}
	return nil
}

// With is the fluent counterpart to Register. Returns the workflow so
// subscribers + DLQ can be wired in one expression:
//
//	wf := cmdworkflow.New[*Invoice, Command, Event](rt, store, inproc.New(), codec).
//	    WithDLQ(store).
//	    With(read.Subscriber(), search.Subscriber(), credit.Subscriber())
//
// Each variadic argument goes through the same validation as Register
// (panics on empty name, missing Handle, Compensate without fn,
// duplicate names).
func (b *Workflow[S, C, E]) With(subs ...Subscriber[S, C, E]) *Workflow[S, C, E] {
	for _, s := range subs {
		b.Register(s)
	}
	return b
}

// HandleCmd is the entry point. Appends events through aggregate.Runtime,
// reads the just-written envelope batch back, loads the post-Decide
// state once, decodes events once, then fans out per registered
// subscriber. Returns the state read inside this dispatch.
//
// A no-op command (Decider returns zero events) returns the current
// state and nil error — no subscribers are notified.
func (b *Workflow[S, C, E]) HandleCmd(
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

	// Step 3: load post-Decide state ONCE for this dispatch. Journaled
	// so workflow replay sees the same state for the same dispatch —
	// subscribers must observe a fixed value, not whatever the
	// state_cache happens to hold at replay time (a later command
	// against the same stream may have advanced it).
	state, err := b.readStateStep(ctx, sid)
	if err != nil {
		return zero, err
	}

	// Step 4: decode typed events once. Codec.Decode is deterministic
	// so no separate journal step is required.
	events, err := b.decodeEvents(envs)
	if err != nil {
		return zero, fmt.Errorf("cmdworkflow: decode events: %w", err)
	}

	// Step 5: fan out per (subscriber, command-batch). One Handle
	// call per subscriber that has at least one envelope passing its
	// Filter. Sync subscribers run concurrently via RunAsync; Async
	// subscribers fire via Spawn.
	type syncEntry struct {
		sub    Subscriber[S, C, E]
		envs   []es.Envelope
		events []E
		future Future
	}
	var syncs []syncEntry
	for _, sub := range b.subscribers {
		// Filter envs+events together — both slices stay index-aligned.
		var (
			filtEnvs   []es.Envelope
			filtEvents []E
		)
		for i, env := range envs {
			if sub.Filter.Matches(env) {
				filtEnvs = append(filtEnvs, env)
				filtEvents = append(filtEvents, events[i])
			}
		}
		if len(filtEnvs) == 0 {
			// No matching envelopes — skip entirely. No journal entry.
			continue
		}
		if sub.Mode == Async {
			if err := b.spawnAsync(ctx, sid, filtEnvs, sub); err != nil {
				return zero, err
			}
			continue
		}
		syncs = append(syncs, syncEntry{
			sub:    sub,
			envs:   filtEnvs,
			events: filtEvents,
			future: b.runSyncSubscriber(ctx, filtEnvs, state, filtEvents, sub),
		})
	}

	// Wait for every Sync subscriber, collect exhaustion errors per
	// (subscriber, batch). Don't short-circuit — every subscriber's
	// policy runs even if a sibling failed.
	//
	// Future.Wait returns:
	//   - (nil,    nil)    on success (fn returned nil/nil)
	//   - (bytes,  nil)    on exhausted (fn returned bytes, nil)
	//   - (any,    err)    on workflow infrastructure error
	type exhausted struct {
		entry syncEntry
		err   error
	}
	var toApply []exhausted
	for _, s := range syncs {
		raw, err := s.future.Wait()
		if err != nil {
			return zero, fmt.Errorf("cmdworkflow: subscriber %s: %w", s.sub.Name, err)
		}
		if len(raw) > 0 {
			toApply = append(toApply, exhausted{
				entry: s,
				err:   errors.New(string(raw)),
			})
		}
	}
	// Apply OnExhausted policy from the outer context. All Run /
	// HandleCmd calls inside onExhausted use the parent ctx directly
	// (real workflow Context, not a RunContext shim) — Restate forbids
	// nested Run from inside a RunContext closure.
	//
	// Compensate appends new events through a nested HandleCmd. When
	// that happens we must re-load the state at the end so the caller
	// observes the compensated state, not the pre-compensation
	// snapshot from step 3. mutated tracks whether any policy
	// produced new events.
	mutated := false
	for _, e := range toApply {
		if err := b.onExhausted(ctx, sid, e.entry.envs, state, e.entry.events, e.entry.sub, e.err); err != nil {
			return zero, err
		}
		if e.entry.sub.OnExhausted == Compensate {
			mutated = true
		}
	}

	// Step 6: return the journaled state from step 3 in the common
	// path. If compensation ran the state has advanced — re-load so
	// the caller sees the post-compensation state (one extra
	// state_cache read, only on the saga-failure path).
	if mutated {
		final, _, err := b.runner.Load(ctx, sid)
		return final, err
	}
	return state, nil
}

// stepPrefixKey is the typed context-value key for namespacing step
// names during nested HandleCmd calls (Compensate recursion). The
// recursive HandleCmd issues "append", "read-envelopes", "read-state"
// — but the outer HandleCmd has already done so. Same step name =
// journal collision in Restate. Prefix disambiguates.
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
func (b *Workflow[S, C, E]) appendStep(
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
func (b *Workflow[S, C, E]) readEnvelopesStep(
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

// readStateStep loads the post-Decide state from state_cache (Tier-1)
// and journals the bytes. On replay the journaled bytes drive the
// decode — state_cache may have advanced since this dispatch (a
// sibling command can have written a later state between this
// dispatch's first run and the replay), and subscribers must see the
// value at the version this command produced, not whatever's current.
//
// The bytes are produced by the AggregateRunner's StateCodec — the
// same codec used for state_cache writes — so the journaled
// representation roundtrips losslessly through the same path the
// runtime already validates.
func (b *Workflow[S, C, E]) readStateStep(
	ctx context.Context,
	sid es.StreamID,
) (S, error) {
	var zero S
	raw, err := b.wf.Run(ctx, stepPrefix(ctx)+"read-state", func(ctx context.Context) ([]byte, error) {
		state, _, lerr := b.runner.Load(ctx, sid)
		if lerr != nil {
			return nil, lerr
		}
		return b.runner.EncodeState(state)
	})
	if err != nil {
		return zero, err
	}
	return b.runner.DecodeState(raw)
}

// decodeEvents runs the codec over the envelope batch and returns a
// slice index-aligned with envs.
func (b *Workflow[S, C, E]) decodeEvents(envs []es.Envelope) ([]E, error) {
	out := make([]E, len(envs))
	for i, env := range envs {
		evt, err := b.codec.Decode(env.TypeURL, env.SchemaVersion, env.Payload)
		if err != nil {
			return nil, fmt.Errorf("decode event %d (%s): %w", i, env.TypeURL, err)
		}
		out[i] = evt
	}
	return out, nil
}

// spawnAsync dispatches an Async subscriber. Two paths:
//
//   - If asyncSend is set (the codegen-emitted Service has wired it
//     in via SetAsyncSend), invoke the durable child-workflow path:
//     encode the envelope batch, generate a deterministic workflowID
//     keyed off the first event id in the batch, and hand off to the
//     runtime's ServiceSend / RunWorkflow targeting the Service's
//     pre-registered AsyncDispatch method.
//
//   - Otherwise, fall back to the goroutine-based Spawn (Phase 2a
//     inproc behavior). Not durable, but correct for tests and
//     single-process apps. The closure does its own state load and
//     event decode so the inproc path mirrors the durable path.
//
// The workflowID format is "<streamType>:<subscriberName>:<firstEventID>"
// — deterministic across replays; per-batch granularity is sufficient
// for dedup since each command produces a fresh set of event ids.
func (b *Workflow[S, C, E]) spawnAsync(
	ctx context.Context,
	sid es.StreamID,
	envs []es.Envelope,
	sub Subscriber[S, C, E],
) error {
	if b.asyncSend != nil {
		envBytes := encodeEnvelopes(envs)
		workflowID := fmt.Sprintf("%s:%s:%s", sid.Type, sub.Name, envs[0].EventID.String())
		return b.asyncSend(ctx, sub.Name, envBytes, workflowID)
	}

	// Fallback: goroutine-based fire-and-forget. We capture the
	// envelope batch and re-derive state + events inside the spawned
	// closure so the path matches DispatchAsync semantically — both
	// sides decode events from envelopes and Load state at dispatch
	// time.
	spawnName := sub.Name + ":" + envs[0].EventID.String()
	return b.wf.Spawn(ctx, spawnName, func(ctx context.Context) error {
		events, err := b.decodeEvents(envs)
		if err != nil {
			return err
		}
		state, _, err := b.runner.Load(ctx, sid)
		if err != nil {
			return err
		}
		if err := b.runRetries(ctx, envs, state, events, sub); err != nil {
			return b.onExhausted(ctx, sid, envs, state, events, sub, err)
		}
		return nil
	})
}

// runSyncSubscriber dispatches a Sync subscriber as one journaled
// async step (RunAsync). Retries happen INSIDE the step's fn so the
// journal sees exactly one entry per (subscriber, command-batch)
// regardless of attempt count.
//
// IMPORTANT: the fn ALWAYS returns nil error to the runtime, even on
// exhaustion. Restate treats a non-nil fn error as step failure and
// retries the whole invocation — which is wrong for our retry budget
// semantics (we already retried inside the fn).
//
// The "exhausted, here's the lastErr" signal travels through the
// bytes return: nil bytes = success, non-empty bytes = exhausted
// error message. HandleCmd's outer loop decodes this and applies the
// OnExhausted policy. The actual DLQ insert / Compensate recursion
// happens from HandleCmd's main context (real workflow Context, not
// the RunContext shim).
func (b *Workflow[S, C, E]) runSyncSubscriber(
	ctx context.Context,
	envs []es.Envelope,
	state S,
	events []E,
	sub Subscriber[S, C, E],
) Future {
	// Step name uses the first event id in the batch — deterministic
	// across replays since envelope ordering is stable, and unique per
	// command since each command produces a distinct set of event ids.
	stepName := stepPrefix(ctx) + fmt.Sprintf("%s:cmd:%s", sub.Name, envs[0].EventID.String())
	return b.wf.RunAsync(ctx, stepName, func(ctx context.Context) ([]byte, error) {
		if err := b.runRetries(ctx, envs, state, events, sub); err != nil {
			return []byte(err.Error()), nil
		}
		return nil, nil
	})
}

// runRetries runs the subscriber's Handle inside the retry budget.
// Returns nil on success, the last error on exhaustion. Does NOT
// apply OnExhausted policy — that's the caller's responsibility.
//
// The budget is per-batch: one Handle call = one attempt.
func (b *Workflow[S, C, E]) runRetries(
	ctx context.Context,
	envs []es.Envelope,
	state S,
	events []E,
	sub Subscriber[S, C, E],
) error {
	maxAttempts := sub.MaxRetries + 1
	infinite := sub.MaxRetries < 0

	var lastErr error
	for attempt := 1; infinite || attempt <= maxAttempts; attempt++ {
		callCtx := ctx
		if sub.AttemptTimeout > 0 {
			var cancel context.CancelFunc
			callCtx, cancel = context.WithTimeout(ctx, sub.AttemptTimeout)
			err := sub.Handle(callCtx, envs, state, events)
			cancel()
			if err == nil {
				return nil
			}
			lastErr = err
			continue
		}
		if err := sub.Handle(callCtx, envs, state, events); err == nil {
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
func (b *Workflow[S, C, E]) onExhausted(
	ctx context.Context,
	sid es.StreamID,
	envs []es.Envelope,
	state S,
	events []E,
	sub Subscriber[S, C, E],
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
		eventIDs := make([]string, len(envs))
		typeURLs := make([]string, len(envs))
		for i, env := range envs {
			eventIDs[i] = env.EventID.String()
			typeURLs[i] = env.TypeURL
		}
		// Step name keyed by the FIRST event id in the batch — that
		// id uniquely identifies the command-batch in this stream,
		// and replays produce identical step names.
		stepName := stepPrefix(ctx) + fmt.Sprintf("%s:%s:dlq", sub.Name, envs[0].EventID.String())
		_, err := b.wf.Run(ctx, stepName, func(ctx context.Context) ([]byte, error) {
			return nil, b.dlq.InsertSubscriberDLQ(ctx, SubscriberDLQRow{
				SubscriberName: sub.Name,
				TenantID:       envs[0].TenantID,
				StreamID:       sid.Canonical(),
				EventIDs:       eventIDs,
				TypeURLs:       typeURLs,
				LastError:      lastErr.Error(),
				Attempts:       sub.MaxRetries + 1,
				EnqueuedAt:     b.runner.Now(),
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
		cmd, err := sub.Compensate(detached, envs, state, events)
		if err != nil {
			return fmt.Errorf("cmdworkflow: subscriber %s compensate fn: %w", sub.Name, err)
		}
		// Recursive HandleCmd for the compensating command. The
		// nested invocation issues its own "append" / "read-envelopes"
		// / "read-state" steps — same names as the parent's, which
		// would collide in the Restate journal. Push a unique prefix
		// on the context; step names downstream all pick it up via
		// stepPrefix(ctx).
		nestedPrefix := stepPrefix(detached) + fmt.Sprintf("compensate:%s:%s:", sub.Name, envs[0].EventID.String())
		detached = withStepPrefix(detached, nestedPrefix)
		_, err = b.HandleCmd(detached, sid, cmd)
		return err
	}
	return fmt.Errorf("cmdworkflow: unknown OnExhausted policy: %d", sub.OnExhausted)
}
