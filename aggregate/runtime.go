// Package aggregate hosts the aggregate runtime: load (replay) + handle
// (decide + append) for a Decider[S, C, E] against an es.Store.
//
// See ADR 0003 (decider model). The runtime is purely orchestration —
// all business logic lives in the Decider.
package aggregate

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/laenenai/eventstore/es"
)

// EncodedEvent is what the Codec produces when serializing one event.
// Embedded in the EventToAppend the runtime hands to es.Store.
type EncodedEvent struct {
	Payload       []byte
	TypeURL       string
	SchemaVersion uint32
}

// Codec marshals events to/from the wire bytes the store carries.
// Implementations are typically codegen-emitted for a real domain;
// hand-written for examples and tests. See ADR 0004.
type Codec[E any] interface {
	// Encode serializes one event. The returned TypeURL identifies the
	// concrete variant; SchemaVersion is the dispatch key used by
	// upcasters on read (ADR 0013).
	Encode(e E) (EncodedEvent, error)

	// Decode is the inverse of Encode. Given a TypeURL +
	// SchemaVersion + payload, return the corresponding Go event.
	// Upcasting (ADR 0013) is the codec's responsibility — the runtime
	// hands the codec the on-disk schema_version and expects back an
	// event in the codec's current shape.
	Decode(typeURL string, schemaVersion uint32, payload []byte) (E, error)
}

// Runtime drives a Decider against an es.Store. Construct directly via
// the struct literal — only the three public fields are required.
//
//	rt := &aggregate.Runtime[State, Command, Event]{
//	    Store:   store,
//	    Decider: counter.Decider,
//	    Codec:   counter.Codec,
//	}
type Runtime[S, C, E any] struct {
	Store   es.Store
	Decider es.Decider[S, C, E]
	Codec   Codec[E]
}

// Load reads the stream, folds it via Decider.Evolve, and returns the
// resulting state together with the current stream version. Empty
// streams return Decider.Initial() and version 0.
//
// On bus consumers and projection rebuilds, upcasting (ADR 0013) is
// applied per event via the Codec.Decode call.
func (r *Runtime[S, C, E]) Load(ctx context.Context, sid es.StreamID) (S, uint64, error) {
	state := r.Decider.Initial()
	envs, err := r.Store.ReadStream(ctx, sid, 0)
	if err != nil {
		return state, 0, fmt.Errorf("aggregate: load: %w", err)
	}
	var version uint64
	for _, env := range envs {
		evt, err := r.Codec.Decode(env.TypeURL, env.SchemaVersion, env.Payload)
		if err != nil {
			return state, 0, fmt.Errorf("aggregate: decode event %s v%d: %w",
				env.TypeURL, env.SchemaVersion, err)
		}
		state = r.Decider.Evolve(state, evt)
		version = env.Version
	}
	return state, version, nil
}

// Handle is Load + Decide + Append in one transactionally-coherent
// call. Returns the AppendResult from the store, or:
//   - any error from Load (read path),
//   - any error from Decider.Decide (business rule failure),
//   - es.ErrConflict on an optimistic-concurrency miss (someone else
//     wrote to the stream between Load and Append — caller may retry),
//   - es.ErrConstraintViolated on a uniqueness claim conflict.
//
// If Decider.Decide returns no events (a no-op command), Handle
// returns the zero AppendResult and no error.
func (r *Runtime[S, C, E]) Handle(
	ctx context.Context,
	sid es.StreamID,
	cmd C,
	opts ...HandleOption,
) (es.AppendResult, error) {
	cfg := defaultHandleConfig()
	for _, opt := range opts {
		opt(&cfg)
	}

	state, version, err := r.Load(ctx, sid)
	if err != nil {
		return es.AppendResult{}, err
	}

	events, constraints, err := r.Decider.Decide(state, cmd)
	if err != nil {
		return es.AppendResult{}, fmt.Errorf("aggregate: decide: %w", err)
	}
	if len(events) == 0 {
		return es.AppendResult{}, nil
	}

	toAppend := make([]es.EventToAppend, len(events))
	for i, evt := range events {
		enc, err := r.Codec.Encode(evt)
		if err != nil {
			return es.AppendResult{}, fmt.Errorf("aggregate: encode event %d: %w", i, err)
		}
		toAppend[i] = es.EventToAppend{
			EventID:       uuid.Must(uuid.NewV7()),
			TypeURL:       enc.TypeURL,
			SchemaVersion: enc.SchemaVersion,
			OccurredAt:    cfg.occurredAt,
			CorrelationID: cfg.correlationID,
			CausationID:   cfg.causationID,
			CommandID:     cfg.commandID,
			Actor:         cfg.actor,
			Payload:       enc.Payload,
		}
	}

	return r.Store.Append(ctx, es.AppendParams{
		StreamID:        sid,
		ExpectedVersion: version,
		Events:          toAppend,
		Constraints:     constraints,
	})
}

// handleConfig holds optional fields populated by HandleOption funcs.
type handleConfig struct {
	commandID     uuid.UUID
	correlationID uuid.UUID
	causationID   uuid.UUID
	occurredAt    time.Time
	actor         es.Actor
}

func defaultHandleConfig() handleConfig {
	cid := uuid.Must(uuid.NewV7())
	return handleConfig{
		commandID:     cid,
		correlationID: cid,
		causationID:   cid,
		occurredAt:    time.Now().UTC(),
	}
}

// HandleOption configures envelope metadata for a Handle call.
type HandleOption func(*handleConfig)

// WithCommandID overrides the command id used for the resulting
// events. Caller-supplied command ids enable idempotent retry behavior
// at the target — see ADR 0015 and DeriveCommandID.
func WithCommandID(id uuid.UUID) HandleOption {
	return func(c *handleConfig) { c.commandID = id }
}

// WithCorrelationID propagates a trace/request-scoped correlation id
// into the events emitted by this command.
func WithCorrelationID(id uuid.UUID) HandleOption {
	return func(c *handleConfig) { c.correlationID = id }
}

// WithCausationID overrides the causation id. Defaults to the
// command id when not set.
func WithCausationID(id uuid.UUID) HandleOption {
	return func(c *handleConfig) { c.causationID = id }
}

// WithOccurredAt overrides the domain-time stamp. Defaults to
// time.Now().UTC() at the moment Handle is invoked.
func WithOccurredAt(t time.Time) HandleOption {
	return func(c *handleConfig) { c.occurredAt = t.UTC() }
}

// WithActor sets the Actor recorded on every event the call produces.
func WithActor(a es.Actor) HandleOption {
	return func(c *handleConfig) { c.actor = a }
}
