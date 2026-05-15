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
	"google.golang.org/protobuf/proto"

	"github.com/laenenai/eventstore/es"
	"github.com/laenenai/eventstore/shred"
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

	// StateCodec is optional. When set, every successful Handle call
	// folds the produced events into a post-decide state via Evolve,
	// marshals via StateCodec, and writes the result to the Tier-1
	// state_cache in the same transaction as the events. The same
	// codec is reused for snapshot read/write (ADR 0011).
	StateCodec StateCodec[S]

	// StateSchemaVersion identifies the shape of S. Snapshots carry
	// this version; on load, a snapshot whose stored version differs
	// from the current StateSchemaVersion is silently discarded with
	// full-replay fallback. Bump when adding/removing/renaming any
	// field in S. Default 1.
	StateSchemaVersion uint32

	// Shredder enables crypto-shredding for event PII fields
	// (ADR 0010). When set, the runtime calls EncryptPII on each
	// event before Codec.Encode (in Handle) and DecryptPII on each
	// decoded event after Codec.Decode (in Load) — but only for
	// events whose generated code implements shred.PIIEncoder.
	// Events without PII fields pass through unchanged.
	//
	// The framework derives the encryption subject from the event's
	// Subject() method when set, falling back to the StreamID's
	// identifier component. Per-field subject overrides
	// (es.v1.subject = "other_field") are honored by the
	// codegen-emitted EncryptPII/DecryptPII bodies.
	Shredder *shred.Shredder

	// OnRedacted is an optional hook invoked when Load encounters
	// PII fields that could not be decrypted (subject shredded,
	// KMS unavailable, etc.). The handler is called once per Load
	// with the aggregate's loaded state and the accumulated
	// redactions across all replayed events. Use to surface a UI
	// warning or audit log entry. Other errors (tag mismatch,
	// real KMS failures) abort Load before this callback runs.
	OnRedacted func(redacted shred.RedactedFields)

	// Clock is the source of "now" for envelope timestamps. Defaults
	// to es.RealClock when unset. Tests inject es.NewManualClock(...)
	// to make expiry windows, last-seen buckets, and other time-bound
	// invariants deterministic — see cookbook 18.
	//
	// Only framework-side stamping goes through this clock. Domain
	// code (Decider, Evolve) MUST NOT call time.Now() directly either;
	// pass the timestamp in on the command if the domain needs it.
	Clock es.Clock
}

// Now returns the current instant from the runtime's Clock, defaulting
// to es.RealClock when not wired. Framework-side callers use this for
// envelope timestamps; domain code receives time via commands.
func (r *Runtime[S, C, E]) Now() time.Time {
	if r.Clock != nil {
		return r.Clock.Now()
	}
	return es.RealClock.Now()
}

// Load reads the stream, folds it via Decider.Evolve, and returns the
// resulting state together with the current stream version. Empty
// streams return Decider.Initial() and version 0.
//
// When StateCodec is set and the Store implements
// es.StateCacheReader, Load reads the state_cache row first and uses
// it as the replay base (ADR 0023). state_cache is always at the
// latest version (written transactionally with events), so in the
// steady state Load reads zero tail events. A row whose stored
// StateSchemaVersion doesn't match the runtime's current
// StateSchemaVersion is silently discarded with full-replay fallback
// — the cache is a cache, not data.
//
// On bus consumers and projection rebuilds, upcasting (ADR 0013) is
// applied per event via the Codec.Decode call.
func (r *Runtime[S, C, E]) Load(ctx context.Context, sid es.StreamID) (S, uint64, error) {
	state := r.Decider.Initial()
	var fromVersion uint64

	if r.StateCodec != nil {
		if reader, ok := r.Store.(es.StateCacheReader); ok {
			row, err := reader.GetState(ctx, sid.Tenant, sid.Canonical())
			if err == nil && row.Version > 0 {
				if row.StateSchemaVersion == r.stateSchemaVersion() {
					if decoded, derr := r.StateCodec.Decode(row.State); derr == nil {
						state = decoded
						fromVersion = row.Version
					}
					// Decode error → fall through to full replay.
				}
				// Schema mismatch → silent discard, full replay.
			}
			// ErrStateNotFound or any read error → full replay.
		}
	}

	envs, err := r.Store.ReadStream(ctx, sid, fromVersion)
	if err != nil {
		return state, 0, fmt.Errorf("aggregate: load: %w", err)
	}
	version := fromVersion
	var allRedacted shred.RedactedFields
	for _, env := range envs {
		evt, err := r.Codec.Decode(env.TypeURL, env.SchemaVersion, env.Payload)
		if err != nil {
			return state, 0, fmt.Errorf("aggregate: decode event %s v%d: %w",
				env.TypeURL, env.SchemaVersion, err)
		}

		// ADR 0010: if the runtime has a Shredder and the decoded
		// event implements PIIEncoder, decrypt PII fields in place
		// before Evolve sees them.
		if r.Shredder != nil {
			if pii, ok := any(evt).(shred.PIIEncoder); ok {
				subject := pii.Subject()
				if subject == "" {
					subject = sid.ID
				}
				redacted, derr := pii.DecryptPII(ctx, r.Shredder, sid.Tenant, subject)
				if derr != nil {
					return state, 0, fmt.Errorf("aggregate: decrypt %s v%d: %w",
						env.TypeURL, env.SchemaVersion, derr)
				}
				if len(redacted) > 0 {
					allRedacted = append(allRedacted, redacted...)
				}
			}
		}

		state = r.Decider.Evolve(state, evt)
		version = env.Version
	}
	if r.OnRedacted != nil && len(allRedacted) > 0 {
		r.OnRedacted(allRedacted)
	}
	return state, version, nil
}

func (r *Runtime[S, C, E]) stateSchemaVersion() uint32 {
	if r.StateSchemaVersion == 0 {
		return 1
	}
	return r.StateSchemaVersion
}

// sumCloner is the interface implemented by codegen-emitted event
// variants: a uniformly-typed CloneSum() method returning the sealed
// sum-type interface E (e.g. shredv1.Event). Each variant's body
// delegates to its typed Clone() *T — no reflection, no proto-runtime
// import. Defined here (not in es/) so it stays adjacent to the only
// caller and the constraint shape is obvious from this file.
type sumCloner[E any] interface {
	CloneSum() E
}

// cloneProtoEvent makes a deep copy of an event so Handle can encrypt
// PII fields without mutating the caller's typed event. Two paths:
//
//  1. Fast path — the event satisfies sumCloner[E] (codegen-emitted
//     CloneSum() E on every Event variant). Invokes the generated
//     deep-copy directly; ~5-7x faster than proto.Clone on small
//     messages and avoids the reflection-based clone machinery.
//
//  2. Fallback — proto.Clone. Kept for hypothetical hand-written event
//     types that satisfy proto.Message but skip the codegen (no current
//     callers in the repo, but the runtime's E is `any` so we cannot
//     statically rule them out).
//
// The function still requires the event to be a proto.Message at the
// boundary, because crypto-shredding only kicks in for variants that
// also implement shred.PIIEncoder — and those are exclusively codegen-
// emitted proto messages today.
func cloneProtoEvent[E any](e E) (E, error) {
	if c, ok := any(e).(sumCloner[E]); ok {
		return c.CloneSum(), nil
	}
	pm, ok := any(e).(proto.Message)
	if !ok {
		var zero E
		return zero, fmt.Errorf("aggregate: event %T does not implement proto.Message (required for crypto-shredding)", e)
	}
	cloned, ok := proto.Clone(pm).(E)
	if !ok {
		var zero E
		return zero, fmt.Errorf("aggregate: proto.Clone(%T) returned %T", e, proto.Clone(pm))
	}
	return cloned, nil
}

// Handle is Load + Decide + Append in one transactionally-coherent
// call. Returns the AppendResult from the store, or:
//   - any error from Load (read path),
//   - es.ErrTerminal if Decider.IsTerminal reports the stream is
//     closed (no further commands accepted),
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
	cfg := defaultHandleConfig(r.Now())
	for _, opt := range opts {
		opt(&cfg)
	}

	state, version, err := r.Load(ctx, sid)
	if err != nil {
		return es.AppendResult{}, err
	}

	if r.Decider.IsTerminal != nil && r.Decider.IsTerminal(state) {
		return es.AppendResult{}, es.ErrTerminal
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
		// ADR 0010: when a Shredder is configured and this event
		// implements PIIEncoder, clone the event and encrypt PII
		// fields before Codec.Encode. Cloning is mandatory — the
		// caller still holds the typed event with plaintext bytes
		// after Handle returns; we mustn't mutate it.
		toEncode := evt
		if r.Shredder != nil {
			if pii, ok := any(evt).(shred.PIIEncoder); ok {
				cloned, err := cloneProtoEvent(evt)
				if err != nil {
					return es.AppendResult{}, fmt.Errorf("aggregate: clone event %d for encryption: %w", i, err)
				}
				clonedPII := any(cloned).(shred.PIIEncoder)
				_ = pii // keep variable referenced
				subject := clonedPII.Subject()
				if subject == "" {
					subject = sid.ID
				}
				if err := clonedPII.EncryptPII(ctx, r.Shredder, sid.Tenant, subject); err != nil {
					return es.AppendResult{}, fmt.Errorf("aggregate: encrypt event %d: %w", i, err)
				}
				toEncode = cloned
			}
		}

		enc, err := r.Codec.Encode(toEncode)
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

	params := es.AppendParams{
		StreamID:        sid,
		ExpectedVersion: version,
		Events:          toAppend,
		Constraints:     constraints,
	}

	// Tier-1 state cache (ADR 0020). When the caller wired a
	// StateCodec, fold the new events through Evolve to produce the
	// post-decide state, marshal it, and ship the bytes alongside
	// the events so the adapter writes the cache row in-tx.
	if r.StateCodec != nil {
		newState := state
		for _, evt := range events {
			newState = r.Decider.Evolve(newState, evt)
		}
		stateBytes, typeURL, err := r.StateCodec.Encode(newState)
		if err != nil {
			return es.AppendResult{}, fmt.Errorf("aggregate: encode state: %w", err)
		}
		params.NewStateBytes = stateBytes
		params.StateTypeURL = typeURL
		params.StateSchemaVersion = r.stateSchemaVersion()
		if r.Decider.IsTerminal != nil {
			params.Terminal = r.Decider.IsTerminal(newState)
		}
	}

	return r.Store.Append(ctx, params)
}

// handleConfig holds optional fields populated by HandleOption funcs.
type handleConfig struct {
	commandID     uuid.UUID
	correlationID uuid.UUID
	causationID   uuid.UUID
	occurredAt    time.Time
	actor         es.Actor
}

// defaultHandleConfig seeds a HandleConfig with framework-managed
// defaults. The occurredAt comes from the runtime's Clock (now) so
// tests can swap in a ManualClock for deterministic envelope
// timestamps; production passes RealClock.Now().
func defaultHandleConfig(now time.Time) handleConfig {
	cid := uuid.Must(uuid.NewV7())
	return handleConfig{
		commandID:     cid,
		correlationID: cid,
		causationID:   cid,
		occurredAt:    now.UTC(),
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

// WithOccurredAt overrides the domain-time stamp. Defaults to the
// runtime's Clock.Now() at the moment Handle is invoked (RealClock in
// production, ManualClock in tests).
func WithOccurredAt(t time.Time) HandleOption {
	return func(c *handleConfig) { c.occurredAt = t.UTC() }
}

// WithActor sets the Actor recorded on every event the call produces.
func WithActor(a es.Actor) HandleOption {
	return func(c *handleConfig) { c.actor = a }
}
