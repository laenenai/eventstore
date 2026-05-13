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

	// SnapshotEvery is the lazy-snapshot cadence: after a successful
	// Load that drove version to >= last-snapshot-version +
	// SnapshotEvery, the runtime writes a fresh snapshot. Default 0
	// disables snapshotting for this aggregate. Recommended 100+ for
	// streams that grow past a few hundred events.
	//
	// Requires Store to implement es.SnapshotStore (both shipped
	// adapters do) and StateCodec to be set (snapshots need to
	// serialize/deserialize the folded state).
	SnapshotEvery int

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
}

// Load reads the stream, folds it via Decider.Evolve, and returns the
// resulting state together with the current stream version. Empty
// streams return Decider.Initial() and version 0.
//
// When snapshots are enabled (StateCodec set and Store implements
// es.SnapshotStore), Load tries the snapshot first. A snapshot whose
// stored StateSchemaVersion matches the runtime's version is used as
// the starting point and only events with version > snapshot.Version
// are replayed. Stale or missing snapshots fall back to full replay
// transparently — they're a cache, not data. See ADR 0011.
//
// On bus consumers and projection rebuilds, upcasting (ADR 0013) is
// applied per event via the Codec.Decode call.
func (r *Runtime[S, C, E]) Load(ctx context.Context, sid es.StreamID) (S, uint64, error) {
	state := r.Decider.Initial()
	var (
		fromVersion uint64
		snapVersion uint64
	)

	if r.useSnapshots() {
		snap, err := r.snapshotStore().LoadSnapshot(ctx, sid.Tenant, sid.Canonical())
		if err == nil {
			if snap.StateSchemaVersion == r.stateSchemaVersion() {
				if decoded, derr := r.StateCodec.Decode(snap.State); derr == nil {
					state = decoded
					fromVersion = snap.Version
					snapVersion = snap.Version
				}
				// Decode failure: treat as stale, fall through to full replay.
			}
			// Schema mismatch: full replay, discard silently.
		}
		// ErrSnapshotNotFound or other read error: full replay, ignore.
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

	// Lazy snapshot write: if enough events have accumulated since
	// the last snapshot for this stream, persist a fresh one. Side
	// effect on read; intentional per ADR 0011.
	if r.useSnapshots() && version > 0 && int(version-snapVersion) >= r.SnapshotEvery {
		if err := r.writeSnapshot(ctx, sid, state, version); err != nil {
			// Best-effort: don't fail the load when snapshot-write fails.
			// The next read will retry.
			_ = err
		}
	}
	return state, version, nil
}

// useSnapshots reports whether snapshot integration is fully wired:
// SnapshotEvery > 0, StateCodec set, Store implements SnapshotStore.
func (r *Runtime[S, C, E]) useSnapshots() bool {
	if r.SnapshotEvery <= 0 || r.StateCodec == nil {
		return false
	}
	_, ok := r.Store.(es.SnapshotStore)
	return ok
}

func (r *Runtime[S, C, E]) snapshotStore() es.SnapshotStore {
	return r.Store.(es.SnapshotStore)
}

func (r *Runtime[S, C, E]) stateSchemaVersion() uint32 {
	if r.StateSchemaVersion == 0 {
		return 1
	}
	return r.StateSchemaVersion
}

// cloneProtoEvent makes a deep copy of an event so Handle can encrypt
// PII fields without mutating the caller's typed event. The type
// constraint E is `any`, so we round-trip through proto.Clone which
// requires the runtime type to be a proto.Message — which it always
// is for events that satisfy PIIEncoder (since codegen only emits
// the PII methods on proto-generated structs).
func cloneProtoEvent[E any](e E) (E, error) {
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

// writeSnapshot encodes the current state and upserts the snapshot row.
func (r *Runtime[S, C, E]) writeSnapshot(ctx context.Context, sid es.StreamID, state S, version uint64) error {
	bs, _, err := r.StateCodec.Encode(state)
	if err != nil {
		return fmt.Errorf("aggregate: snapshot encode: %w", err)
	}
	return r.snapshotStore().SaveSnapshot(ctx, es.Snapshot{
		TenantID:           sid.Tenant,
		StreamID:           sid.Canonical(),
		Version:            version,
		StateSchemaVersion: r.stateSchemaVersion(),
		State:              bs,
	})
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
	cfg := defaultHandleConfig()
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
