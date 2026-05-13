package linked

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	"github.com/laenenai/eventstore/es"
	"github.com/laenenai/eventstore/projection"
)

// Route maps one source event to a derived event in some destination
// stream, or signals Skip to drop the source event without producing
// anything.
type Route struct {
	// Skip drops the source event silently. The dispatcher does not
	// append anything.
	Skip bool

	// DestinationStream identifies where the derived event lands.
	// Required when Skip is false.
	DestinationStream es.StreamID

	// DerivedEvent is the typed event to append. Required when Skip
	// is false.
	DerivedEvent proto.Message

	// DerivedTypeURL is the proto full name of DerivedEvent. Caller
	// supplies it (typically read from the message descriptor).
	DerivedTypeURL string

	// SchemaVersion is the (es.v1.schema_version) of DerivedEvent.
	// Default 1.
	SchemaVersion uint32

	// ExpectedVersion is the destination stream's expected current
	// version (ADR 0009 optimistic concurrency). Pass 0 for "stream
	// must not exist yet" — typical for linked projections that
	// produce one event per source. For cumulative destination
	// streams, the caller is responsible for loading the
	// destination state and passing the right version, or accepting
	// ErrConflict and retrying.
	ExpectedVersion uint64

	// Actor optionally attributes the derived event to a synthetic
	// principal. Defaults to a "linked-projection:<projection_name>"
	// service actor when zero.
	Actor es.Actor

	// OccurredAt overrides the timestamp of the derived event.
	// Defaults to env.OccurredAt — the source event's domain time —
	// so the derived event preserves causal timing.
	OccurredAt nilTime
}

// nilTime is a small wrapper that lets callers leave OccurredAt at
// its zero value without colliding with "epoch 1970-01-01" being a
// legitimate value (which it isn't in practice). The zero value
// signals "default" to the handler.
type nilTime struct {
	Set   bool
	Value es.Envelope // unused — placeholder to keep the field type opaque
}

// RouteFn produces a Route for one source envelope. Return Skip=true
// to drop the event. Returning an error halts the projection batch
// per the standard fail-stop semantics (ADR 0020 decision 3d).
type RouteFn func(ctx context.Context, env es.Envelope) (Route, error)

// Projection is a configured LinkedProjection. Construct via New;
// install on a projection.Runtime by passing Handler() as the
// Runtime.Handler.
type Projection struct {
	cfg Config
}

// Config controls a LinkedProjection.
type Config struct {
	// Name is the linked projection's logical name. Used in the
	// uniqueness-claim scope ("linked:<name>") so different linked
	// projections deriving from the same source event don't collide.
	// Typically matches the host projection.Runtime.Name.
	Name string

	// Destination is the Store that receives the derived events.
	// Usually the same Store as the projection's source, but can
	// differ for cross-DB topologies.
	Destination es.Store

	// SourceTypeURLs filters source events. nil/empty matches every
	// envelope. Useful when one LinkedProjection routes a known set
	// of source types (the Tier-3.5 ADR's primary use case).
	SourceTypeURLs []string

	// Route maps source → derived. Required.
	Route RouteFn

	// IdempotentEmit (default true) wraps each Append in a
	// uniqueness claim on (tenant, "linked:<Name>", source.event_id).
	// On replay (cursor reset, schema rebuild), the claim conflicts
	// with the previously-claimed source id and the dispatcher
	// silently skips — no duplicate derived event. Set to false to
	// disable, but then the destination aggregate must dedupe itself.
	IdempotentEmit *bool
}

// New validates the config and returns a Projection ready to install.
func New(cfg Config) (*Projection, error) {
	if cfg.Name == "" {
		return nil, errors.New("linked: Config.Name is required")
	}
	if cfg.Destination == nil {
		return nil, errors.New("linked: Config.Destination is required")
	}
	if cfg.Route == nil {
		return nil, errors.New("linked: Config.Route is required")
	}
	return &Projection{cfg: cfg}, nil
}

// Handler returns the projection.Handler. Wire it into a
// projection.Runtime as the Handler field; everything else (cursor,
// fail-stop, LockKey) works as for any Tier-3 projection.
func (p *Projection) Handler() projection.Handler {
	filter := map[string]bool{}
	for _, t := range p.cfg.SourceTypeURLs {
		filter[t] = true
	}
	hasFilter := len(filter) > 0

	idempotent := true
	if p.cfg.IdempotentEmit != nil {
		idempotent = *p.cfg.IdempotentEmit
	}

	scope := "linked:" + p.cfg.Name

	return func(ctx context.Context, env es.Envelope) error {
		if hasFilter && !filter[env.TypeURL] {
			return nil
		}
		route, err := p.cfg.Route(ctx, env)
		if err != nil {
			return fmt.Errorf("linked %s: route: %w", p.cfg.Name, err)
		}
		if route.Skip {
			return nil
		}
		if route.DerivedEvent == nil {
			return fmt.Errorf("linked %s: route returned no DerivedEvent (and Skip=false)", p.cfg.Name)
		}

		payload, err := proto.Marshal(route.DerivedEvent)
		if err != nil {
			return fmt.Errorf("linked %s: marshal derived event: %w", p.cfg.Name, err)
		}

		actor := route.Actor
		if actor.Principal == "" {
			actor = es.Actor{
				Kind:      es.ActorService,
				Principal: "linked-projection:" + p.cfg.Name,
			}
		}

		schemaVersion := route.SchemaVersion
		if schemaVersion == 0 {
			schemaVersion = 1
		}

		params := es.AppendParams{
			StreamID:        route.DestinationStream,
			ExpectedVersion: route.ExpectedVersion,
			Events: []es.EventToAppend{{
				EventID:       uuid.Must(uuid.NewV7()),
				TypeURL:       route.DerivedTypeURL,
				SchemaVersion: schemaVersion,
				OccurredAt:    env.OccurredAt,
				CorrelationID: env.CorrelationID,
				CausationID:   env.EventID, // source event caused this derived event
				CommandID:     uuid.Must(uuid.NewV7()),
				Actor:         actor,
				Payload:       payload,
			}},
		}

		if idempotent {
			params.Constraints = []es.ConstraintOp{{
				Op:    es.ClaimConstraint,
				Scope: scope,
				Value: env.EventID.String(),
			}}
		}

		_, err = p.cfg.Destination.Append(ctx, params)
		if err != nil {
			if errors.Is(err, es.ErrConstraintViolated) && idempotent {
				// Already emitted for this source event; this is the
				// replay path — silently swallow.
				return nil
			}
			return fmt.Errorf("linked %s: append derived: %w", p.cfg.Name, err)
		}
		return nil
	}
}
