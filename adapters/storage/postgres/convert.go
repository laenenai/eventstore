package postgres

import (
	"encoding/json"
	"fmt"

	"github.com/laenenai/eventstore/adapters/storage/postgres/internal/db"
	"github.com/laenenai/eventstore/es"
)

// actorJSON is the on-disk shape of the events.actor JSONB column. It
// mirrors es.Actor with explicit JSON field names so the column is
// queryable in psql for audit ("WHERE actor->>'api_key_id' = '...'")
// without requiring the codec.
//
// Note: ADR 0005 originally specified actor as proto bytes; this
// adapter stores it as JSONB because Actor is operational metadata,
// not domain payload — see ADR 0006's "payload is proto, metadata
// can be queryable" spirit.
type actorJSON struct {
	Kind       es.ActorKind      `json:"kind"`
	Principal  string            `json:"principal,omitempty"`
	OnBehalfOf string            `json:"on_behalf_of,omitempty"`
	APIKeyID   string            `json:"api_key_id,omitempty"`
	Attributes map[string]string `json:"attributes,omitempty"`
}

// encodeActor marshals an es.Actor to canonical JSON bytes for the
// events.actor JSONB column. An empty Actor produces a minimal "{}"-
// shaped object so the column is never NULL.
func encodeActor(a es.Actor) ([]byte, error) {
	out, err := json.Marshal(actorJSON{
		Kind:       a.Kind,
		Principal:  a.Principal,
		OnBehalfOf: a.OnBehalfOf,
		APIKeyID:   a.APIKeyID,
		Attributes: a.Attributes,
	})
	if err != nil {
		return nil, fmt.Errorf("encode actor: %w", err)
	}
	return out, nil
}

// decodeActor unmarshals JSONB bytes from the events.actor column.
func decodeActor(b []byte) (es.Actor, error) {
	if len(b) == 0 {
		return es.Actor{}, nil
	}
	var aj actorJSON
	if err := json.Unmarshal(b, &aj); err != nil {
		return es.Actor{}, fmt.Errorf("decode actor: %w", err)
	}
	return es.Actor{
		Kind:       aj.Kind,
		Principal:  aj.Principal,
		OnBehalfOf: aj.OnBehalfOf,
		APIKeyID:   aj.APIKeyID,
		Attributes: aj.Attributes,
	}, nil
}

// rowToEnvelope converts a sqlc-generated event row to es.Envelope.
func rowToEnvelope(r db.Event) (es.Envelope, error) {
	sid, err := es.ParseCanonical(r.TenantID, r.StreamID)
	if err != nil {
		return es.Envelope{}, fmt.Errorf("parse stream id %q: %w", r.StreamID, err)
	}
	actor, err := decodeActor(r.Actor)
	if err != nil {
		return es.Envelope{}, err
	}
	return es.Envelope{
		EventID:        r.EventID,
		TenantID:       r.TenantID,
		StreamID:       sid,
		Version:        uint64(r.Version),
		GlobalPosition: uint64(r.GlobalPosition),
		TypeURL:        r.TypeUrl,
		SchemaVersion:  uint32(r.SchemaVersion),
		OccurredAt:     r.OccurredAt,
		RecordedAt:     r.RecordedAt,
		CorrelationID:  r.CorrelationID,
		CausationID:    r.CausationID,
		CommandID:      r.CommandID,
		Actor:          actor,
		Payload:        r.Payload,
		PayloadJSON:    r.PayloadJson,
		KeyRefs:        r.EncryptionKeyRefs,
	}, nil
}
