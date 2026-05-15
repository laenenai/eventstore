package sqlite

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/laenenai/eventstore/adapters/storage/sqlite/internal/db"
	"github.com/laenenai/eventstore/es"
)

// timeLayout is the canonical wire format for TIMESTAMPTZ-equivalent
// TEXT columns. RFC3339 with nanosecond precision, UTC. Matches the
// SQLite schema's strftime default closely enough for round-tripping.
const timeLayout = time.RFC3339Nano

// formatTime converts a time.Time to the canonical TEXT form.
// All adapter writes use this layout.
func formatTime(t time.Time) string {
	return t.UTC().Format(timeLayout)
}

// parseTime is the inverse of formatTime. Tries the canonical layout
// first, then falls back to a few common SQLite default forms.
func parseTime(s string) (time.Time, error) {
	for _, layout := range []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02T15:04:05.999Z",  // strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
		"2006-01-02 15:04:05.999",   // SQLite's CURRENT_TIMESTAMP-ish
	} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("sqlite: cannot parse timestamp %q", s)
}

// actorJSON is the on-disk shape of the events.actor TEXT (JSON)
// column. Mirrors the Postgres adapter for consistency.
type actorJSON struct {
	Kind       es.ActorKind      `json:"kind"`
	Principal  string            `json:"principal,omitempty"`
	OnBehalfOf string            `json:"on_behalf_of,omitempty"`
	APIKeyID   string            `json:"api_key_id,omitempty"`
	Attributes map[string]string `json:"attributes,omitempty"`
}

func encodeActor(a es.Actor) (string, error) {
	out, err := json.Marshal(actorJSON{
		Kind:       a.Kind,
		Principal:  a.Principal,
		OnBehalfOf: a.OnBehalfOf,
		APIKeyID:   a.APIKeyID,
		Attributes: a.Attributes,
	})
	if err != nil {
		return "", fmt.Errorf("encode actor: %w", err)
	}
	return string(out), nil
}

func decodeActor(s string) (es.Actor, error) {
	if s == "" {
		return es.Actor{}, nil
	}
	var aj actorJSON
	if err := json.Unmarshal([]byte(s), &aj); err != nil {
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

// rowToEnvelope converts a sqlc-generated event row to es.Envelope,
// parsing the TEXT timestamps and nullable JSON columns.
func rowToEnvelope(r db.Event) (es.Envelope, error) {
	sid, err := es.ParseCanonical(r.TenantID, r.StreamID)
	if err != nil {
		return es.Envelope{}, fmt.Errorf("parse stream id %q: %w", r.StreamID, err)
	}
	actor, err := decodeActor(r.Actor)
	if err != nil {
		return es.Envelope{}, err
	}
	occurred, err := parseTime(r.OccurredAt)
	if err != nil {
		return es.Envelope{}, fmt.Errorf("occurred_at: %w", err)
	}
	recorded, err := parseTime(r.RecordedAt)
	if err != nil {
		return es.Envelope{}, fmt.Errorf("recorded_at: %w", err)
	}

	env := es.Envelope{
		EventID:        r.EventID,
		TenantID:       r.TenantID,
		StreamID:       sid,
		Version:        uint64(r.Version),
		GlobalPosition: uint64(r.GlobalPosition),
		TypeURL:        r.TypeUrl,
		SchemaVersion:  uint32(r.SchemaVersion),
		OccurredAt:     occurred,
		RecordedAt:     recorded,
		CorrelationID:  r.CorrelationID,
		CausationID:    r.CausationID,
		CommandID:      r.CommandID,
		Actor:          actor,
		Payload:        r.Payload,
	}
	if r.PayloadJson != nil {
		env.PayloadJSON = []byte(*r.PayloadJson)
	}
	if r.EncryptionKeyRefs != nil {
		env.KeyRefs = []byte(*r.EncryptionKeyRefs)
	}
	env.Hash = r.Hash
	env.PrevHash = r.PrevHash
	return env, nil
}
