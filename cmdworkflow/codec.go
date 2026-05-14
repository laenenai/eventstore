package cmdworkflow

import (
	"bytes"
	"encoding/gob"
	"time"

	"github.com/google/uuid"

	"github.com/laenenai/eventstore/es"
)

// The WorkflowRuntime.Run signature uses []byte so adapters can
// journal step results without knowing application types. The bus
// encodes its own intermediate values (AppendResult, []Envelope)
// using gob — internal only, never crosses an adapter boundary other
// than as opaque bytes.

func init() {
	gob.Register(time.Time{})
}

type wireAppendResult struct {
	StartVersion        uint64
	EndVersion          uint64
	StartGlobalPosition uint64
	EndGlobalPosition   uint64
	RecordedAt          time.Time
}

func encodeAppendResult(r es.AppendResult) []byte {
	var buf bytes.Buffer
	_ = gob.NewEncoder(&buf).Encode(wireAppendResult{
		StartVersion:        r.StartVersion,
		EndVersion:          r.EndVersion,
		StartGlobalPosition: r.StartGlobalPosition,
		EndGlobalPosition:   r.EndGlobalPosition,
		RecordedAt:          r.RecordedAt,
	})
	return buf.Bytes()
}

func decodeAppendResult(b []byte) (es.AppendResult, error) {
	var w wireAppendResult
	if err := gob.NewDecoder(bytes.NewReader(b)).Decode(&w); err != nil {
		return es.AppendResult{}, err
	}
	return es.AppendResult{
		StartVersion:        w.StartVersion,
		EndVersion:          w.EndVersion,
		StartGlobalPosition: w.StartGlobalPosition,
		EndGlobalPosition:   w.EndGlobalPosition,
		RecordedAt:          w.RecordedAt,
	}, nil
}

type wireEnvelope struct {
	EventID        uuid.UUID
	TenantID       string
	StreamCanon    string
	StreamTenant   string
	StreamType     string
	StreamRawID    string
	Version        uint64
	GlobalPosition uint64
	TypeURL        string
	SchemaVersion  uint32
	OccurredAt     time.Time
	RecordedAt     time.Time
	CorrelationID  uuid.UUID
	CausationID    uuid.UUID
	CommandID      uuid.UUID
	ActorPrincipal  string
	ActorKind       int
	ActorOnBehalfOf string
	ActorAPIKeyID   string
	ActorAttributes map[string]string
	Payload        []byte
	PayloadJSON    []byte
	KeyRefs        []byte
}

func encodeEnvelopes(envs []es.Envelope) []byte {
	wires := make([]wireEnvelope, len(envs))
	for i, env := range envs {
		wires[i] = wireEnvelope{
			EventID:        env.EventID,
			TenantID:       env.TenantID,
			StreamCanon:    env.StreamID.Canonical(),
			StreamTenant:   env.StreamID.Tenant,
			StreamType:     env.StreamID.Type,
			StreamRawID:    env.StreamID.ID,
			Version:        env.Version,
			GlobalPosition: env.GlobalPosition,
			TypeURL:        env.TypeURL,
			SchemaVersion:  env.SchemaVersion,
			OccurredAt:     env.OccurredAt,
			RecordedAt:     env.RecordedAt,
			CorrelationID:  env.CorrelationID,
			CausationID:    env.CausationID,
			CommandID:      env.CommandID,
			ActorPrincipal:  env.Actor.Principal,
			ActorKind:       int(env.Actor.Kind),
			ActorOnBehalfOf: env.Actor.OnBehalfOf,
			ActorAPIKeyID:   env.Actor.APIKeyID,
			ActorAttributes: env.Actor.Attributes,
			Payload:        env.Payload,
			PayloadJSON:    env.PayloadJSON,
			KeyRefs:        env.KeyRefs,
		}
	}
	var buf bytes.Buffer
	_ = gob.NewEncoder(&buf).Encode(wires)
	return buf.Bytes()
}

func decodeEnvelopes(b []byte) ([]es.Envelope, error) {
	var wires []wireEnvelope
	if err := gob.NewDecoder(bytes.NewReader(b)).Decode(&wires); err != nil {
		return nil, err
	}
	envs := make([]es.Envelope, len(wires))
	for i, w := range wires {
		envs[i] = es.Envelope{
			EventID: w.EventID,
			StreamID: es.StreamID{
				Tenant: w.StreamTenant,
				Type:   w.StreamType,
				ID:     w.StreamRawID,
			},
			TenantID:       w.TenantID,
			Version:        w.Version,
			GlobalPosition: w.GlobalPosition,
			TypeURL:        w.TypeURL,
			SchemaVersion:  w.SchemaVersion,
			OccurredAt:     w.OccurredAt,
			RecordedAt:     w.RecordedAt,
			CorrelationID:  w.CorrelationID,
			CausationID:    w.CausationID,
			CommandID:      w.CommandID,
			Actor: es.Actor{
				Kind:       es.ActorKind(w.ActorKind),
				Principal:  w.ActorPrincipal,
				OnBehalfOf: w.ActorOnBehalfOf,
				APIKeyID:   w.ActorAPIKeyID,
				Attributes: w.ActorAttributes,
			},
			Payload:     w.Payload,
			PayloadJSON: w.PayloadJSON,
			KeyRefs:     w.KeyRefs,
		}
	}
	return envs, nil
}
