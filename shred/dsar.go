package shred

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/laenenai/eventstore/es"
)

// uuidString returns the canonical string form, or "" for the zero
// UUID (so omitempty on the JSON tag drops empty causality fields).
func uuidString(u uuid.UUID) string {
	if u == uuid.Nil {
		return ""
	}
	return u.String()
}

// SubjectInspector inspects one envelope and reports whether it carries
// data about a given subject, returning a redacted representation of
// the event suitable for inclusion in a DSAR export. Adopters implement
// one Inspector per aggregate (or one chained Inspector across
// aggregates) and pass it to RunSubjectExport.
//
// The runner hands every envelope in the tenant's stream to Inspect.
// The Inspector decodes the payload via its aggregate's Codec, applies
// crypto-shredding decryption (calling DecryptPII through the
// Shredder), narrows the result to the caller's access level via
// View(), and serializes to JSON. Returning (nil, nil) means "this
// envelope doesn't reference the requested subject" — the runner skips
// without recording.
//
// Why type-erased: a subject (one human, one customer) typically has
// events across many aggregates. The framework cannot maintain a
// global registry of event types — that lives in the codegen output
// of each adopter's proto packages. The Inspector lets the adopter
// pick which aggregates participate in DSAR export and how each
// reduces its events to subject-relevant JSON. Codegen of an
// Inspector per aggregate is a future addition (see ADR 0027).
type SubjectInspector interface {
	// Inspect returns the redacted JSON payload for env iff env
	// references subject. Returning (nil, nil) is the "skip" signal;
	// returning (nil, err) propagates a hard failure (decoder error,
	// shredder error). The Inspector MUST NOT include un-redacted PII
	// of OTHER subjects — its job is to produce the AccessLevelSubject
	// view of the event for THIS subject.
	Inspect(ctx context.Context, env es.Envelope, subject string) ([]byte, error)
}

// SubjectInspectorChain composes multiple Inspectors. The first
// Inspector to return a non-nil payload wins; subsequent Inspectors
// are not consulted for that envelope. This matches the per-event
// "which aggregate owns this TypeURL" partitioning.
type SubjectInspectorChain []SubjectInspector

// Inspect implements SubjectInspector by walking the chain in order.
func (c SubjectInspectorChain) Inspect(ctx context.Context, env es.Envelope, subject string) ([]byte, error) {
	for _, ins := range c {
		out, err := ins.Inspect(ctx, env, subject)
		if err != nil {
			return nil, err
		}
		if out != nil {
			return out, nil
		}
	}
	return nil, nil
}

// SubjectExportSource is the read surface the export runner needs.
// es.Store satisfies it via ReadAllForTenant; adopters can pass a
// narrower wrapper if they want to limit the range (e.g., "events
// after position N" for incremental DSAR refreshes).
type SubjectExportSource interface {
	ReadAllForTenant(ctx context.Context, tenantID string, fromPosition uint64, limit int) ([]es.Envelope, error)
}

// SubjectExportRequest is the input to RunSubjectExport.
type SubjectExportRequest struct {
	// TenantID scopes the read. Required. The export covers every
	// stream in this tenant; subject identifiers are typically
	// globally unique within a tenant.
	TenantID string

	// Subject is the data subject id (the value of subject_field on
	// events that carry one). Required.
	Subject string

	// FromPosition lets adopters resume an incremental DSAR: pass the
	// EndGlobalPosition of the previous export. Zero (the default)
	// starts at the beginning of the tenant's stream.
	FromPosition uint64

	// BatchSize is the read page size. Default 200.
	BatchSize int

	// MaxRecords caps the size of an export. Zero means unlimited.
	// Useful when the SLA mandates "return what we found within an
	// hour" and the rest follows in a paginated response.
	MaxRecords int
}

// SubjectExportRecord is one entry in a DSAR export.
type SubjectExportRecord struct {
	EventID        string    `json:"event_id"`
	TenantID       string    `json:"tenant_id"`
	StreamID       string    `json:"stream_id"`
	TypeURL        string    `json:"type_url"`
	SchemaVersion  uint32    `json:"schema_version"`
	Version        uint64    `json:"version"`
	GlobalPosition uint64    `json:"global_position"`
	OccurredAt     time.Time `json:"occurred_at"`
	RecordedAt     time.Time `json:"recorded_at"`
	CorrelationID  string    `json:"correlation_id,omitempty"`
	CausationID    string    `json:"causation_id,omitempty"`
	CommandID      string    `json:"command_id,omitempty"`

	// Payload is the Inspector's redacted JSON view of the event.
	// Already narrowed to AccessLevelSubject by the Inspector.
	Payload json.RawMessage `json:"payload"`
}

// SubjectExportResult bundles the records with the export's final
// cursor position, suitable for resumable DSAR pipelines.
type SubjectExportResult struct {
	Records []SubjectExportRecord `json:"records"`

	// LastPosition is the highest global_position observed during the
	// export. Pass this as the next request's FromPosition to resume
	// where this run left off. Zero if no events were observed.
	LastPosition uint64 `json:"last_position"`

	// Truncated is true when MaxRecords was reached before the
	// tenant's stream was exhausted. Callers should re-issue with
	// FromPosition = LastPosition to fetch the remainder.
	Truncated bool `json:"truncated"`
}

// RunSubjectExport walks every event in the tenant's stream after
// FromPosition, asks the Inspector whether each one references the
// subject, and accumulates the Inspector's redacted JSON payloads
// into a SubjectExportResult.
//
// The runner does NOT decrypt anything itself — that's the
// Inspector's job, because decryption requires the typed Codec of
// the originating aggregate. The runner is a pure read+filter loop;
// the Inspector is the per-aggregate compliance code.
//
// Output ordering matches the global_position order returned by the
// Store: earliest first, which is the order regulators expect ("show
// me everything you have on this person, chronologically").
//
// Errors from the Inspector surface immediately and abort the
// export — better to fail loudly than to ship a regulator a
// partially-redacted document.
func RunSubjectExport(
	ctx context.Context,
	src SubjectExportSource,
	inspector SubjectInspector,
	req SubjectExportRequest,
) (SubjectExportResult, error) {
	if err := req.validate(); err != nil {
		return SubjectExportResult{}, err
	}
	if inspector == nil {
		return SubjectExportResult{}, errors.New("shred: RunSubjectExport: inspector is required")
	}

	batchSize := req.BatchSize
	if batchSize <= 0 {
		batchSize = 200
	}

	cursor := req.FromPosition
	var (
		records   []SubjectExportRecord
		truncated bool
		last      uint64
	)
	for {
		if err := ctx.Err(); err != nil {
			return SubjectExportResult{}, err
		}
		envs, err := src.ReadAllForTenant(ctx, req.TenantID, cursor, batchSize)
		if err != nil {
			return SubjectExportResult{}, fmt.Errorf("shred: DSAR read tenant=%s cursor=%d: %w",
				req.TenantID, cursor, err)
		}
		if len(envs) == 0 {
			break
		}
		for _, env := range envs {
			payload, err := inspector.Inspect(ctx, env, req.Subject)
			if err != nil {
				return SubjectExportResult{}, fmt.Errorf(
					"shred: DSAR inspect event=%s tenant=%s subject=%s: %w",
					env.EventID, req.TenantID, req.Subject, err)
			}
			last = env.GlobalPosition
			if payload == nil {
				continue
			}
			records = append(records, SubjectExportRecord{
				EventID:        env.EventID.String(),
				TenantID:       env.TenantID,
				StreamID:       env.StreamID.Canonical(),
				TypeURL:        env.TypeURL,
				SchemaVersion:  env.SchemaVersion,
				Version:        env.Version,
				GlobalPosition: env.GlobalPosition,
				OccurredAt:     env.OccurredAt,
				RecordedAt:     env.RecordedAt,
				CorrelationID:  uuidString(env.CorrelationID),
				CausationID:    uuidString(env.CausationID),
				CommandID:      uuidString(env.CommandID),
				Payload:        payload,
			})
			if req.MaxRecords > 0 && len(records) >= req.MaxRecords {
				truncated = true
				return SubjectExportResult{
					Records:      records,
					LastPosition: last,
					Truncated:    truncated,
				}, nil
			}
		}
		cursor = last
		if len(envs) < batchSize {
			break
		}
	}
	return SubjectExportResult{
		Records:      records,
		LastPosition: last,
		Truncated:    false,
	}, nil
}

func (r SubjectExportRequest) validate() error {
	if r.TenantID == "" {
		return errors.New("shred: SubjectExportRequest.TenantID is required")
	}
	if r.Subject == "" {
		return errors.New("shred: SubjectExportRequest.Subject is required")
	}
	return nil
}
