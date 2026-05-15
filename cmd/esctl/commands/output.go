package commands

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/laenenai/eventstore/es"
)

// Renderer emits read results. Two formats:
//
//	pretty — human-friendly multi-line per record, with optional ANSI colour.
//	json   — one JSON object per line; pipe into jq.
type Renderer struct {
	Out     io.Writer
	Format  string // "pretty" | "json"
	NoColor bool
}

// NewRenderer constructs from CLI flags.
func NewRenderer(format string, noColor bool) *Renderer {
	return &Renderer{Out: os.Stdout, Format: format, NoColor: noColor}
}

func (r *Renderer) Println(s string) { fmt.Fprintln(r.Out, s) }

// Envelope renders one event envelope.
func (r *Renderer) Envelope(e es.Envelope) error {
	switch r.Format {
	case "json":
		return r.json(envelopeJSON(e))
	default:
		return r.prettyEnvelope(e)
	}
}

// StateRow renders one state_cache row.
func (r *Renderer) StateRow(row es.StateCacheRow) error {
	switch r.Format {
	case "json":
		return r.json(stateRowJSON(row))
	default:
		return r.prettyStateRow(row)
	}
}

// StreamSummary is a short per-stream line for list output.
type StreamSummary struct {
	StreamID      string    `json:"stream_id"`
	TypeURL       string    `json:"type_url"`
	Version       uint64    `json:"version"`
	Terminal      bool      `json:"terminal"`
	SchemaVersion uint32    `json:"state_schema_version"`
	UpdatedAt     time.Time `json:"updated_at"`
}

func (r *Renderer) StreamSummary(s StreamSummary) error {
	switch r.Format {
	case "json":
		return r.json(s)
	default:
		fmt.Fprintf(r.Out, "%s%s%s  v%d  %s  %s\n",
			r.bold(), s.StreamID, r.reset(),
			s.Version,
			r.dim()+s.TypeURL+r.reset(),
			r.terminalMarker(s.Terminal))
		return nil
	}
}

// OutboxRow renders one pending outbox entry.
type OutboxRow struct {
	TenantID       string    `json:"tenant_id"`
	GlobalPosition int64     `json:"global_position"`
	EventID        string    `json:"event_id"`
	EnqueuedAt     time.Time `json:"enqueued_at"`
	Attempts       int64     `json:"attempts"`
	LastError      string    `json:"last_error,omitempty"`
	NextAttemptAt  string    `json:"next_attempt_at,omitempty"`
}

func (r *Renderer) Outbox(row OutboxRow) error {
	switch r.Format {
	case "json":
		return r.json(row)
	default:
		fmt.Fprintf(r.Out, "gp=%-8d  %s  attempts=%d  enqueued=%s",
			row.GlobalPosition, row.EventID, row.Attempts,
			row.EnqueuedAt.Format(time.RFC3339))
		if row.LastError != "" {
			fmt.Fprintf(r.Out, "  err=%q", truncate(row.LastError, 80))
		}
		fmt.Fprintln(r.Out)
		return nil
	}
}

// VerifyResult is the structured outcome of `stream verify`.
type VerifyResult struct {
	StreamID   string `json:"stream_id"`
	OK         bool   `json:"ok"`
	BrokenAt   uint64 `json:"broken_at_version,omitempty"`
	EventCount int    `json:"event_count"`
	Message    string `json:"message,omitempty"`
}

func (r *Renderer) Verify(v VerifyResult) error {
	switch r.Format {
	case "json":
		return r.json(v)
	default:
		if v.OK {
			fmt.Fprintf(r.Out, "%sOK%s  stream=%s  events=%d\n",
				r.green(), r.reset(), v.StreamID, v.EventCount)
		} else {
			fmt.Fprintf(r.Out, "%sBROKEN%s  stream=%s  broken_at_version=%d  events=%d  %s\n",
				r.red(), r.reset(), v.StreamID, v.BrokenAt, v.EventCount, v.Message)
		}
		return nil
	}
}

// ---- pretty helpers ---------------------------------------------------

func (r *Renderer) prettyEnvelope(e es.Envelope) error {
	bold := r.bold()
	dim := r.dim()
	reset := r.reset()

	fmt.Fprintf(r.Out, "%s[v%d gp=%d]%s %s %s(%s)%s\n",
		bold, e.Version, e.GlobalPosition, reset,
		e.TypeURL,
		dim, e.StreamID.Canonical(), reset)
	fmt.Fprintf(r.Out, "  event_id:      %s\n", e.EventID)
	fmt.Fprintf(r.Out, "  occurred_at:   %s\n", e.OccurredAt.Format(time.RFC3339Nano))
	fmt.Fprintf(r.Out, "  recorded_at:   %s\n", e.RecordedAt.Format(time.RFC3339Nano))
	if e.CorrelationID.String() != "00000000-0000-0000-0000-000000000000" {
		fmt.Fprintf(r.Out, "  correlation:   %s\n", e.CorrelationID)
	}
	if e.Actor.Principal != "" {
		fmt.Fprintf(r.Out, "  actor:         %s/%s\n", actorKind(int(e.Actor.Kind)), e.Actor.Principal)
	}
	if len(e.Hash) > 0 {
		fmt.Fprintf(r.Out, "  hash:          %s\n", hex.EncodeToString(e.Hash))
	}
	if len(e.PrevHash) > 0 {
		fmt.Fprintf(r.Out, "  prev_hash:     %s\n", hex.EncodeToString(e.PrevHash))
	}
	if len(e.Payload) > 0 {
		fmt.Fprintf(r.Out, "  payload:       %d bytes (hex: %s)\n",
			len(e.Payload), truncate(hex.EncodeToString(e.Payload), 64))
	}
	if len(e.KeyRefs) > 0 {
		fmt.Fprintf(r.Out, "  key_refs:      %s\n", string(e.KeyRefs))
	}
	fmt.Fprintln(r.Out)
	return nil
}

func (r *Renderer) prettyStateRow(row es.StateCacheRow) error {
	fmt.Fprintf(r.Out, "%s%s%s  v%d  %s  %s\n",
		r.bold(), row.StreamID, r.reset(),
		row.Version,
		row.TypeURL,
		r.terminalMarker(row.Terminal))
	fmt.Fprintf(r.Out, "  state_schema_version: %d\n", row.StateSchemaVersion)
	fmt.Fprintf(r.Out, "  updated_at:           %s\n", row.UpdatedAt.Format(time.RFC3339))
	fmt.Fprintf(r.Out, "  state:                %d bytes\n", len(row.State))
	fmt.Fprintln(r.Out)
	return nil
}

// ---- JSON shapes ------------------------------------------------------

type envelopeOut struct {
	EventID        string `json:"event_id"`
	TenantID       string `json:"tenant_id"`
	StreamID       string `json:"stream_id"`
	Version        uint64 `json:"version"`
	GlobalPosition uint64 `json:"global_position"`
	TypeURL        string `json:"type_url"`
	SchemaVersion  uint32 `json:"schema_version"`
	OccurredAt     string `json:"occurred_at"`
	RecordedAt     string `json:"recorded_at"`
	CorrelationID  string `json:"correlation_id,omitempty"`
	CausationID    string `json:"causation_id,omitempty"`
	CommandID      string `json:"command_id,omitempty"`
	Actor          struct {
		Kind      string `json:"kind"`
		Principal string `json:"principal"`
	} `json:"actor,omitempty"`
	PayloadLen    int    `json:"payload_bytes"`
	PayloadHex    string `json:"payload_hex,omitempty"`
	KeyRefsJSON   string `json:"encryption_key_refs,omitempty"`
	Hash          string `json:"hash,omitempty"`
	PrevHash      string `json:"prev_hash,omitempty"`
}

func envelopeJSON(e es.Envelope) envelopeOut {
	out := envelopeOut{
		EventID:        e.EventID.String(),
		TenantID:       e.TenantID,
		StreamID:       e.StreamID.Canonical(),
		Version:        e.Version,
		GlobalPosition: e.GlobalPosition,
		TypeURL:        e.TypeURL,
		SchemaVersion:  e.SchemaVersion,
		OccurredAt:     e.OccurredAt.Format(time.RFC3339Nano),
		RecordedAt:     e.RecordedAt.Format(time.RFC3339Nano),
		PayloadLen:     len(e.Payload),
		PayloadHex:     hex.EncodeToString(e.Payload),
		Hash:           hex.EncodeToString(e.Hash),
		PrevHash:       hex.EncodeToString(e.PrevHash),
	}
	if !isZeroUUID(e.CorrelationID.String()) {
		out.CorrelationID = e.CorrelationID.String()
	}
	if !isZeroUUID(e.CausationID.String()) {
		out.CausationID = e.CausationID.String()
	}
	if !isZeroUUID(e.CommandID.String()) {
		out.CommandID = e.CommandID.String()
	}
	out.Actor.Kind = actorKind(int(e.Actor.Kind))
	out.Actor.Principal = e.Actor.Principal
	if len(e.KeyRefs) > 0 {
		out.KeyRefsJSON = string(e.KeyRefs)
	}
	return out
}

type stateRowOut struct {
	TenantID           string `json:"tenant_id"`
	StreamID           string `json:"stream_id"`
	TypeURL            string `json:"type_url"`
	Version            uint64 `json:"version"`
	Terminal           bool   `json:"terminal"`
	StateSchemaVersion uint32 `json:"state_schema_version"`
	StateBytes         int    `json:"state_bytes"`
	UpdatedAt          string `json:"updated_at"`
}

func stateRowJSON(row es.StateCacheRow) stateRowOut {
	return stateRowOut{
		TenantID:           row.TenantID,
		StreamID:           row.StreamID,
		TypeURL:            row.TypeURL,
		Version:            row.Version,
		Terminal:           row.Terminal,
		StateSchemaVersion: row.StateSchemaVersion,
		StateBytes:         len(row.State),
		UpdatedAt:          row.UpdatedAt.Format(time.RFC3339),
	}
}

func (r *Renderer) json(v any) error {
	enc := json.NewEncoder(r.Out)
	return enc.Encode(v)
}

// ---- ANSI helpers -----------------------------------------------------

func (r *Renderer) bold() string {
	if r.NoColor {
		return ""
	}
	return "\033[1m"
}
func (r *Renderer) dim() string {
	if r.NoColor {
		return ""
	}
	return "\033[2m"
}
func (r *Renderer) reset() string {
	if r.NoColor {
		return ""
	}
	return "\033[0m"
}
func (r *Renderer) green() string {
	if r.NoColor {
		return ""
	}
	return "\033[32m"
}
func (r *Renderer) red() string {
	if r.NoColor {
		return ""
	}
	return "\033[31m"
}

func (r *Renderer) terminalMarker(t bool) string {
	if !t {
		return ""
	}
	return r.dim() + "[terminal]" + r.reset()
}

// ---- misc helpers -----------------------------------------------------

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

func actorKind(k int) string {
	switch k {
	case 1:
		return "user"
	case 2:
		return "system"
	case 3:
		return "service"
	case 4:
		return "integration"
	default:
		return "unspecified"
	}
}

func isZeroUUID(s string) bool {
	return s == "" || s == "00000000-0000-0000-0000-000000000000"
}

