package cwrestate

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/laenenai/eventstore/cmdworkflow"
	"github.com/laenenai/eventstore/es"
	invoicev1 "github.com/laenenai/eventstore/gen/myapp/invoice/v1"
)

// ReadModel is a Sync subscriber that maintains an in-memory view of
// open invoices. Read-your-writes: HandleCmd blocks on this
// subscriber's completion before returning to the API caller (which
// in this example is Restate's ingress).
//
// Per-batch delivery (ADR 0029) gives the projection the full
// post-Decide state directly — store-or-delete based on terminal
// status, no per-event type switch.
type ReadModel struct {
	mu   sync.RWMutex
	rows map[string]*invoicev1.Invoice
}

func NewReadModel() *ReadModel {
	return &ReadModel{rows: map[string]*invoicev1.Invoice{}}
}

func (r *ReadModel) Subscriber() cmdworkflow.Subscriber[*invoicev1.Invoice, invoicev1.Command, invoicev1.Event] {
	return cmdworkflow.Subscriber[*invoicev1.Invoice, invoicev1.Command, invoicev1.Event]{
		Name: "active-invoices",
		Filter: cmdworkflow.EventFilter{
			TypeURLs: []string{
				"myapp.invoice.v1.Created",
				"myapp.invoice.v1.Paid",
				"myapp.invoice.v1.Voided",
			},
		},
		Mode:        cmdworkflow.Sync,
		MaxRetries:  3,
		OnExhausted: cmdworkflow.Drop, // Sync DLQ blocked on task #71
		Handle:      r.handle,
	}
}

func (r *ReadModel) handle(_ context.Context, envs []es.Envelope, state *invoicev1.Invoice, _ []invoicev1.Event) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	streamID := envs[0].StreamID.Canonical()
	if state.Status == invoicev1.Status_STATUS_PAID || state.Status == invoicev1.Status_STATUS_VOIDED {
		delete(r.rows, streamID)
		return nil
	}
	r.rows[streamID] = state
	return nil
}

// Lookup returns the current row for an invoice.
func (r *ReadModel) Lookup(streamID string) (*invoicev1.Invoice, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	row, ok := r.rows[streamID]
	return row, ok
}

// Len returns the count of active rows.
func (r *ReadModel) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.rows)
}

// AuditLog is an Async subscriber that records every command-batch as
// a best-effort append-only log. Async means HandleCmd doesn't block
// on it — the API caller gets their response while the audit log
// catches up in the background. Per-batch delivery: one Calls() bump
// per command, not per event.
type AuditLog struct {
	mu    sync.RWMutex
	calls atomic.Int32
}

func NewAuditLog() *AuditLog { return &AuditLog{} }

func (a *AuditLog) Subscriber() cmdworkflow.Subscriber[*invoicev1.Invoice, invoicev1.Command, invoicev1.Event] {
	return cmdworkflow.Subscriber[*invoicev1.Invoice, invoicev1.Command, invoicev1.Event]{
		Name:        "audit-log",
		Mode:        cmdworkflow.Async,
		MaxRetries:  3,
		OnExhausted: cmdworkflow.Drop, // Async DLQ also blocked on task #71
		Handle: func(_ context.Context, _ []es.Envelope, _ *invoicev1.Invoice, _ []invoicev1.Event) error {
			a.calls.Add(1)
			return nil
		},
	}
}

// Calls returns the number of command-batches the audit log has
// received.
func (a *AuditLog) Calls() int32 { return a.calls.Load() }
