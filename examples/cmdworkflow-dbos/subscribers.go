package cwdbosex

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/laenenai/eventstore/cmdworkflow"
	"github.com/laenenai/eventstore/es"
	invoicev1 "github.com/laenenai/eventstore/gen/myapp/invoice/v1"
)

// ReadModel — Sync subscriber. HandleCmd waits for it before
// returning to the caller; read-your-writes holds. Per-batch
// delivery (ADR 0029) hands over the full Invoice state directly,
// so the projection is one expression: store-or-delete based on
// terminal status.
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
		OnExhausted: cmdworkflow.DLQ,
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

func (r *ReadModel) Lookup(streamID string) (*invoicev1.Invoice, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	row, ok := r.rows[streamID]
	return row, ok
}

func (r *ReadModel) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.rows)
}

// AuditLog — Async subscriber. Spawned as a child workflow; HandleCmd
// doesn't wait for it. Counts batch deliveries rather than individual
// events — one Handle invocation per command.
type AuditLog struct {
	calls atomic.Int32
}

func NewAuditLog() *AuditLog { return &AuditLog{} }

func (a *AuditLog) Subscriber() cmdworkflow.Subscriber[*invoicev1.Invoice, invoicev1.Command, invoicev1.Event] {
	return cmdworkflow.Subscriber[*invoicev1.Invoice, invoicev1.Command, invoicev1.Event]{
		Name:        "audit-log",
		Mode:        cmdworkflow.Async,
		MaxRetries:  3,
		OnExhausted: cmdworkflow.Drop,
		Handle: func(_ context.Context, _ []es.Envelope, _ *invoicev1.Invoice, _ []invoicev1.Event) error {
			a.calls.Add(1)
			return nil
		},
	}
}

func (a *AuditLog) Calls() int32 { return a.calls.Load() }
