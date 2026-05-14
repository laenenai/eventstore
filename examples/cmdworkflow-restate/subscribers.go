package cwrestate

import (
	"context"
	"sync"
	"sync/atomic"

	"google.golang.org/protobuf/proto"

	"github.com/laenenai/eventstore/cmdworkflow"
	"github.com/laenenai/eventstore/es"
	invoicev1 "github.com/laenenai/eventstore/gen/myapp/invoice/v1"
)

// ReadModel is a Sync subscriber that maintains an in-memory view of
// open invoices. Read-your-writes: HandleCmd blocks on this
// subscriber's completion before returning to the API caller (which
// in this example is Restate's ingress).
type ReadModel struct {
	mu   sync.RWMutex
	rows map[string]*invoicev1.Invoice
}

func NewReadModel() *ReadModel {
	return &ReadModel{rows: map[string]*invoicev1.Invoice{}}
}

func (r *ReadModel) Subscriber() cmdworkflow.Subscriber[invoicev1.Command] {
	return cmdworkflow.Subscriber[invoicev1.Command]{
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

func (r *ReadModel) handle(_ context.Context, env es.Envelope) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	streamID := env.StreamID.Canonical()
	switch env.TypeURL {
	case "myapp.invoice.v1.Created":
		var e invoicev1.Created
		if err := proto.Unmarshal(env.Payload, &e); err != nil {
			return err
		}
		r.rows[streamID] = &invoicev1.Invoice{
			InvoiceId:  e.InvoiceId,
			CustomerId: e.CustomerId,
			Currency:   e.Currency,
			TotalCents: e.TotalCents,
			Status:     invoicev1.Status_STATUS_OPEN,
		}
	case "myapp.invoice.v1.Paid", "myapp.invoice.v1.Voided":
		// Terminal — drop from "active" view.
		delete(r.rows, streamID)
	}
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

// AuditLog is an Async subscriber that records every event as a
// best-effort append-only log. Async means HandleCmd doesn't block
// on it — the API caller gets their response while the audit log
// catches up in the background.
type AuditLog struct {
	mu    sync.RWMutex
	calls atomic.Int32
}

func NewAuditLog() *AuditLog { return &AuditLog{} }

func (a *AuditLog) Subscriber() cmdworkflow.Subscriber[invoicev1.Command] {
	return cmdworkflow.Subscriber[invoicev1.Command]{
		Name:        "audit-log",
		Mode:        cmdworkflow.Async,
		MaxRetries:  3,
		OnExhausted: cmdworkflow.Drop, // Async DLQ also blocked on task #71
		Handle: func(_ context.Context, _ es.Envelope) error {
			a.calls.Add(1)
			return nil
		},
	}
}

// Calls returns the number of events the audit log has received.
func (a *AuditLog) Calls() int32 { return a.calls.Load() }
