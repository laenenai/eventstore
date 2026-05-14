package cwdbosex

import (
	"context"
	"sync"
	"sync/atomic"

	"google.golang.org/protobuf/proto"

	"github.com/laenenai/eventstore/cmdworkflow"
	"github.com/laenenai/eventstore/es"
	invoicev1 "github.com/laenenai/eventstore/gen/myapp/invoice/v1"
)

// ReadModel — Sync subscriber. HandleCmd waits for it before
// returning to the caller; read-your-writes holds.
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
		OnExhausted: cmdworkflow.DLQ,
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
		delete(r.rows, streamID)
	}
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

// AuditLog — Async subscriber. Spawned as a goroutine; HandleCmd
// doesn't wait for it.
type AuditLog struct {
	calls atomic.Int32
}

func NewAuditLog() *AuditLog { return &AuditLog{} }

func (a *AuditLog) Subscriber() cmdworkflow.Subscriber[invoicev1.Command] {
	return cmdworkflow.Subscriber[invoicev1.Command]{
		Name:        "audit-log",
		Mode:        cmdworkflow.Async,
		MaxRetries:  3,
		OnExhausted: cmdworkflow.Drop,
		Handle: func(_ context.Context, _ es.Envelope) error {
			a.calls.Add(1)
			return nil
		},
	}
}

func (a *AuditLog) Calls() int32 { return a.calls.Load() }
