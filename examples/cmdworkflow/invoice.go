// Package commandbus is a worked example of the workflow-orchestrated
// command bus (ADR 0025). One Invoice aggregate, three subscribers
// showcasing the three points of the subscriber matrix:
//
//   - readModel — Sync local-UPSERT into an "active invoices" table.
//     Read-your-writes for the UI.
//   - searchIndex — Async best-effort durable mirror to a fake
//     external search service. DLQ on permanent failure; recovery is
//     state_stream.Drain.
//   - reservation — Sync saga step that "reserves credit" for the
//     invoice. On exhaustion, emits a compensating Void command.
//
// All three run against the same in-memory eventstore + the inproc
// WorkflowRuntime. Production would swap inproc for Restate / DBOS;
// the Subscriber definitions don't change.
package cmdworkflow

import (
	"context"
	"errors"

	invoicev1 "github.com/laenenai/eventstore/gen/myapp/invoice/v1"
	"github.com/laenenai/eventstore/es"
)

// Decider — minimal inline version. The canonical reference lives in
// examples/invoice; this duplicate keeps the example self-contained.
var Decider = es.Decider[*invoicev1.Invoice, invoicev1.Command, invoicev1.Event]{
	Initial: func() *invoicev1.Invoice { return &invoicev1.Invoice{} },
	Decide: func(s *invoicev1.Invoice, c invoicev1.Command) ([]invoicev1.Event, []es.ConstraintOp, error) {
		switch cmd := c.(type) {
		case *invoicev1.Create:
			if s.InvoiceId != "" {
				return nil, nil, errors.New("invoice: already created")
			}
			var total int64
			for _, li := range cmd.LineItems {
				total += li.Quantity * li.UnitCents
			}
			return []invoicev1.Event{
				&invoicev1.Created{
					InvoiceId: cmd.InvoiceId, CustomerId: cmd.CustomerId,
					Currency: cmd.Currency, TotalCents: total,
					LineItems: cmd.LineItems, CreatedAtMs: cmd.CreatedAtMs,
				},
			}, nil, nil
		case *invoicev1.MarkPaid:
			if s.InvoiceId == "" {
				return nil, nil, errors.New("invoice: not created")
			}
			return []invoicev1.Event{
				&invoicev1.Paid{PaymentRef: cmd.PaymentRef, PaidAtMs: cmd.PaidAtMs},
			}, nil, nil
		case *invoicev1.Void:
			if s.InvoiceId == "" {
				return nil, nil, errors.New("invoice: not created")
			}
			return []invoicev1.Event{
				&invoicev1.Voided{Reason: cmd.Reason, VoidedAtMs: cmd.VoidedAtMs},
			}, nil, nil
		}
		return nil, nil, errors.New("invoice: unknown command")
	},
	Evolve: func(s *invoicev1.Invoice, e invoicev1.Event) *invoicev1.Invoice {
		out := *s
		switch evt := e.(type) {
		case *invoicev1.Created:
			out.InvoiceId = evt.InvoiceId
			out.CustomerId = evt.CustomerId
			out.Currency = evt.Currency
			out.TotalCents = evt.TotalCents
			out.LineItems = evt.LineItems
			out.Status = invoicev1.Status_STATUS_OPEN
		case *invoicev1.Paid:
			out.Status = invoicev1.Status_STATUS_PAID
		case *invoicev1.Voided:
			out.Status = invoicev1.Status_STATUS_VOIDED
		}
		return &out
	},
	IsTerminal: func(s *invoicev1.Invoice) bool {
		return s.Status == invoicev1.Status_STATUS_PAID || s.Status == invoicev1.Status_STATUS_VOIDED
	},
}

func ctxWithCtxKey(parent context.Context, key, val any) context.Context {
	return context.WithValue(parent, key, val)
}
