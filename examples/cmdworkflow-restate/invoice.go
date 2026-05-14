// Package cwrestate is a worked example of the cmdworkflow framework
// running against a real Restate cluster (Phase 2a of ADR 0026).
// Same Invoice aggregate as examples/cmdworkflow; same Decider;
// same subscriber matrix. The only differences from the inproc
// example:
//
//  1. The WorkflowRuntime is cwrestate.New() instead of inproc.New().
//  2. The bus is wrapped in the codegen-emitted RestateService
//     (invoicev1restate.RestateService) which Restate's Reflect
//     binds as a service.
//  3. Commands flow in via the Restate ingress, not direct
//     wf.HandleCmd calls.
//
// Run the test (requires Docker for the Restate testcontainer):
//
//	cd examples/cmdworkflow-restate
//	go test ./...
package cwrestate

import (
	"errors"

	"github.com/laenenai/eventstore/es"
	invoicev1 "github.com/laenenai/eventstore/gen/myapp/invoice/v1"
)

// Decider — same as examples/cmdworkflow's, duplicated here so the
// example module stays self-contained. (Sharing Deciders across
// example modules would require a third "shared" example module
// importable by both; not worth the indirection.)
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
		// Clone — never mutate the input.
		out := &invoicev1.Invoice{
			InvoiceId:  s.InvoiceId,
			CustomerId: s.CustomerId,
			Currency:   s.Currency,
			TotalCents: s.TotalCents,
			Status:     s.Status,
			LineItems:  s.LineItems,
		}
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
		return out
	},
	IsTerminal: func(s *invoicev1.Invoice) bool {
		return s.Status == invoicev1.Status_STATUS_PAID || s.Status == invoicev1.Status_STATUS_VOIDED
	},
}
