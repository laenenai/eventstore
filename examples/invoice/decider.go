// Package invoice is a worked example of the framework's Tier 1
// state_cache + IsTerminal features. See examples/invoice/README.md
// for the walkthrough.
package invoice

import (
	"errors"

	"github.com/laenenai/eventstore/es"
	invoicev1 "github.com/laenenai/eventstore/gen/myapp/invoice/v1"
)

// Domain errors. Match via errors.Is.
var (
	ErrAlreadyCreated = errors.New("invoice: already created")
	ErrNotCreated     = errors.New("invoice: not yet created")
	ErrAlreadyClosed  = errors.New("invoice: already closed (paid or voided)")
	ErrEmptyLineItems = errors.New("invoice: requires at least one line item")
	ErrEmptyCurrency  = errors.New("invoice: currency is required")
	ErrUnknownCmd     = errors.New("invoice: unknown command")
)

// Decider is the framework Decider for the Invoice aggregate.
var Decider = es.Decider[*invoicev1.Invoice, invoicev1.Command, invoicev1.Event]{
	Initial: func() *invoicev1.Invoice { return &invoicev1.Invoice{} },

	Decide: func(s *invoicev1.Invoice, c invoicev1.Command) ([]invoicev1.Event, []es.ConstraintOp, error) {
		switch cmd := c.(type) {
		case *invoicev1.Create:
			if s.InvoiceId != "" {
				return nil, nil, ErrAlreadyCreated
			}
			if len(cmd.LineItems) == 0 {
				return nil, nil, ErrEmptyLineItems
			}
			if cmd.Currency == "" {
				return nil, nil, ErrEmptyCurrency
			}
			var total int64
			for _, li := range cmd.LineItems {
				total += li.Quantity * li.UnitCents
			}
			return []invoicev1.Event{
				&invoicev1.Created{
					InvoiceId:   cmd.InvoiceId,
					CustomerId:  cmd.CustomerId,
					Currency:    cmd.Currency,
					TotalCents:  total,
					LineItems:   cmd.LineItems,
					CreatedAtMs: cmd.CreatedAtMs,
				},
			}, nil, nil

		case *invoicev1.MarkPaid:
			if s.InvoiceId == "" {
				return nil, nil, ErrNotCreated
			}
			// IsTerminal would already block this; defensive check anyway.
			if s.Status != invoicev1.Status_STATUS_OPEN {
				return nil, nil, ErrAlreadyClosed
			}
			return []invoicev1.Event{
				&invoicev1.Paid{PaymentRef: cmd.PaymentRef, PaidAtMs: cmd.PaidAtMs},
			}, nil, nil

		case *invoicev1.Void:
			if s.InvoiceId == "" {
				return nil, nil, ErrNotCreated
			}
			if s.Status != invoicev1.Status_STATUS_OPEN {
				return nil, nil, ErrAlreadyClosed
			}
			return []invoicev1.Event{
				&invoicev1.Voided{Reason: cmd.Reason, VoidedAtMs: cmd.VoidedAtMs},
			}, nil, nil
		}
		return nil, nil, ErrUnknownCmd
	},

	Evolve: func(s *invoicev1.Invoice, e invoicev1.Event) *invoicev1.Invoice {
		out := cloneState(s)
		switch evt := e.(type) {
		case *invoicev1.Created:
			out.InvoiceId = evt.InvoiceId
			out.CustomerId = evt.CustomerId
			out.Currency = evt.Currency
			out.TotalCents = evt.TotalCents
			out.LineItems = append([]*invoicev1.LineItem(nil), evt.LineItems...)
			out.CreatedAtMs = evt.CreatedAtMs
			out.Status = invoicev1.Status_STATUS_OPEN
		case *invoicev1.Paid:
			out.Status = invoicev1.Status_STATUS_PAID
			out.PaymentRef = evt.PaymentRef
			out.ClosedAtMs = evt.PaidAtMs
		case *invoicev1.Voided:
			out.Status = invoicev1.Status_STATUS_VOIDED
			out.VoidReason = evt.Reason
			out.ClosedAtMs = evt.VoidedAtMs
		}
		return out
	},

	// IsTerminal closes the stream once paid or voided. The aggregate
	// runtime rejects further commands with es.ErrTerminal — see
	// ADR 0003 and the runtime check.
	IsTerminal: func(s *invoicev1.Invoice) bool {
		return s.Status == invoicev1.Status_STATUS_PAID ||
			s.Status == invoicev1.Status_STATUS_VOIDED
	},
}

// cloneState makes a shallow clone of the state struct so Evolve
// doesn't mutate the caller's reference.
func cloneState(s *invoicev1.Invoice) *invoicev1.Invoice {
	if s == nil {
		return &invoicev1.Invoice{}
	}
	return &invoicev1.Invoice{
		InvoiceId:   s.InvoiceId,
		CustomerId:  s.CustomerId,
		Currency:    s.Currency,
		TotalCents:  s.TotalCents,
		Status:      s.Status,
		LineItems:   s.LineItems,
		CreatedAtMs: s.CreatedAtMs,
		ClosedAtMs:  s.ClosedAtMs,
		PaymentRef:  s.PaymentRef,
		VoidReason:  s.VoidReason,
	}
}
