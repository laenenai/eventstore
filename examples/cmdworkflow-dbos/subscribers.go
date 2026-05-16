package cwdbosex

import (
	"context"
	"errors"
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

// CreditReservation — Sync subscriber with OnExhausted = Compensate.
// Mirrors the inproc example's saga step (examples/cmdworkflow). On
// Created, "reserves" the invoice amount with a fake external credit
// authority; on a deterministic decline, the framework's compensation
// path emits the supplied rollback command (Void) back through the
// same bus.
//
// Why this matters for DBOS specifically: the compensating recursion
// runs under the same DBOSContext as the original HandleCmd, prefixed
// with "compensate:<sub>:<event>:" step names so DBOS journals it
// distinctly from the primary handler (see adapters/cmdworkflow/dbos
// dbos_test.go § TestDBOS_SyncCompensate). A crash mid-compensation
// resumes from the journal — the example proves the contract holds
// end-to-end through the codegen-emitted DBOSService.
type CreditReservation struct {
	mu          sync.RWMutex
	approveAll  bool
	reservedFor map[string]int64
}

func NewCreditReservation() *CreditReservation {
	return &CreditReservation{approveAll: true, reservedFor: map[string]int64{}}
}

// SetApproveAll toggles between "always approve" and "always decline →
// compensate". The decline path is deterministic so the saga test
// doesn't depend on retry timing.
func (c *CreditReservation) SetApproveAll(b bool) {
	c.mu.Lock()
	c.approveAll = b
	c.mu.Unlock()
}

func (c *CreditReservation) Subscriber() cmdworkflow.Subscriber[*invoicev1.Invoice, invoicev1.Command, invoicev1.Event] {
	return cmdworkflow.Subscriber[*invoicev1.Invoice, invoicev1.Command, invoicev1.Event]{
		Name:        "credit-reservation",
		Filter:      cmdworkflow.EventFilter{TypeURLs: []string{"myapp.invoice.v1.Created"}},
		Mode:        cmdworkflow.Sync,
		MaxRetries:  2, // tight budget — saga steps fail fast
		OnExhausted: cmdworkflow.Compensate,
		Handle:      c.handle,
		Compensate:  c.compensate,
	}
}

func (c *CreditReservation) handle(_ context.Context, envs []es.Envelope, state *invoicev1.Invoice, _ []invoicev1.Event) error {
	c.mu.RLock()
	approve := c.approveAll
	c.mu.RUnlock()
	if !approve {
		return errors.New("credit-reservation: declined")
	}
	c.mu.Lock()
	c.reservedFor[envs[0].StreamID.Canonical()] = state.TotalCents
	c.mu.Unlock()
	return nil
}

func (c *CreditReservation) compensate(_ context.Context, envs []es.Envelope, _ *invoicev1.Invoice, _ []invoicev1.Event) (invoicev1.Command, error) {
	env := envs[0]
	return &invoicev1.Void{
		TenantId:   env.TenantID,
		InvoiceId:  env.StreamID.ID,
		Reason:     "credit reservation declined",
		VoidedAtMs: env.OccurredAt.UnixMilli(),
	}, nil
}

func (c *CreditReservation) ReservedFor(streamID string) (int64, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	v, ok := c.reservedFor[streamID]
	return v, ok
}
