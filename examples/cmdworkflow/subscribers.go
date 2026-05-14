package cmdworkflow

import (
	"context"
	"errors"
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/laenenai/eventstore/cmdworkflow"
	"github.com/laenenai/eventstore/es"
	invoicev1 "github.com/laenenai/eventstore/gen/myapp/invoice/v1"
)

// ============================================================
// Subscriber #1: ReadModel — Sync local-UPSERT into "active invoices"
// table. Demonstrates the most common subscriber shape: a Tier 3
// projection that the UI queries directly, kept consistent via the
// command bus rather than a polled projection runner.
// ============================================================

// ActiveInvoice is one row in the fake read model.
type ActiveInvoice struct {
	InvoiceID  string
	CustomerID string
	TotalCents int64
	Status     invoicev1.Status
}

// ReadModel is an in-memory read store. Real implementations would
// be Postgres / MySQL / wherever the read side lives. The contract is
// the same: idempotent UPSERT keyed by stream id.
type ReadModel struct {
	mu   sync.RWMutex
	rows map[string]*ActiveInvoice
}

func NewReadModel() *ReadModel {
	return &ReadModel{rows: map[string]*ActiveInvoice{}}
}

// Subscriber returns the bus-registration entry for this read model.
// Sync + retries + DLQ: this is the read-your-writes path.
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
		r.rows[streamID] = &ActiveInvoice{
			InvoiceID:  e.InvoiceId,
			CustomerID: e.CustomerId,
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
func (r *ReadModel) Lookup(streamID string) (ActiveInvoice, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	row, ok := r.rows[streamID]
	if !ok {
		return ActiveInvoice{}, false
	}
	return *row, true
}

// Len returns the count of active rows.
func (r *ReadModel) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.rows)
}

// ============================================================
// Subscriber #2: SearchIndex — Async DLQ. Mirrors current invoice
// state to a fake external "search service". Best-effort durable;
// permanent failures land in subscriber_dlq for operator action.
// In production: typesense / algolia / elasticsearch.
// ============================================================

type SearchIndex struct {
	mu       sync.RWMutex
	docs     map[string]string // streamID → typeURL of latest seen
	failNext int               // test hook — fail this many calls
}

func NewSearchIndex() *SearchIndex {
	return &SearchIndex{docs: map[string]string{}}
}

func (s *SearchIndex) FailNext(n int) {
	s.mu.Lock()
	s.failNext = n
	s.mu.Unlock()
}

func (s *SearchIndex) Subscriber() cmdworkflow.Subscriber[invoicev1.Command] {
	return cmdworkflow.Subscriber[invoicev1.Command]{
		Name:        "search-index-mirror",
		Mode:        cmdworkflow.Async,
		MaxRetries:  3,
		OnExhausted: cmdworkflow.DLQ,
		Handle:      s.handle,
	}
}

func (s *SearchIndex) handle(_ context.Context, env es.Envelope) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failNext > 0 {
		s.failNext--
		return errors.New("search-index: transient API failure")
	}
	s.docs[env.StreamID.Canonical()] = env.TypeURL
	return nil
}

func (s *SearchIndex) Has(streamID string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.docs[streamID]
	return ok
}

// ============================================================
// Subscriber #3: CreditReservation — Sync Compensate saga step.
// On invoice creation, "reserve" the invoice amount with a fake
// credit-control service. If exhausted, emits a Void command back
// through the bus to roll back the invoice.
// ============================================================

type CreditReservation struct {
	mu          sync.RWMutex
	failNextN   int  // test hook
	approveAll  bool // when true, always succeeds (default test mode)
	reservedFor map[string]int64
}

func NewCreditReservation() *CreditReservation {
	return &CreditReservation{
		approveAll:  true,
		reservedFor: map[string]int64{},
	}
}

// SetApproveAll toggles between "always approve" (default) and
// "always exhaust → compensate".
func (c *CreditReservation) SetApproveAll(b bool) {
	c.mu.Lock()
	c.approveAll = b
	c.mu.Unlock()
}

func (c *CreditReservation) Subscriber() cmdworkflow.Subscriber[invoicev1.Command] {
	return cmdworkflow.Subscriber[invoicev1.Command]{
		Name: "credit-reservation",
		Filter: cmdworkflow.EventFilter{
			TypeURLs: []string{"myapp.invoice.v1.Created"},
		},
		Mode:        cmdworkflow.Sync,
		MaxRetries:  2, // small budget — fail fast in the saga
		OnExhausted: cmdworkflow.Compensate,
		Handle:      c.handle,
		Compensate:  c.compensate,
	}
}

func (c *CreditReservation) handle(_ context.Context, env es.Envelope) error {
	c.mu.RLock()
	approve := c.approveAll
	c.mu.RUnlock()
	if !approve {
		return errors.New("credit-reservation: declined")
	}
	var e invoicev1.Created
	if err := proto.Unmarshal(env.Payload, &e); err != nil {
		return err
	}
	c.mu.Lock()
	c.reservedFor[env.StreamID.Canonical()] = e.TotalCents
	c.mu.Unlock()
	return nil
}

func (c *CreditReservation) compensate(_ context.Context, env es.Envelope) (invoicev1.Command, error) {
	// Roll back: Void the invoice.
	return &invoicev1.Void{
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
