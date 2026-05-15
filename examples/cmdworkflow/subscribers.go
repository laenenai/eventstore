package cmdworkflow

import (
	"context"
	"errors"
	"sync"

	"github.com/laenenai/eventstore/cmdworkflow"
	"github.com/laenenai/eventstore/es"
	invoicev1 "github.com/laenenai/eventstore/gen/myapp/invoice/v1"
)

// ============================================================
// Subscriber #1: ReadModel — Sync local-UPSERT into "active invoices"
// table. With per-batch delivery (ADR 0029), the projection becomes
// "given the post-Decide state, set the active-row to that state if
// the invoice is open, otherwise drop it" — no per-event type switch.
// ============================================================

// ReadModel is an in-memory read store keyed by stream id. Real
// implementations would be Postgres / MySQL / wherever the read side
// lives. The contract is the same: idempotent UPSERT keyed by stream
// id.
type ReadModel struct {
	mu   sync.RWMutex
	rows map[string]*invoicev1.Invoice
}

func NewReadModel() *ReadModel {
	return &ReadModel{rows: map[string]*invoicev1.Invoice{}}
}

// Subscriber returns the bus-registration entry for this read model.
// Sync + retries + DLQ: this is the read-your-writes path.
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

// handle receives the WHOLE post-Decide state on every batch — no
// re-deriving it from event payloads, no second Load. The projection
// reduces to: store the state if active, drop it otherwise. The new
// shape is dramatically simpler than the per-event type-switch the
// old per-event subscriber model required.
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
	if !ok {
		return nil, false
	}
	return row, true
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
	docs     map[string]string // streamID → typeURL of latest seen event in batch
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

func (s *SearchIndex) Subscriber() cmdworkflow.Subscriber[*invoicev1.Invoice, invoicev1.Command, invoicev1.Event] {
	return cmdworkflow.Subscriber[*invoicev1.Invoice, invoicev1.Command, invoicev1.Event]{
		Name:        "search-index-mirror",
		Mode:        cmdworkflow.Async,
		MaxRetries:  3,
		OnExhausted: cmdworkflow.DLQ,
		Handle:      s.handle,
	}
}

func (s *SearchIndex) handle(_ context.Context, envs []es.Envelope, _ *invoicev1.Invoice, _ []invoicev1.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failNext > 0 {
		s.failNext--
		return errors.New("search-index: transient API failure")
	}
	// Record the last event's type URL for the stream — enough for
	// the example's assertions.
	last := envs[len(envs)-1]
	s.docs[last.StreamID.Canonical()] = last.TypeURL
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
//
// With per-batch delivery the saga step receives the post-Decide
// state directly, so it can decide based on the in-memory Invoice
// (total cents, customer, etc.) without re-decoding events.
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

func (c *CreditReservation) Subscriber() cmdworkflow.Subscriber[*invoicev1.Invoice, invoicev1.Command, invoicev1.Event] {
	return cmdworkflow.Subscriber[*invoicev1.Invoice, invoicev1.Command, invoicev1.Event]{
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
	// Roll back: Void the invoice.
	return &invoicev1.Void{
		Reason:     "credit reservation declined",
		VoidedAtMs: envs[0].OccurredAt.UnixMilli(),
	}, nil
}

func (c *CreditReservation) ReservedFor(streamID string) (int64, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	v, ok := c.reservedFor[streamID]
	return v, ok
}
