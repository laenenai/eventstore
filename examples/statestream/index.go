// Package statestream is a worked example of the framework's
// state_stream coalesced state-mirror delivery (ADR 0024). It
// implements a fake "search index" — an in-memory map keyed by
// invoice ID, with per-customer secondary lookup — that mirrors
// the current state of every Invoice aggregate via state_stream.
//
// In production this would be an Elasticsearch / Algolia / external
// Postgres receiver. The contract is the same: receive
// es.StateEnvelope, idempotently upsert by Version, done.
package statestream

import (
	"context"
	"sync"

	"google.golang.org/protobuf/encoding/protojson"

	"github.com/laenenai/eventstore/es"
	invoicev1 "github.com/laenenai/eventstore/gen/myapp/invoice/v1"
)

// Row is one denormalized row in the search-index-like read store.
type Row struct {
	InvoiceID  string
	CustomerID string
	Currency   string
	TotalCents int64
	Status     invoicev1.Status
	Version    uint64
	Terminal   bool
}

// Index is the example subscriber: a goroutine-safe in-memory store
// keyed by stream_id, with a secondary by-customer index. Implements
// es.StatePublisher.
type Index struct {
	mu      sync.RWMutex
	byID    map[string]*Row
	byCust  map[string]map[string]struct{} // customer_id → set of invoice_ids
}

// NewIndex returns an empty Index.
func NewIndex() *Index {
	return &Index{
		byID:   map[string]*Row{},
		byCust: map[string]map[string]struct{}{},
	}
}

// PublishState implements es.StatePublisher. Idempotent on Version:
// if the incoming version is less than or equal to what we already
// have, the delivery is a duplicate (retry) and is ignored.
func (i *Index) PublishState(_ context.Context, env es.StateEnvelope) error {
	if env.TypeURL != "myapp.invoice.v1.Invoice" {
		// This subscriber only mirrors invoices. Real subscribers
		// typically have one Index per TypeURL; if you want a
		// cross-aggregate index, route by TypeURL here.
		return nil
	}

	state := &invoicev1.Invoice{}
	if err := protojson.Unmarshal(env.State, state); err != nil {
		return err
	}

	i.mu.Lock()
	defer i.mu.Unlock()

	// Idempotency: skip if we already have a row at >= this version.
	// Receivers MUST do this — coalesced delivery can re-send the
	// same version after a transient failure (ADR 0024 § 2).
	if existing, ok := i.byID[env.StreamID]; ok && existing.Version >= env.Version {
		return nil
	}

	row := &Row{
		InvoiceID:  state.InvoiceId,
		CustomerID: state.CustomerId,
		Currency:   state.Currency,
		TotalCents: state.TotalCents,
		Status:     state.Status,
		Version:    env.Version,
		Terminal:   isTerminal(state.Status),
	}

	// Maintain by-customer secondary index. If the customer
	// changed (rare in real life but possible in event sourcing —
	// e.g., a refactoring), update both sides.
	if old, ok := i.byID[env.StreamID]; ok && old.CustomerID != row.CustomerID {
		i.removeFromCustomer(old.CustomerID, env.StreamID)
	}
	i.byID[env.StreamID] = row
	if _, ok := i.byCust[row.CustomerID]; !ok {
		i.byCust[row.CustomerID] = map[string]struct{}{}
	}
	i.byCust[row.CustomerID][env.StreamID] = struct{}{}
	return nil
}

func (i *Index) removeFromCustomer(customerID, streamID string) {
	if set, ok := i.byCust[customerID]; ok {
		delete(set, streamID)
		if len(set) == 0 {
			delete(i.byCust, customerID)
		}
	}
}

// Lookup returns the row for one invoice.
func (i *Index) Lookup(streamID string) (Row, bool) {
	i.mu.RLock()
	defer i.mu.RUnlock()
	r, ok := i.byID[streamID]
	if !ok {
		return Row{}, false
	}
	return *r, true
}

// FindByCustomer returns every invoice's row for one customer.
func (i *Index) FindByCustomer(customerID string) []Row {
	i.mu.RLock()
	defer i.mu.RUnlock()
	streamSet := i.byCust[customerID]
	out := make([]Row, 0, len(streamSet))
	for streamID := range streamSet {
		if r, ok := i.byID[streamID]; ok {
			out = append(out, *r)
		}
	}
	return out
}

// Len returns the total number of indexed invoices.
func (i *Index) Len() int {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return len(i.byID)
}

func isTerminal(s invoicev1.Status) bool {
	return s == invoicev1.Status_STATUS_PAID || s == invoicev1.Status_STATUS_VOIDED
}
