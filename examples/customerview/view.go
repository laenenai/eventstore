// Package customerview is the framework's worked example for v2
// spec-driven projection codegen (ADR 0020, cookbook 12). The
// `myapp.customerview.v1.CustomerView` proto carries an
// `(es.v1.projection)` annotation; protoc-gen-es-go reads it and
// emits a typed CustomerViewHandler interface + dispatcher.
//
// This package implements the handler against an in-memory read
// model. In production the read model would be a Postgres table; the
// shape is the same.
package customerview

import (
	"context"
	"sync"

	"github.com/laenenai/eventstore/es"
	invoicev1 "github.com/laenenai/eventstore/gen/myapp/invoice/v1"
	orderv1 "github.com/laenenai/eventstore/gen/myapp/order/v1"
)

// Row is one denormalized customer summary.
type Row struct {
	CustomerID         string
	OpenInvoiceCount   int
	PaidInvoiceCount   int
	VoidedInvoiceCount int
	OpenOrderCount     int
	ShippedOrderCount  int
	CompletedOrderCount int
	TotalInvoiceCents  int64
	LastEventGP        uint64
}

// View is the customer-view read model. Goroutine-safe.
type View struct {
	mu   sync.RWMutex
	rows map[string]*Row // keyed by customer_id

	// orderToCustomer remembers which customer owns an order, so
	// OrderShipped / OrderCompleted (which only carry order_id) can
	// route to the right summary row.
	orderToCustomer map[string]string
}

// NewView returns an empty View.
func NewView() *View {
	return &View{
		rows:            map[string]*Row{},
		orderToCustomer: map[string]string{},
	}
}

// Get returns a copy of the summary row for one customer.
func (v *View) Get(customerID string) (Row, bool) {
	v.mu.RLock()
	defer v.mu.RUnlock()
	r, ok := v.rows[customerID]
	if !ok {
		return Row{}, false
	}
	return *r, true
}

// All returns a snapshot of every row.
func (v *View) All() []Row {
	v.mu.RLock()
	defer v.mu.RUnlock()
	out := make([]Row, 0, len(v.rows))
	for _, r := range v.rows {
		out = append(out, *r)
	}
	return out
}

func (v *View) ensureRow(customerID string) *Row {
	r, ok := v.rows[customerID]
	if !ok {
		r = &Row{CustomerID: customerID}
		v.rows[customerID] = r
	}
	return r
}

// ---- Codegen'd handler interface implementation -----------------------

func (v *View) OnCreated(_ context.Context, env es.Envelope, e *invoicev1.Created) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	r := v.ensureRow(e.CustomerId)
	r.OpenInvoiceCount++
	r.TotalInvoiceCents += e.TotalCents
	r.LastEventGP = env.GlobalPosition
	return nil
}

func (v *View) OnPaid(_ context.Context, env es.Envelope, _ *invoicev1.Paid) error {
	// Paid events don't carry the customer id — but the invoice's
	// stream-id (canonical) embeds it indirectly via convention.
	// In production we'd cache invoice→customer in another map; for
	// this example we increment a tenant-wide counter on the first
	// matching row. Real-world: store customer_id IN the Paid event.
	v.mu.Lock()
	defer v.mu.Unlock()
	for _, r := range v.rows {
		// Decrement an open invoice from each customer that has one
		// — naive demo logic. A real projection looks up the invoice
		// → customer mapping (e.g., via a sibling table or by reading
		// the original Created event from the stream).
		if r.OpenInvoiceCount > 0 {
			r.OpenInvoiceCount--
			r.PaidInvoiceCount++
			r.LastEventGP = env.GlobalPosition
			return nil
		}
	}
	return nil
}

func (v *View) OnVoided(_ context.Context, env es.Envelope, _ *invoicev1.Voided) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	for _, r := range v.rows {
		if r.OpenInvoiceCount > 0 {
			r.OpenInvoiceCount--
			r.VoidedInvoiceCount++
			r.LastEventGP = env.GlobalPosition
			return nil
		}
	}
	return nil
}

func (v *View) OnOrderPlaced(_ context.Context, env es.Envelope, e *orderv1.OrderPlaced) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	r := v.ensureRow(e.CustomerId)
	r.OpenOrderCount++
	r.LastEventGP = env.GlobalPosition
	v.orderToCustomer[e.OrderId] = e.CustomerId
	return nil
}

func (v *View) OnOrderShipped(_ context.Context, env es.Envelope, e *orderv1.OrderShipped) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	custID, ok := v.orderToCustomer[e.OrderId]
	if !ok {
		// Replay scenario: OrderShipped arrived without our seeing
		// the OrderPlaced (e.g., cursor reset across a partial
		// rebuild). Skip — the projection re-runs from gp=0 will
		// fix it.
		return nil
	}
	r := v.ensureRow(custID)
	r.OpenOrderCount--
	r.ShippedOrderCount++
	r.LastEventGP = env.GlobalPosition
	return nil
}

func (v *View) OnOrderCompleted(_ context.Context, env es.Envelope, _ *orderv1.OrderCompleted) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	// Naive: increment the first row with shipped orders. Real
	// projection would look up the order→customer index again.
	for _, r := range v.rows {
		if r.ShippedOrderCount > 0 {
			r.ShippedOrderCount--
			r.CompletedOrderCount++
			r.LastEventGP = env.GlobalPosition
			return nil
		}
	}
	return nil
}
