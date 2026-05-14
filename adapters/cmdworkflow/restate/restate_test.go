//go:build restate

package restate_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	restatesdk "github.com/restatedev/sdk-go"
	"github.com/restatedev/sdk-go/ingress"

	cwrestate "github.com/laenenai/eventstore/adapters/cmdworkflow/restate"
	invoicev1restate "github.com/laenenai/eventstore/adapters/cmdworkflow/restate/gen/myapp/invoice/v1"
	"github.com/laenenai/eventstore/adapters/cmdworkflow/restate/testsupport"
	sqliteadapter "github.com/laenenai/eventstore/adapters/storage/sqlite"
	"github.com/laenenai/eventstore/aggregate"
	"github.com/laenenai/eventstore/cmdworkflow"
	"github.com/laenenai/eventstore/es"
	invoicev1 "github.com/laenenai/eventstore/gen/myapp/invoice/v1"
)

// Build-tagged `restate` — pulls a real restatedev/restate container
// via testcontainers. Skip the default `go test ./...` run; opt in:
//
//	go test -tags restate ./adapters/cmdworkflow/restate/...

// invoiceDecider — minimal inline Invoice Decider for the Restate
// smoke test. Mirrors examples/invoice; duplicated here to keep the
// test self-contained.
var invoiceDecider = es.Decider[*invoicev1.Invoice, invoicev1.Command, invoicev1.Event]{
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

// testFixture wires SQLite eventstore + Workflow + the generated
// RestateService + Restate testcontainer.
type testFixture struct {
	env     *testsupport.Env
	wf      *cmdworkflow.Workflow[*invoicev1.Invoice, invoicev1.Command]
	adapter *sqliteadapter.Adapter
}

// fixtureOpt allows test scenarios to register subscribers before the
// workflow is wrapped in the Restate service. Multiple opts can be
// applied; they run in order.
type fixtureOpt func(wf *cmdworkflow.Workflow[*invoicev1.Invoice, invoicev1.Command])

func newFixture(t *testing.T, opts ...fixtureOpt) *testFixture {
	t.Helper()

	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	a := sqliteadapter.New(db)
	if err := a.Migrate(context.Background()); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	runtime := cwrestate.New()
	wf := cmdworkflow.New[*invoicev1.Invoice, invoicev1.Command](
		aggregate.NewProto(a, invoiceDecider, invoicev1.EventCodec{}),
		a, runtime,
	).WithDLQ(a)

	for _, opt := range opts {
		opt(wf)
	}

	svc := invoicev1restate.NewRestateService(wf)
	env := testsupport.Start(t, restatesdk.Reflect(svc))

	return &testFixture{env: env, wf: wf, adapter: a}
}

// TestRestate_Smoke verifies end-to-end: Restate container + SDK
// server + the codegen-emitted RestateService for invoicev1 + our
// cmdworkflow workflow + SQLite eventstore. One Create + one MarkPaid,
// verify the returned states reflect both.
func TestRestate_Smoke(t *testing.T) {
	fx := newFixture(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create.
	createSvc := ingress.Service[*invoicev1.Create, *invoicev1.Invoice](
		fx.env.Ingress(), "Invoice", "Create")
	created, err := createSvc.Request(ctx, &invoicev1.Create{
		TenantId:    "acme",
		InvoiceId:   "smoke-1",
		CustomerId:  "alice",
		Currency:    "USD",
		LineItems:   []*invoicev1.LineItem{{Sku: "X", Quantity: 2, UnitCents: 500}},
		CreatedAtMs: time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if created.Status != invoicev1.Status_STATUS_OPEN {
		t.Errorf("status after Create: got %v want STATUS_OPEN", created.Status)
	}
	if created.TotalCents != 1000 {
		t.Errorf("total: got %d want 1000", created.TotalCents)
	}

	// MarkPaid.
	paidSvc := ingress.Service[*invoicev1.MarkPaid, *invoicev1.Invoice](
		fx.env.Ingress(), "Invoice", "MarkPaid")
	paid, err := paidSvc.Request(ctx, &invoicev1.MarkPaid{
		TenantId:   "acme",
		InvoiceId:  "smoke-1",
		PaymentRef: "stripe_x",
		PaidAtMs:   time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("MarkPaid: %v", err)
	}
	if paid.Status != invoicev1.Status_STATUS_PAID {
		t.Errorf("status after MarkPaid: got %v want STATUS_PAID", paid.Status)
	}
}
