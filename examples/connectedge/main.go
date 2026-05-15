// Command connectedge is the worked example for cookbook recipe 15:
// exposing a workflow-orchestrated command bus over HTTP via Connect.
//
// The wiring is the whole point — once the bus exists, each Connect
// RPC is one call to connectedge.Dispatch. There is no codegen for
// the HTTP surface; the framework deliberately keeps transport
// choices in user space (see recipe 15 for the reasoning).
package main

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"net/http"

	"connectrpc.com/connect"
	_ "modernc.org/sqlite"

	cwinproc "github.com/laenenai/eventstore/adapters/cmdworkflow/inproc"
	connectedge "github.com/laenenai/eventstore/adapters/httpedge/connect"
	sqliteadapter "github.com/laenenai/eventstore/adapters/storage/sqlite"
	"github.com/laenenai/eventstore/aggregate"
	"github.com/laenenai/eventstore/cmdworkflow"
	"github.com/laenenai/eventstore/es"
	"github.com/laenenai/eventstore/examples/employee"
	employeev1 "github.com/laenenai/eventstore/gen/myapp/employee/v1"
)

// TenantHeader is read by the decoders below to scope the StreamID.
// In production this is typically derived from auth middleware
// (JWT claim, mTLS subject, etc.) — see recipe 15 § "auth & tenant".
const TenantHeader = "X-Tenant"

// EmployeeBus is the per-aggregate command bus. The bus type is fully
// generic over (S, C, E); newServer takes one as input so tests can
// substitute fakes.
type EmployeeBus = *cmdworkflow.Workflow[*employeev1.Employee, employeev1.Command, employeev1.Event]

func main() {
	db, err := sql.Open("sqlite", "file:edge.db?cache=shared")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	bus, err := buildBus(context.Background(), db)
	if err != nil {
		log.Fatal(err)
	}

	mux := newMux(bus)
	log.Println("connectedge listening on :8080")
	log.Fatal(http.ListenAndServe(":8080", mux))
}

// buildBus wires the storage adapter, aggregate runtime, and workflow
// runtime into a cmdworkflow.Workflow ready to receive commands.
func buildBus(ctx context.Context, db *sql.DB) (EmployeeBus, error) {
	a := sqliteadapter.New(db)
	if err := a.Migrate(ctx); err != nil {
		return nil, err
	}
	bus := cmdworkflow.New[*employeev1.Employee, employeev1.Command, employeev1.Event](
		aggregate.NewProto(a, employee.Decider, employeev1.EventCodec{}),
		a, cwinproc.New(), employeev1.EventCodec{},
	)
	return bus, nil
}

// newMux wires each command to a Connect unary handler. The procedure
// paths follow Connect's `/<package>.<service>/<method>` convention so
// generated clients (in any language) line up if someone later adds
// the codegen step.
func newMux(bus EmployeeBus) *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("/myapp.employee.v1.EmployeeService/Hire",
		connect.NewUnaryHandler(
			"/myapp.employee.v1.EmployeeService/Hire",
			hireHandler(bus),
		))
	mux.Handle("/myapp.employee.v1.EmployeeService/Promote",
		connect.NewUnaryHandler(
			"/myapp.employee.v1.EmployeeService/Promote",
			promoteHandler(bus),
		))
	return mux
}

// hireHandler shows the canonical wiring: one closure over the bus,
// one call to connectedge.Dispatch with a small decode callback.
// Returns the post-Decide aggregate state directly as the response.
//
// The decode callback is the DTO seam — in this minimal example the
// request message IS the internal command, but in a real app it would
// translate a stable public request shape into the internal Command.
func hireHandler(bus EmployeeBus) func(context.Context, *connect.Request[employeev1.Hire]) (*connect.Response[employeev1.Employee], error) {
	return func(ctx context.Context, req *connect.Request[employeev1.Hire]) (*connect.Response[employeev1.Employee], error) {
		tenant := req.Header().Get(TenantHeader)
		ctx = es.WithTenant(ctx, tenant)
		state, err := connectedge.Dispatch(ctx, bus, req,
			func(c *employeev1.Hire) (es.StreamID, employeev1.Command, error) {
				if tenant == "" {
					return es.StreamID{}, nil, errors.New("missing " + TenantHeader)
				}
				sid, err := es.ParseCanonical(tenant, "employee:"+c.EmployeeId)
				return sid, c, err
			},
		)
		if err != nil {
			return nil, err
		}
		return connect.NewResponse(state), nil
	}
}

func promoteHandler(bus EmployeeBus) func(context.Context, *connect.Request[employeev1.Promote]) (*connect.Response[employeev1.Employee], error) {
	return func(ctx context.Context, req *connect.Request[employeev1.Promote]) (*connect.Response[employeev1.Employee], error) {
		tenant := req.Header().Get(TenantHeader)
		ctx = es.WithTenant(ctx, tenant)
		// Promote arrives with the employee_id outside the proto
		// message (path param, header, etc. in a real API). For this
		// example we read it from a header to keep the wire shape
		// simple.
		empID := req.Header().Get("X-Employee-Id")
		state, err := connectedge.Dispatch(ctx, bus, req,
			func(c *employeev1.Promote) (es.StreamID, employeev1.Command, error) {
				if tenant == "" || empID == "" {
					return es.StreamID{}, nil, errors.New("missing X-Tenant or X-Employee-Id")
				}
				sid, err := es.ParseCanonical(tenant, "employee:"+empID)
				return sid, c, err
			},
		)
		if err != nil {
			return nil, err
		}
		return connect.NewResponse(state), nil
	}
}
