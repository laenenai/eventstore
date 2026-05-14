package main

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"connectrpc.com/connect"
	_ "modernc.org/sqlite"

	employeev1 "github.com/laenenai/eventstore/gen/myapp/employee/v1"
)

// End-to-end: HTTP server → Connect codec → helper → bus → SQLite.

func newTestServer(t *testing.T) (string, func()) {
	t.Helper()
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	bus, err := buildBus(context.Background(), db)
	if err != nil {
		_ = db.Close()
		t.Fatalf("buildBus: %v", err)
	}
	srv := httptest.NewServer(newMux(bus))
	return srv.URL, func() {
		srv.Close()
		_ = db.Close()
	}
}

func TestEdge_HireRoundTrip(t *testing.T) {
	baseURL, cleanup := newTestServer(t)
	defer cleanup()

	client := connect.NewClient[employeev1.Hire, employeev1.Employee](
		httpTestClient(),
		baseURL+"/myapp.employee.v1.EmployeeService/Hire",
	)

	req := connect.NewRequest(&employeev1.Hire{
		EmployeeId:  "emp-1",
		LegalName:   []byte("Alice"),
		Email:       []byte("alice@example.com"),
		DateOfBirth: []byte("1990-01-01"),
		Department:  "eng",
		InitialRole: "swe-2",
	})
	req.Header().Set("X-Tenant", "acme")

	resp, err := client.CallUnary(context.Background(), req)
	if err != nil {
		t.Fatalf("Hire: %v", err)
	}
	if resp.Msg.EmployeeId != "emp-1" {
		t.Errorf("employee_id: got %q want emp-1", resp.Msg.EmployeeId)
	}
	if resp.Msg.Department != "eng" {
		t.Errorf("department: got %q want eng", resp.Msg.Department)
	}
}

func TestEdge_MissingTenantIsInvalidArgument(t *testing.T) {
	baseURL, cleanup := newTestServer(t)
	defer cleanup()

	client := connect.NewClient[employeev1.Hire, employeev1.Employee](
		httpTestClient(),
		baseURL+"/myapp.employee.v1.EmployeeService/Hire",
	)
	// No X-Tenant header — decode callback should reject.
	req := connect.NewRequest(&employeev1.Hire{EmployeeId: "emp-2"})

	_, err := client.CallUnary(context.Background(), req)
	var ce *connect.Error
	if !errors.As(err, &ce) {
		t.Fatalf("want *connect.Error, got %T: %v", err, err)
	}
	if ce.Code() != connect.CodeInvalidArgument {
		t.Errorf("code: got %v want InvalidArgument", ce.Code())
	}
}

func TestEdge_DomainErrorMapsToFailedPrecondition(t *testing.T) {
	baseURL, cleanup := newTestServer(t)
	defer cleanup()

	hire := connect.NewClient[employeev1.Hire, employeev1.Employee](
		httpTestClient(),
		baseURL+"/myapp.employee.v1.EmployeeService/Hire",
	)
	first := connect.NewRequest(&employeev1.Hire{
		EmployeeId: "emp-3", LegalName: []byte("Bob"),
		Email: []byte("b@e.com"), DateOfBirth: []byte("1990-01-01"),
		Department: "eng", InitialRole: "swe-1",
	})
	first.Header().Set("X-Tenant", "acme")
	if _, err := hire.CallUnary(context.Background(), first); err != nil {
		t.Fatalf("first Hire: %v", err)
	}

	// Second Hire on the same stream — decider returns
	// employee.ErrAlreadyHired (not a framework sentinel), so MapError
	// falls through to CodeUnknown. That's the v1 trade-off: domain
	// errors need explicit handling in the decode wrapper if the user
	// wants richer codes. The recipe documents the pattern.
	second := connect.NewRequest(&employeev1.Hire{
		EmployeeId: "emp-3", LegalName: []byte("Bob"),
		Email: []byte("b@e.com"), DateOfBirth: []byte("1990-01-01"),
		Department: "eng", InitialRole: "swe-1",
	})
	second.Header().Set("X-Tenant", "acme")
	_, err := hire.CallUnary(context.Background(), second)
	var ce *connect.Error
	if !errors.As(err, &ce) {
		t.Fatalf("want *connect.Error, got %v", err)
	}
	if ce.Code() != connect.CodeUnknown {
		t.Errorf("domain error code: got %v want Unknown (see recipe for richer mapping)", ce.Code())
	}
}

// httpTestClient is the default HTTP/1.1 client. httptest.Server is
// HTTP/1.1 by default and Connect's binary codec works over that.
func httpTestClient() connect.HTTPClient { return http.DefaultClient }
