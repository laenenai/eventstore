// Package authz hosts the framework's authorization contract.
// Aggregates know about state transitions and domain invariants; they
// do not know about users, roles, or policies. The Policy interface is
// the small, engine-agnostic seam that lets you layer authz onto
// aggregate.Runtime without baking a policy engine into the framework.
//
// Engine implementations ship as separate packages:
//
//   - adapters/authz/cedar — Cedar policy language (cedar-policy/cedar-go)
//
// See cookbook recipe 05 for the wrapper pattern around
// aggregate.Runtime.Handle.
package authz

import (
	"context"
	"errors"

	"github.com/laenenai/eventstore/es"
)

// ErrUnauthorized is returned by Policy.Authorize when the request is
// denied. Callers can match via errors.Is(err, authz.ErrUnauthorized).
var ErrUnauthorized = errors.New("authz: unauthorized")

// Policy decides whether one Request is allowed. Implementations
// route the request to Cedar / OPA / RBAC / hand-rolled logic.
type Policy interface {
	Authorize(ctx context.Context, req Request) error
}

// Request is the input to Policy.Authorize. Principal, Action, and
// Stream are required; Resource and Context attributes are optional.
type Request struct {
	Principal Principal
	Action    string // e.g. "myapp.invoice.v1.Approve"
	Stream    es.StreamID
	Resource  any // command payload, current state, or whatever the policy needs
	Context   map[string]any
}

// Principal identifies the actor on whose behalf the request is made.
type Principal struct {
	ID         string
	Type       string // e.g. "User", "Service"
	Attributes map[string]any
}

type principalKey struct{}

// WithPrincipal returns a new context carrying the authenticated
// Principal. Typically set by the application's auth middleware
// before Handle is called.
func WithPrincipal(ctx context.Context, p Principal) context.Context {
	return context.WithValue(ctx, principalKey{}, p)
}

// PrincipalFrom extracts the Principal previously set with
// WithPrincipal. The second return is false when the context carries
// no principal.
func PrincipalFrom(ctx context.Context) (Principal, bool) {
	p, ok := ctx.Value(principalKey{}).(Principal)
	return p, ok
}
