package cedar_test

import (
	"context"
	"errors"
	"testing"

	"github.com/laenenai/eventstore/adapters/authz/cedar"
	"github.com/laenenai/eventstore/authz"
	"github.com/laenenai/eventstore/es"
	"github.com/laenenai/eventstore/estest"
)

const samplePolicies = `
permit (
    principal in Group::"approvers",
    action == Action::"myapp.invoice.v1.Approve",
    resource is Stream
);

permit (
    principal == User::"alice",
    action == Action::"myapp.invoice.v1.Create",
    resource is Stream
);
`

const sampleEntities = `[
    { "uid": {"type":"User","id":"alice"},
      "attrs": {},
      "parents": [{"type":"Group","id":"approvers"}] },
    { "uid": {"type":"User","id":"bob"},
      "attrs": {},
      "parents": [] }
]`

func mustStream(t *testing.T) es.StreamID {
	return estest.MustStream(t, "t-cedar", "invoice", "1")
}

func TestPolicy_AllowsWhenPolicyMatches(t *testing.T) {
	p, err := cedar.New(cedar.Config{
		Policies: samplePolicies,
		Entities: cedar.EntitiesJSON(sampleEntities),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	err = p.Authorize(context.Background(), authz.Request{
		Principal: authz.Principal{ID: "alice", Type: "User"},
		Action:    "myapp.invoice.v1.Approve", // alice is in approvers group
		Stream:    mustStream(t),
	})
	if err != nil {
		t.Errorf("expected allow, got %v", err)
	}
}

func TestPolicy_DeniesWhenNoPolicyMatches(t *testing.T) {
	p, _ := cedar.New(cedar.Config{
		Policies: samplePolicies,
		Entities: cedar.EntitiesJSON(sampleEntities),
	})
	err := p.Authorize(context.Background(), authz.Request{
		Principal: authz.Principal{ID: "bob", Type: "User"},
		Action:    "myapp.invoice.v1.Approve", // bob not in approvers
		Stream:    mustStream(t),
	})
	if !errors.Is(err, authz.ErrUnauthorized) {
		t.Errorf("expected ErrUnauthorized, got %v", err)
	}
}

func TestPolicy_DeniesWhenPrincipalMissing(t *testing.T) {
	p, _ := cedar.New(cedar.Config{
		Policies: samplePolicies,
		Entities: cedar.EntitiesJSON(sampleEntities),
	})
	err := p.Authorize(context.Background(), authz.Request{
		// Principal: zero — no ID.
		Action: "myapp.invoice.v1.Approve",
		Stream: mustStream(t),
	})
	if !errors.Is(err, authz.ErrUnauthorized) {
		t.Errorf("expected ErrUnauthorized on missing principal, got %v", err)
	}
}

func TestPolicy_AllowMissingPrincipalFallthrough(t *testing.T) {
	p, _ := cedar.New(cedar.Config{
		Policies:              `permit (principal, action, resource);`,
		AllowMissingPrincipal: true,
	})
	err := p.Authorize(context.Background(), authz.Request{
		Action: "anything",
		Stream: mustStream(t),
	})
	if err != nil {
		t.Errorf("AllowMissingPrincipal: got %v want allow", err)
	}
}

func TestPolicy_RequiresPolicies(t *testing.T) {
	_, err := cedar.New(cedar.Config{})
	if err == nil {
		t.Errorf("expected validation error on empty Policies")
	}
}

func TestPolicy_DistinguishesActionByExactString(t *testing.T) {
	// Same principal, different action — only Create is permitted.
	p, _ := cedar.New(cedar.Config{
		Policies: samplePolicies,
		Entities: cedar.EntitiesJSON(sampleEntities),
	})
	// Allow path
	if err := p.Authorize(context.Background(), authz.Request{
		Principal: authz.Principal{ID: "alice", Type: "User"},
		Action:    "myapp.invoice.v1.Create",
		Stream:    mustStream(t),
	}); err != nil {
		t.Errorf("Create: got %v want allow", err)
	}
	// Deny path
	if err := p.Authorize(context.Background(), authz.Request{
		Principal: authz.Principal{ID: "alice", Type: "User"},
		Action:    "myapp.invoice.v1.Cancel", // no policy
		Stream:    mustStream(t),
	}); !errors.Is(err, authz.ErrUnauthorized) {
		t.Errorf("Cancel: got %v want ErrUnauthorized", err)
	}
}
