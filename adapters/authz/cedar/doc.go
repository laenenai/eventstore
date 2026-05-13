// Package cedar is the Cedar authorization adapter — a Policy
// implementation of authz.Policy backed by cedar-policy/cedar-go.
//
// Usage:
//
//	policies := `
//	    permit (
//	        principal in Group::"approvers",
//	        action == Action::"myapp.invoice.v1.Approve",
//	        resource is Stream
//	    );
//	`
//	p, err := cedar.New(cedar.Config{
//	    Policies: policies,
//	    Entities: cedar.EntitiesJSON(`[
//	        { "uid": {"type":"User", "id":"alice"},
//	          "attrs": {},
//	          "parents": [{"type":"Group", "id":"approvers"}] }
//	    ]`),
//	})
//
//	// Now p satisfies authz.Policy. Use it from your wrapper around
//	// aggregate.Runtime — see cookbook recipe 05.
//	if err := p.Authorize(ctx, authz.Request{
//	    Principal: principal,
//	    Action:    cmd.(interface{ Action() string }).Action(),
//	    Stream:    sid,
//	}); err != nil { ... }
//
// Cedar maps cleanly onto framework conventions:
//
//   - Principal.ID + Principal.Type → cedar EntityUID
//   - Request.Action (string) → Action::"<value>"
//   - Request.Stream → Stream::"<canonical>"
//   - Request.Context attributes → cedar Record fields
//
// Per ADR 0010 + recipe 05: authorization is not a framework feature;
// this adapter is purely a Policy implementation. The wrapper around
// aggregate.Runtime stays application-defined.
package cedar
