# 05: Layered Authorization

The framework intentionally stays out of authz. Aggregates know about
state transitions and domain invariants; they do not know about users,
roles, or policies. Authorization layers on top via a thin wrapper
around `aggregate.Runtime`.

## When to use this

- You need to enforce "who can do what" at the command boundary.
- Your domain has structural rules ("no self-approval") and identity
  rules ("only the compliance team can approve") and you want them
  cleanly separated.
- You want pluggable policy engines (Cedar / OPA / hand-rolled RBAC)
  without baking one into the framework.

## The pattern

A thin wrapper that intercepts `Handle`, runs the policy check, then
delegates. The wrapper holds a `Policy` interface; you implement that
interface with whatever engine you prefer.

```go
package authz

import (
    "context"

    "github.com/laenenai/eventstore/aggregate"
    "github.com/laenenai/eventstore/es"
)

// Policy is the authorization contract. Implement against Cedar,
// OPA, RBAC, your own engine.
type Policy interface {
    Authorize(ctx context.Context, req Request) error
}

type Request struct {
    Principal Principal
    Action    string          // e.g., "myapp.party.v1.Approve"
    Stream    es.StreamID
    Resource  any             // optional: command payload, current state
}

type Principal struct {
    ID         string
    Type       string
    Attributes map[string]any
}

type principalKey struct{}

func WithPrincipal(ctx context.Context, p Principal) context.Context {
    return context.WithValue(ctx, principalKey{}, p)
}

func PrincipalFrom(ctx context.Context) (Principal, bool) {
    p, ok := ctx.Value(principalKey{}).(Principal)
    return p, ok
}

// AuthzRuntime wraps an aggregate.Runtime with a Policy check.
type AuthzRuntime[S, C, E any] struct {
    Inner  *aggregate.Runtime[S, C, E]
    Policy Policy
}

// actionOf extracts the stable action name from a command. The
// codegen plugin emits an Action() string method on every variant
// returning the variant's full proto type name.
func actionOf(cmd any) string {
    if a, ok := cmd.(interface{ Action() string }); ok {
        return a.Action()
    }
    return ""
}

func (a *AuthzRuntime[S, C, E]) Handle(
    ctx context.Context,
    sid es.StreamID,
    cmd C,
    opts ...aggregate.HandleOption,
) (es.AppendResult, error) {
    principal, _ := PrincipalFrom(ctx)
    if err := a.Policy.Authorize(ctx, Request{
        Principal: principal,
        Action:    actionOf(cmd),
        Stream:    sid,
        Resource:  cmd,
    }); err != nil {
        return es.AppendResult{}, err
    }
    return a.Inner.Handle(ctx, sid, cmd, opts...)
}
```

That's the whole pattern — ~50 lines a consumer writes once and reuses.

## Why this isn't in the framework

See ADR 0015 for the same reasoning applied to sagas. Briefly: authz
is heterogeneous (Cedar / OPA / Zanzibar / custom RBAC are all
legitimate choices), the framework's value-add over a 50-line wrapper
is small, and baking it in would force a `Principal` shape on every
consumer regardless of their identity model. Better to ship the
ergonomic helper (`Action()` codegen) and let the wrapping pattern
be application code.

## Engine implementations

### Cedar

[Cedar](https://www.cedarpolicy.com/) is AWS's policy language with a
Go SDK (`github.com/cedar-policy/cedar-go`).

```go
package authzcedar

import (
    "context"
    "fmt"

    cedar "github.com/cedar-policy/cedar-go"

    "github.com/laenenai/eventstore/es"
    "github.com/<you>/myapp/authz"
)

type Policy struct {
    set *cedar.PolicySet
}

func New(set *cedar.PolicySet) *Policy { return &Policy{set: set} }

func (p *Policy) Authorize(ctx context.Context, req authz.Request) error {
    decision := p.set.IsAuthorized(cedar.Request{
        Principal: principalToEntity(req.Principal),
        Action:    cedar.NewEntityUID("Action", req.Action),
        Resource:  streamToEntity(req.Stream),
        Context:   resourceContext(req.Resource),
    })
    if !decision.Allowed {
        return fmt.Errorf("forbidden: %s", decision.Reasons)
    }
    return nil
}
```

Example policy for the Party aggregate:

```cedar
// Compliance can approve any change, except their own.
permit (
  principal in Group::"compliance",
  action == Action::"myapp.party.v1.Approve",
  resource is Party
) when {
  context.proposed_by != principal.id
};

// Party owner or KYC operators can update phone/address.
permit (
  principal,
  action in [
    Action::"myapp.party.v1.UpdatePhone",
    Action::"myapp.party.v1.UpdateAddress"
  ],
  resource is Party
) when {
  principal.id == resource.created_by ||
  principal in Group::"kyc_operators"
};

// Only KYC makers can propose identity changes.
permit (
  principal in Group::"kyc_makers",
  action in [
    Action::"myapp.party.v1.ProposeName",
    Action::"myapp.party.v1.ProposeEmail",
    Action::"myapp.party.v1.ProposeDateOfBirth"
  ],
  resource is Party
);
```

### OPA

[OPA](https://www.openpolicyagent.org/) with Rego policies and the
`github.com/open-policy-agent/opa` SDK works the same way — different
evaluation engine, same wrapping pattern. The `Policy.Authorize`
method evaluates the Rego policy with the same `Request` payload.

### Simple RBAC

For applications that don't need a policy engine, a map suffices:

```go
package authzrbac

import (
    "context"
    "errors"

    "github.com/<you>/myapp/authz"
)

// Permissions maps role -> set of allowed action names.
type Policy struct {
    Roles map[string]map[string]bool // role -> action -> allowed
}

func (p *Policy) Authorize(ctx context.Context, req authz.Request) error {
    role, _ := req.Principal.Attributes["role"].(string)
    if p.Roles[role][req.Action] {
        return nil
    }
    return errors.New("forbidden")
}

// Usage:
policy := &authzrbac.Policy{
    Roles: map[string]map[string]bool{
        "kyc_maker": {
            "myapp.party.v1.ProposeName":  true,
            "myapp.party.v1.ProposeEmail": true,
        },
        "kyc_checker": {
            "myapp.party.v1.Approve": true,
            "myapp.party.v1.Reject":  true,
        },
    },
}
```

100 lines tops, no external dependency, fine for small apps.

## Wiring it up

```go
// main.go

policy := authzcedar.New(loadPolicySet())

baseRuntime := &aggregate.Runtime[party.State, partyv1.Command, partyv1.Event]{
    Store:   store,
    Decider: party.Decider,
    Codec:   party.EventCodec,
}

rt := &authz.AuthzRuntime[party.State, partyv1.Command, partyv1.Event]{
    Inner:  baseRuntime,
    Policy: policy,
}

// In the HTTP/gRPC handler:
func handleApprove(w http.ResponseWriter, r *http.Request) {
    principal := extractPrincipal(r)  // verify JWT, build Principal
    ctx := authz.WithPrincipal(r.Context(), principal)
    ctx = es.WithTenant(ctx, principal.Attributes["tenant"].(string))

    _, err := rt.Handle(ctx, sid, &partyv1.Approve{
        ChangeId: r.FormValue("change_id"),
        ApprovedBy: principal.ID,
    })
    // ...
}
```

## Structural rules vs identity rules

A clean separation:

| Lives in the decider                                | Lives in the policy                           |
| --------------------------------------------------- | --------------------------------------------- |
| Self-approval forbidden (`approved_by != proposed_by`) | Only compliance role can approve            |
| At most one pending change of the same kind        | Only the party owner can update their phone   |
| Cannot mutate suspended/closed parties              | Only admins can suspend                       |
| Email uniqueness                                    | Only KYC makers can propose email changes    |

The decider's rules are domain invariants — they hold for any caller,
including the admin/system. The policy's rules are identity-scoped —
who can attempt the action in the first place.

Crucially: structural rules in the decider catch authz bypasses. If
someone forgets to wrap a runtime with the authz wrapper, the
decider still enforces self-approval-forbidden and at-most-one-pending.
The authz layer is *additional* protection, not the only protection.

## System actors

Sagas, projectors, drain workers, scheduled jobs all call
`Runtime.Handle` and don't have a user-style principal. Three options:

1. **Skip the authz wrapper.** Internal runtimes don't wrap; HTTP/gRPC
   runtimes do. The same `aggregate.Runtime` instance can be
   referenced both ways.
2. **System principal.** A reserved `Principal{Type: "system", ID:
   "<service-name>"}` that policies explicitly allow.
3. **Bypass attribute on the principal.** A boolean flag in
   `Principal.Attributes["bypass_authz"] = true` that the policy
   checks for. Auditable, scriptable.

Option 1 is the cleanest. Internal workers operate on the unwrapped
runtime; the wrapper exists only at the API boundary where user
identity matters.

## Audit and observability

Authz decisions are themselves events worth recording. Options:

- **Log the decision** at the wrapper level (allow/deny + reason +
  principal). Cheap; ties to existing log infrastructure.
- **Emit a domain event for audit-significant denials.** E.g., a
  `AuthzFailureRecorded` event in a separate audit aggregate. More
  invasive but searchable in the same event store as the domain.
- **Push to an external audit system.** Splunk, Datadog, etc.

The framework provides the `Actor` field on every successful event;
the authz layer adds the principal context for the denial side.

## Notes and pitfalls

- **Re-check inside long-running handlers.** A multi-step saga that
  takes minutes may need to re-check authorization at each step (the
  principal's role may have changed). Don't cache authz decisions.
- **Tenant scoping is upstream of authz.** Always set
  `es.WithTenant(ctx, ...)` BEFORE the authz check; policies often
  depend on tenant.
- **Action names are stable** because they come from the proto type
  URL. Renaming a command type IS a breaking change for the policy.
- **Test policies in isolation** before integration. Cedar's
  `cedar test` tool, OPA's `opa test`, and equivalent for other
  engines, are essential.

## Reference

- The [Action()-method codegen](../../cmd/protoc-gen-es-go/main.go) is
  what makes `actionOf(cmd)` work without reflection.
- [`examples/party/README.md`](../../examples/party/README.md) has a
  Cedar policy sketch for the Party aggregate.
- [ADR 0015](../adr/0015-decider-output-and-saga-scope.md) — the same
  "frameworks shouldn't try to be everything" argument that keeps
  sagas out also keeps authz out.
