package cedar

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	cedarsdk "github.com/cedar-policy/cedar-go"

	"github.com/laenenai/eventstore/authz"
)

// Config controls the Cedar Policy adapter.
type Config struct {
	// Policies is the Cedar policy set as a single text document.
	// See https://www.cedarpolicy.com for syntax.
	Policies string

	// Entities is the entity store (users, groups, resources)
	// referenced by the policies. Use EntitiesJSON for the most
	// common case.
	Entities cedarsdk.EntityMap

	// AllowMissingPrincipal lets requests without an authz.Principal
	// fall through to Cedar with an empty principal. Default false:
	// missing principal → ErrUnauthorized. Useful for tests or
	// public-by-default actions.
	AllowMissingPrincipal bool
}

// EntitiesJSON parses a Cedar entity document from JSON. Convenience
// for Config.Entities; bad JSON panics — feed it a constant or fail
// fast at startup.
func EntitiesJSON(s string) cedarsdk.EntityMap {
	var em cedarsdk.EntityMap
	if err := json.Unmarshal([]byte(s), &em); err != nil {
		panic(fmt.Errorf("cedar: bad entities JSON: %w", err))
	}
	return em
}

// Policy implements authz.Policy by evaluating Cedar policies.
type Policy struct {
	cfg      Config
	policies *cedarsdk.PolicySet
}

// New parses the policies and returns a ready-to-use Policy.
func New(cfg Config) (*Policy, error) {
	if cfg.Policies == "" {
		return nil, errors.New("cedar: Config.Policies is required")
	}
	ps, err := cedarsdk.NewPolicySetFromBytes("policies.cedar", []byte(cfg.Policies))
	if err != nil {
		return nil, fmt.Errorf("cedar: parse policies: %w", err)
	}
	return &Policy{cfg: cfg, policies: ps}, nil
}

// Authorize implements authz.Policy. Returns nil when Cedar permits,
// authz.ErrUnauthorized (wrapped with Cedar's diagnostic reason) when
// denied.
func (p *Policy) Authorize(_ context.Context, req authz.Request) error {
	if req.Principal.ID == "" && !p.cfg.AllowMissingPrincipal {
		return fmt.Errorf("%w: missing principal", authz.ErrUnauthorized)
	}

	cedarReq := cedarsdk.Request{
		Principal: cedarsdk.NewEntityUID(
			cedarsdk.EntityType(defaultType(req.Principal.Type, "Principal")),
			cedarsdk.String(req.Principal.ID),
		),
		Action:   cedarsdk.NewEntityUID("Action", cedarsdk.String(req.Action)),
		Resource: cedarsdk.NewEntityUID("Stream", cedarsdk.String(req.Stream.Canonical())),
	}
	if len(req.Context) > 0 {
		cedarReq.Context = recordFromMap(req.Context)
	}

	ok, diag := cedarsdk.Authorize(p.policies, p.cfg.Entities, cedarReq)
	if ok {
		return nil
	}
	if len(diag.Errors) > 0 {
		return fmt.Errorf("%w: policy errors: %v", authz.ErrUnauthorized, diag.Errors)
	}
	return fmt.Errorf("%w: no policy permits %s on %s for %s::\"%s\"",
		authz.ErrUnauthorized, req.Action, req.Stream.Canonical(),
		req.Principal.Type, req.Principal.ID)
}

func defaultType(t, fallback string) string {
	if t == "" {
		return fallback
	}
	return t
}

func recordFromMap(m map[string]any) cedarsdk.Record {
	out := cedarsdk.RecordMap{}
	for k, v := range m {
		if cv := toCedarValue(v); cv != nil {
			out[cedarsdk.String(k)] = cv
		}
	}
	return cedarsdk.NewRecord(out)
}

func toCedarValue(v any) cedarsdk.Value {
	switch x := v.(type) {
	case string:
		return cedarsdk.String(x)
	case bool:
		if x {
			return cedarsdk.True
		}
		return cedarsdk.False
	case int:
		return cedarsdk.Long(int64(x))
	case int32:
		return cedarsdk.Long(int64(x))
	case int64:
		return cedarsdk.Long(x)
	}
	return nil
}

// Compile-time check.
var _ authz.Policy = (*Policy)(nil)
