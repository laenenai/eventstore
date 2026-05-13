package es

import "context"

// tenantCtxKey is the unexported context key under which the tenant id
// is carried. Unexported to prevent consumers from bypassing the
// helpers below and reaching into context directly with a literal key.
type tenantCtxKey struct{}

// WithTenant returns a context carrying tenantID for downstream reads
// via TenantFrom or RequireTenant. Setting an empty tenant is a no-op;
// validation happens at read time.
func WithTenant(ctx context.Context, tenantID string) context.Context {
	if tenantID == "" {
		return ctx
	}
	return context.WithValue(ctx, tenantCtxKey{}, tenantID)
}

// TenantFrom returns the tenant id carried by ctx, or "" and false if
// none is set.
func TenantFrom(ctx context.Context) (string, bool) {
	v, _ := ctx.Value(tenantCtxKey{}).(string)
	return v, v != ""
}

// RequireTenant returns the tenant id from ctx, or ErrTenantMissing.
// Framework operations call this at their boundary — the multi-tenancy
// contract refuses operations without a tenant (ADR 0007).
func RequireTenant(ctx context.Context) (string, error) {
	t, ok := TenantFrom(ctx)
	if !ok {
		return "", ErrTenantMissing
	}
	return t, nil
}
