package es

import (
	"regexp"
	"strings"
)

// canonicalSep separates the type and id components in the canonical
// stream_id storage form (Type + ":" + ID). The separator is reserved
// — the slug regex below forbids it inside Type or ID, so splitting is
// unambiguous.
const canonicalSep = ":"

// slugRe matches valid Type and ID components: lowercase alphanumeric
// with optional underscores and hyphens, starting with alphanumeric,
// up to 128 characters total. See ADR 0008.
var slugRe = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,127}$`)

// StreamID identifies one stream. Every storage operation in the
// framework takes a StreamID; multi-tenancy is enforced by the
// non-empty Tenant invariant.
//
// Codegen produces typed wrappers per aggregate (e.g., user.StreamID)
// that wrap this type with compile-time aggregate-type binding —
// see ADR 0008. v1 framework code constructs es.StreamID directly.
type StreamID struct {
	Tenant string // required, non-empty
	Type   string // slug-validated
	ID     string // slug-validated
}

// NewStreamID constructs a validated StreamID. Returns
// ErrTenantMissing or ErrInvalidStreamID on failure.
func NewStreamID(tenant, typ, id string) (StreamID, error) {
	sid := StreamID{Tenant: tenant, Type: typ, ID: id}
	if err := sid.Validate(); err != nil {
		return StreamID{}, err
	}
	return sid, nil
}

// Validate reports whether the StreamID is well-formed.
func (s StreamID) Validate() error {
	if s.Tenant == "" {
		return ErrTenantMissing
	}
	if !slugRe.MatchString(s.Type) || !slugRe.MatchString(s.ID) {
		return ErrInvalidStreamID
	}
	return nil
}

// Canonical returns the storage form: Type + ":" + ID. The Tenant
// component is stored in a separate column.
func (s StreamID) Canonical() string {
	return s.Type + canonicalSep + s.ID
}

// String renders the fully-qualified identity for log/debug output.
// Format: "<tenant>/<type>:<id>". Not the same as Canonical().
func (s StreamID) String() string {
	var b strings.Builder
	b.WriteString(s.Tenant)
	b.WriteByte('/')
	b.WriteString(s.Canonical())
	return b.String()
}

// ParseCanonical splits the storage-form stream_id into (Type, ID).
// Used by storage adapters when reconstructing a StreamID from rows.
func ParseCanonical(tenant, canonical string) (StreamID, error) {
	i := strings.IndexByte(canonical, canonicalSep[0])
	if i <= 0 || i == len(canonical)-1 {
		return StreamID{}, ErrInvalidStreamID
	}
	return NewStreamID(tenant, canonical[:i], canonical[i+1:])
}
