package es

// ActorKind enumerates the kinds of agents that can initiate an action.
// See ADR 0005.
type ActorKind int

const (
	ActorUnspecified ActorKind = 0
	ActorUser        ActorKind = 1
	ActorSystem      ActorKind = 2
	ActorService     ActorKind = 3
	ActorIntegration ActorKind = 4
)

// String returns a stable wire-friendly name for the kind. Used in
// logs and audit output.
func (k ActorKind) String() string {
	switch k {
	case ActorUser:
		return "user"
	case ActorSystem:
		return "system"
	case ActorService:
		return "service"
	case ActorIntegration:
		return "integration"
	default:
		return "unspecified"
	}
}

// Actor describes who or what initiated an action. Stored on the
// envelope as proto bytes (ADR 0005); the Principal is also
// denormalized to an indexed column for audit queries.
type Actor struct {
	Kind       ActorKind
	Principal  string            // primary identifier; indexed
	OnBehalfOf string            // "service acting on behalf of"; empty when N/A
	APIKeyID   string            // key id (not the secret), if applicable
	Attributes map[string]string // free-form attribution (device, region, ...)
}
