package es

import "errors"

// Typed errors. All framework operations return one of these wrapped via
// fmt.Errorf("...: %w", err) or returned bare. Consumers use errors.Is
// for matching.
var (
	// ErrConflict reports an optimistic-concurrency failure on Append —
	// the expected stream version did not match the current state.
	// Caller should reload the stream and retry.
	ErrConflict = errors.New("eventstore: append conflict (expected version mismatch)")

	// ErrConstraintViolated reports that a ClaimConstraint operation
	// failed because the (tenant, scope, value) was already claimed.
	// Caller decides whether this is a domain error to surface or a
	// retry case.
	ErrConstraintViolated = errors.New("eventstore: unique constraint violated")

	// ErrStreamNotFound is returned when a read operation targets a
	// stream that has never been written to.
	ErrStreamNotFound = errors.New("eventstore: stream not found")

	// ErrEventNotFound is returned by GetEventByID when the event id
	// does not exist for the tenant.
	ErrEventNotFound = errors.New("eventstore: event not found")

	// ErrTenantMissing reports that the operation requires a tenant in
	// context (via WithTenant) but none was set. The framework refuses
	// to operate without a tenant — see ADR 0007.
	ErrTenantMissing = errors.New("eventstore: tenant missing from context")

	// ErrInvalidStreamID reports that a StreamID failed validation.
	ErrInvalidStreamID = errors.New("eventstore: invalid stream id")

	// ErrUnknownSchemaVersion is returned by upcaster chains when an
	// event carries a schema_version newer than any registered upcaster
	// targets. Indicates a version-skew deployment — see ADR 0013.
	ErrUnknownSchemaVersion = errors.New("eventstore: unknown schema version")

	// ErrCryptoIntegrity reports an AEAD tag mismatch on a crypto-
	// shredded field. Indicates corruption or tampering — see ADR 0010.
	ErrCryptoIntegrity = errors.New("eventstore: ciphertext integrity check failed")

	// ErrKMSUnavailable reports that the KMS adapter could not resolve
	// a KEK and no cached DEK is available.
	ErrKMSUnavailable = errors.New("eventstore: KMS unavailable")

	// ErrTerminal reports that a command was issued against a stream
	// whose Decider.IsTerminal returned true. The stream is closed —
	// no further events may be appended. Callers can match via
	// errors.Is(err, es.ErrTerminal).
	ErrTerminal = errors.New("eventstore: stream is terminal")

	// ErrStateNotFound reports that GetState found no cached state
	// for the given (tenant_id, stream_id). The stream may exist in
	// the events table but the aggregate is not opted into the
	// state cache (or the cache hasn't been backfilled for older
	// streams yet — see aggregate.RebuildStateCache).
	ErrStateNotFound = errors.New("eventstore: state not found")
)
