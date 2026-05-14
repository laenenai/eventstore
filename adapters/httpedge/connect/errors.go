package connectedge

import (
	"errors"

	"connectrpc.com/connect"

	"github.com/laenenai/eventstore/es"
)

// MapError converts a framework error to a Connect error with an
// appropriate code. Applies these rules in order; the first match wins:
//
//   - es.ErrConflict             → Aborted          (retry-after-reload)
//   - es.ErrConstraintViolated   → AlreadyExists    (uniqueness fail)
//   - es.ErrTerminal             → FailedPrecondition (closed stream)
//   - es.ErrInvalidStreamID      → InvalidArgument
//   - es.ErrTenantMissing        → Unauthenticated  (caller skipped auth)
//   - es.ErrStreamNotFound       → NotFound
//   - es.ErrEventNotFound        → NotFound
//   - es.ErrStateNotFound        → NotFound
//   - es.ErrKMSUnavailable       → Unavailable
//   - es.ErrCryptoIntegrity      → DataLoss         (cipher tampering)
//   - es.ErrUnknownSchemaVersion → Internal         (version skew)
//   - any *connect.Error         → returned as-is   (decode preserved)
//   - other                      → Unknown
//
// Callers that want a richer mapping (domain errors → their own
// Connect codes) can wrap MapError: check their own sentinels first,
// fall through to MapError for framework errors.
func MapError(err error) error {
	if err == nil {
		return nil
	}
	// Preserve already-mapped Connect errors (e.g. from inner handlers).
	var ce *connect.Error
	if errors.As(err, &ce) {
		return err
	}

	switch {
	case errors.Is(err, es.ErrConflict):
		return connect.NewError(connect.CodeAborted, err)
	case errors.Is(err, es.ErrConstraintViolated):
		return connect.NewError(connect.CodeAlreadyExists, err)
	case errors.Is(err, es.ErrTerminal):
		return connect.NewError(connect.CodeFailedPrecondition, err)
	case errors.Is(err, es.ErrInvalidStreamID):
		return connect.NewError(connect.CodeInvalidArgument, err)
	case errors.Is(err, es.ErrTenantMissing):
		return connect.NewError(connect.CodeUnauthenticated, err)
	case errors.Is(err, es.ErrStreamNotFound),
		errors.Is(err, es.ErrEventNotFound),
		errors.Is(err, es.ErrStateNotFound):
		return connect.NewError(connect.CodeNotFound, err)
	case errors.Is(err, es.ErrKMSUnavailable):
		return connect.NewError(connect.CodeUnavailable, err)
	case errors.Is(err, es.ErrCryptoIntegrity):
		return connect.NewError(connect.CodeDataLoss, err)
	case errors.Is(err, es.ErrUnknownSchemaVersion):
		return connect.NewError(connect.CodeInternal, err)
	default:
		return connect.NewError(connect.CodeUnknown, err)
	}
}
