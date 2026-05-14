# 15: HTTP edge with Connect-go

Expose a `cmdworkflow.Workflow` over HTTP so clients can issue
commands. This recipe wires Connect-go (HTTP/1.1, HTTP/2, gRPC-Web
from one definition) to the bus through the
`adapters/httpedge/connect` runtime helper.

The framework deliberately ships **no codegen** for the HTTP edge.
Internal `Command` types evolve with the aggregate (ADR 0013); a
public HTTP contract has stricter rules. The helper keeps the
mechanical bits — dispatch, error mapping, idempotency-key passthrough
— and leaves the transport contract under your control.

The worked example lives at [`examples/connectedge`](../../examples/connectedge).

## The pattern

For every command RPC, you write:

```go
func (h *Handler) Hire(
    ctx context.Context,
    req *connect.Request[employeev1.Hire],
) (*connect.Response[employeev1.Employee], error) {
    state, err := connectedge.Dispatch(ctx, h.bus, req,
        func(c *employeev1.Hire) (es.StreamID, employeev1.Command, error) {
            tenant := req.Header().Get("X-Tenant")
            sid, err := es.ParseCanonical(tenant, "employee:"+c.EmployeeId)
            return sid, c, err
        },
    )
    if err != nil {
        return nil, err
    }
    return connect.NewResponse(state), nil
}
```

That's the whole shape. The `decode` callback is the DTO seam — the
one place where you translate a public request message into the
internal sealed `Command` sum type and extract the `StreamID`.

`connectedge.Dispatch` does three things:

1. Calls `decode(req.Msg)` — returns `connect.CodeInvalidArgument` if
   the callback returns an error.
2. Reads the `Idempotency-Key` header (IETF draft) and passes it
   through to `cmdworkflow.WithIdempotencyKey` so Restate / DBOS can
   dedupe on retry.
3. Calls `bus.HandleCmd` and runs framework errors through
   `connectedge.MapError` (table below).

The caller wraps the returned state into a `connect.Response[T]` of
their choosing — the aggregate state directly, a public DTO
projection, or a fixed ack.

## Error mapping

`MapError` covers framework sentinels. Apply your own mapping for
domain errors before falling through:

```go
func mapDomainError(err error) error {
    switch {
    case errors.Is(err, employee.ErrAlreadyHired):
        return connect.NewError(connect.CodeAlreadyExists, err)
    case errors.Is(err, employee.ErrNotHired):
        return connect.NewError(connect.CodeNotFound, err)
    }
    return connectedge.MapError(err)
}
```

Then wrap the Dispatch result:

```go
state, err := connectedge.Dispatch(...)
if err != nil { return nil, mapDomainError(err) }
```

The framework mapping built into `MapError`:

| Framework error             | Connect code            |
| --------------------------- | ----------------------- |
| `es.ErrConflict`            | `Aborted`               |
| `es.ErrConstraintViolated`  | `AlreadyExists`         |
| `es.ErrTerminal`            | `FailedPrecondition`    |
| `es.ErrInvalidStreamID`     | `InvalidArgument`       |
| `es.ErrTenantMissing`       | `Unauthenticated`       |
| `es.ErrStreamNotFound`      | `NotFound`              |
| `es.ErrEventNotFound`       | `NotFound`              |
| `es.ErrStateNotFound`       | `NotFound`              |
| `es.ErrKMSUnavailable`      | `Unavailable`           |
| `es.ErrCryptoIntegrity`     | `DataLoss`              |
| `es.ErrUnknownSchemaVersion`| `Internal`              |
| any `*connect.Error`        | returned as-is          |
| anything else               | `Unknown`               |

## DTO vs internal command — why the decode callback exists

The example happens to use the internal command type
(`*employeev1.Hire`) directly as the request message. In a real
service you almost always want a public DTO:

```proto
// proto/api/employee/v1/employee_service.proto — your PUBLIC contract
service EmployeeService {
  rpc Hire(HireRequest) returns (HireResponse);
}

message HireRequest {
  string employee_id = 1;
  string legal_name  = 2;  // plain string — clients don't see bytes/PII typing
  string email       = 3;
  string department  = 5;
  string initial_role = 6;
}

message HireResponse {
  string employee_id = 1;
  string status      = 2;
}
```

The internal `myapp.employee.v1.Hire` keeps its `bytes` PII fields
and its `Commands` sum-type membership. The decode callback bridges
the two:

```go
func decodeHire(req *connect.Request[HireRequest]) (es.StreamID, employeev1.Command, error) {
    sid, err := es.ParseCanonical(req.Header().Get("X-Tenant"),
                                  "employee:"+req.Msg.EmployeeId)
    if err != nil { return es.StreamID{}, nil, err }
    cmd := &employeev1.Hire{
        EmployeeId:  req.Msg.EmployeeId,
        LegalName:   []byte(req.Msg.LegalName),
        Email:       []byte(req.Msg.Email),
        DateOfBirth: []byte(req.Msg.DateOfBirth),
        Department:  req.Msg.Department,
        InitialRole: req.Msg.InitialRole,
    }
    return sid, cmd, nil
}
```

When you later evolve the internal command (new field, schema_version
bump per ADR 0013), the public `HireRequest` stays put. When you need
to evolve the public API, add `HireRequestV2` or a new RPC without
touching the aggregate.

## Codegen if you want it

The helper is the same; only the handler shape changes. Add
`protoc-gen-connect-go` to your `buf.gen.yaml` and define service
blocks in your **own** proto (not the framework's):

```yaml
# in your project's buf.gen.yaml
plugins:
  - remote: buf.build/connectrpc/go
    out: gen
    opt:
      - paths=source_relative
```

```proto
// proto/api/employee/v1/employee_service.proto
service EmployeeService {
  rpc Hire(HireRequest)       returns (HireResponse);
  rpc Promote(PromoteRequest) returns (PromoteResponse);
}
```

Codegen produces an `EmployeeServiceHandler` interface; your
implementation methods each delegate to `connectedge.Dispatch`. The
generated registration call (`employeev1connect.NewEmployeeServiceHandler(impl)`)
replaces the manual `http.ServeMux` wiring.

## Auth and tenant

The helper does not extract tenant or user from auth — that belongs
in middleware. The typical shape:

```go
mux := http.NewServeMux()
mux.Handle("/myapp.employee.v1.EmployeeService/Hire",
    authMiddleware(connect.NewUnaryHandler(...)))
```

Where `authMiddleware` validates the bearer token and attaches the
tenant to the request context (or a custom header). The decode
callback then reads it from there.

The reason for keeping this outside the helper: every shop has a
different auth story (JWT, mTLS, session cookies, internal SPIFFE).
A built-in extractor would be wrong half the time.

## Idempotency

Clients that retry should set the `Idempotency-Key` header
(IETF draft). The helper passes the value to
`cmdworkflow.WithIdempotencyKey`, which:

- Derives a deterministic `command_id` so every event from this
  invocation carries the same id across retries — subscribers that
  dedup on `(command_id, output_index)` (ADR 0015) observe stable
  ids.
- On **Restate** / **DBOS**: the workflow runtime uses the same key
  as the invocation id. A second call with the same key joins the
  in-progress execution or returns the cached result (true
  cross-call dedup).
- On **inproc**: command_id is stable, but inproc has no journal —
  two concurrent calls with the same key both run. Inproc is for
  tests, not production.

## Async-ack — when 200 isn't right

If a command kicks off a long-running workflow (multi-minute saga,
external API calls with retries), blocking the HTTP request until
`HandleCmd` returns is wrong. Two options:

1. **202 + poll.** Return immediately with a command id; expose a
   separate status endpoint. The bus exposes the AppendResult via the
   aggregate's `state_cache` — clients poll until `version >= N`.
2. **Server-Sent Events / Connect streaming.** Open a bidi stream;
   stream subscriber events as they fan out.

Neither is built into the helper; both are 20-line user-land additions.
Pick the one your clients can actually consume.

## When not to use this recipe

- **You're not exposing commands over HTTP.** If your edge is a
  message queue trigger, CLI, or cron, you call `bus.HandleCmd`
  directly — the helper adds nothing.
- **You're integrating with a non-Connect HTTP framework (chi, gin,
  echo).** The same `Dispatch` works — `connect.NewUnaryHandler`
  returns a `http.Handler`, mountable on any router. But you may
  prefer to skip the Connect codec entirely and use the underlying
  framework's JSON decoding; in that case, write a thin shim that
  decodes → calls `bus.HandleCmd` → encodes. The helper isn't load-
  bearing there.

## Failure modes

- **Decode panics.** The callback should never panic; if it does,
  Connect returns `Internal`. Validate inputs explicitly in the
  callback and return an error for `Dispatch` to convert.
- **Bus returns nil state.** `Dispatch` returns the zero value. If
  your response type can't represent zero (rare with proto messages),
  guard at the caller.
- **`Idempotency-Key` collision across tenants.** The derived
  `command_id` is namespaced by the key alone — two tenants sending
  the same key produce the same id. If your aggregates are scoped per
  tenant (default), this is harmless: command ids only need to be
  unique per stream. If you ever scan globally on `command_id`,
  prefix the key with tenant in middleware before reaching the helper.
