# connectedge — HTTP edge with Connect-go

Worked example for [cookbook recipe 15](../../docs/cookbook/15-http-edge-with-connect.md):
the **Employee** aggregate exposed over HTTP via Connect, wired through
the `adapters/httpedge/connect` runtime helper.

## What it shows

- One `cmdworkflow.Workflow` per aggregate, built once at startup.
- Two Connect unary handlers (`Hire`, `Promote`), each one closure over
  the bus + one call to `connectedge.Dispatch`.
- The `decode` callback is the DTO seam — where you translate a public
  request shape into the internal sealed `Command` and pull the
  `StreamID` out of headers / fields / claims.
- Framework error → Connect code mapping handled by `connectedge.MapError`
  (called inside `Dispatch`); domain errors fall through and the recipe
  documents how to extend the mapping in user code.

## What it does NOT show (intentionally)

- **Codegen.** The handlers are hand-rolled with `connect.NewUnaryHandler`.
  Recipe 15 explains the codegen variant — adding `protoc-gen-connect-go`
  to `buf.gen.yaml` and defining `service` blocks in your proto.
- **Auth / tenant from JWT.** Tenant is read from a `X-Tenant` header.
  Production: replace with middleware that derives it from auth claims.
- **Async-ack (202 + poll).** The bus is invoked synchronously and the
  response carries the post-Decide aggregate state. For long-running
  workflows you'd return a command id and expose a status endpoint;
  recipe 15 sketches the pattern.

## Run

```sh
go run .
```

Then in another shell:

```sh
curl -sS http://localhost:8080/myapp.employee.v1.EmployeeService/Hire \
  -H 'Content-Type: application/json' \
  -H 'X-Tenant: acme' \
  -d '{"employee_id":"emp-1","legal_name":"QWxpY2U=","email":"YUBlLmNvbQ==","date_of_birth":"MTk5MC0wMS0wMQ==","department":"eng","initial_role":"swe-2"}'
```

Connect's JSON codec encodes `bytes` proto fields as base64, hence the
encoded PII fields above. Response is the post-Decide `Employee` state.

## Run the test

```sh
go test ./...
```
