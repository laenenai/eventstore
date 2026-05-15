# esctl

Eventstore debugging CLI. Read-only inspection of streams, events,
state_cache, projections, and outbox state — against either the
Postgres or the SQLite adapter, auto-detected from the `--db` URL.

`esctl` uses the framework as a library (no admin RPC service
required) so it works the moment your DB is reachable.

## Install

```sh
go install github.com/laenenai/eventstore/cmd/esctl@latest
```

Or in a local checkout: `cd cmd/esctl && go build -o esctl .`

## Configure

Every flag also reads from an env var, and env vars can be sourced
from a `.env` (or `.env.local`) file in the current directory.
Precedence: explicit `--flag` → process env → `.env.local` → `.env`
→ built-in defaults. Copy `.env.example` to get started:

```sh
cp .env.example .env
$EDITOR .env
```

| Flag | Env var | Notes |
|------|---------|-------|
| `--db` | `ESCTL_DB` | **Required.** `postgres://...` or `file:./events.db` |
| `--tenant`, `-t` | `ESCTL_TENANT` | Default tenant; per-command `--tenant` overrides |
| `--output`, `-o` | `ESCTL_OUTPUT` | `pretty` (default) or `json` |
| `--no-color` | `ESCTL_NO_COLOR`, `NO_COLOR` | Disable ANSI colour |

## Commands

```sh
# Streams
esctl stream list   --type myapp.employee.v1.Employee [--limit N] [--after CURSOR]
esctl stream read   --stream "employee:emp-42" [--from-version N] [--watch] [--refresh 2s]
esctl stream verify --stream "employee:emp-42"               # checks ADR 0028 chain

# Single events
esctl event get --event-id <uuid>

# state_cache
esctl state get  --stream "employee:emp-42"
esctl state list --type myapp.employee.v1.Employee [--all] [--limit N] [--watch]

# Store-wide event tail (default: start at current head)
esctl events tail [--from-position N] [--from-beginning] [--watch] [--refresh 2s]

# Projections (cross-tenant)
esctl projection list

# Outbox
esctl outbox pending [--max-attempts 5] [--watch] [--refresh 2s]
esctl outbox dlq     [--max-attempts 5] [--limit N] [--after-position N]
```

### `--watch`

Most read commands accept `--watch` + `--refresh DURATION` for
continuous tailing. Polling cadence is clamped to ≥ 100 ms so an
accidental `--refresh 1ms` doesn't hammer your DB. Ctrl-C exits the
loop cleanly.

`events tail` defaults to `--watch` on; pass `--watch=false` for a
one-shot.

## Output

**pretty** (default) — multi-line per record, ANSI colour:

```
[v1 gp=1] myapp.employee.v1.Hired (employee:emp-42)
  event_id:    019e2a4a-99c2-75e3-9e7f-370ffcafd3d6
  occurred_at: 2026-05-15T06:19:52.642157Z
  hash:        cef5425418992b723975cb2c21bdf93dac6d40dff61c169ed65d664b7135dcf7
  prev_hash:   0000000000000000000000000000000000000000000000000000000000000000
  payload:     57 bytes (hex: 0a05656d702d311205416c6963651a11616c69636540…)
```

**json** — one object per line, fields stable. Pipe to `jq` for
filtering:

```sh
esctl --output json stream read --stream employee:emp-42 | jq -r '.type_url'
```

## Limitations (v1)

- **Event payloads are shown as hex bytes + the proto `type_url`.**
  Decoding to typed JSON needs a proto descriptor set; not yet
  wired. Planned for v2 via `--descriptor-set path.bin` (output of
  `buf build -o`).
- **No decryption.** Encrypted PII fields (per ADR 0010 / 0027) show
  as raw ciphertext in the hex dump. v2 will accept a KEK keyfile
  (inproc KMS) and decrypt with `--decrypt`. Production deployments
  using AWS / GCP / Vault KMS need the corresponding framework
  adapter to land first.
- **Read-only.** Operator actions (`projection reset`, `outbox replay`,
  `state-cache rebuild`) are not exposed yet. Use the framework
  helpers directly (`es.ProjectionAdmin`, `es.OutboxAdmin`,
  `aggregate.RebuildStateCache`).

## Examples

```sh
# Local SQLite, with .env set up
esctl stream list --type myapp.invoice.v1.Invoice
esctl stream read --stream "invoice:inv-001" --watch

# Postgres, one-shot via flags
esctl --db postgres://localhost/myapp --tenant acme \
      stream verify --stream "employee:emp-42"

# Tail every event in a tenant, JSON output piped to jq
esctl --db file:./events.db --tenant acme --output json \
      events tail --from-beginning | jq -r '"\(.global_position) \(.type_url)"'

# DLQ inspection, refreshing every 5 seconds
esctl --tenant acme outbox pending --watch --refresh 5s
```
