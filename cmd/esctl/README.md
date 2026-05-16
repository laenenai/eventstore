# esctl

Eventstore debugging + operator CLI. Inspect streams, events,
state_cache, projections, and outbox state — and run the standard
operator write actions (projection reset, DLQ retry/abandon,
state_cache wipe) — against either the Postgres or the SQLite
adapter, auto-detected from the `--db` URL.

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
| `--yes`, `-y` | `ESCTL_YES` | Confirm destructive write commands. Without it, every write command is a DRY RUN. |

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

# Projections — read
esctl projection list
# Projections — write (see "Write commands" below)
esctl projection reset    --name <name> [--all-tenants]
esctl projection reset-to --name <name> --position <gp>

# Outbox — read
esctl outbox pending [--max-attempts 5] [--watch] [--refresh 2s]
esctl outbox dlq     [--max-attempts 5] [--limit N] [--after-position N]
# Outbox — write
esctl outbox retry     --position <gp>
esctl outbox retry-all [--max-attempts N]
esctl outbox abandon   --position <gp>

# state_cache — operator write
esctl state-cache rebuild --type <typeURL>
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

## Write commands

Every write command requires `--yes` (or `-y`) to actually execute.
Without it, the command prints `DRY RUN: would <cmd> <args>` and
exits with status 0 — letting you preview an action before committing
to it. Each successful write also emits a structured audit line to
**stderr** prefixed with `[esctl-write]`; pipe it to syslog, a file,
or your log collector for an operator-action trail:

```
[esctl-write] projection reset tenant=acme name=billing at=2026-05-14T12:34:56.789Z
```

### Projection reset / partial replay

```sh
esctl projection reset --name billing --tenant acme --yes
esctl projection reset --name billing --all-tenants --yes
esctl projection reset-to --name billing --tenant acme --position 12345 --yes
```

`reset` zeroes the projector's cursor; the runner replays from
`gp=0` on its next tick. Reseting cursor is half the work — the
**read-model TRUNCATE is application-specific** and must be done
separately (cookbook recipe 08). `reset-to` is for partial replay
from a known-good cursor.

### Outbox DLQ retry + abandon

```sh
esctl outbox retry     --position 42  --tenant acme --yes        # single row
esctl outbox retry-all --max-attempts 5 --tenant acme --yes      # whole tenant's DLQ
esctl outbox abandon   --position 42  --tenant acme --yes        # drop the row
```

`retry` resets a single DLQ'd row's attempts so the next drain
picks it up; use after the publisher-side root cause is fixed.
`retry-all` does the same for every DLQ'd row in a tenant — handy
after a publisher outage recovery. `abandon` closes the outbox row
without ever publishing the event (the event row itself stays in
`events`; only the outbox marker goes — ADR 0005).

### state_cache rebuild (wipe-only)

```sh
esctl state-cache rebuild --type myapp.employee.v1.Employee --tenant acme --yes
```

This deletes every state_cache row for the given (tenant, typeURL).
The next `Load()` of each affected stream rebuilds from full event
replay (ADR 0023).

**Limitation: `esctl` is generic and has no compiled-in proto
types**, so it cannot run `aggregate.RebuildStateCache` directly
(that helper needs your aggregate's typed `Runtime[S, C, E]` to
decode events and re-fold state). The wipe-only path is the
generic-tool-safe form: lazy on-Load rebuild. If you need eager
rebuild, write a small Go program around `aggregate.RebuildStateCache`
— that path is documented in cookbook recipe 08.

## Limitations

- **Event payloads are shown as hex bytes + the proto `type_url`.**
  Decoding to typed JSON needs a proto descriptor set; not yet
  wired. Planned via `--descriptor-set path.bin` (output of
  `buf build -o`).
- **No decryption.** Encrypted PII fields (per ADR 0010 / 0027) show
  as raw ciphertext in the hex dump. A future iteration will accept
  a KEK keyfile (inproc KMS) and decrypt with `--decrypt`. Production
  deployments using AWS / GCP / Vault KMS need the corresponding
  framework adapter to land first.
- **state-cache rebuild is wipe-only** in esctl; see the write
  commands section above for the rationale and the typed-runtime
  alternative.

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

# Preview a projection reset (DRY RUN), then commit
esctl --tenant acme projection reset --name billing
esctl --tenant acme projection reset --name billing --yes

# Retry every DLQ'd row for a tenant, with audit line captured
esctl --tenant acme outbox retry-all --max-attempts 5 --yes 2>>esctl-audit.log
```
