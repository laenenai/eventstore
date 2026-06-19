# Conversations: an AI backend in 500 lines of Go

A walking tour of `eventstore` through a real, runnable thing: a
multi-tenant chat backend with crypto-shredded history, live token
accounting, streaming responses, and a local LLM you can `ollama
pull`. By the end you'll have a `chat.db` you can grep, a JSON KMS
sidecar you can rotate, and a working mental model of how the
framework's primitives compose.

You'll see this in your terminal:

```text
starting new conversation 9ae371d2-… (tenant=acme user=alice model=llama3.2)

> what's the capital of France?

Paris is the capital of France.
  [tokens in=8 out=27 turns=1]

> tell me one fun fact about it

Paris is sometimes called the "City of Light"…
  [tokens in=15 out=68 turns=2]

> :quit

closed conversation 9ae371d2-…
final token usage: in=15 out=68 turns=2 model=llama3.2
```

Two SQLite files on disk afterward (`chat.db`, `chat.db.kms.json`),
both inspectable. Every user/assistant turn became one immutable event
plus one mutating read-model row, all in one local file.

---

## Before you start

You need three things:

```sh
# 1. Go (this repo pins 1.25.10 via mise/asdf)
go version            # if you don't have it, install via https://go.dev/dl/

# 2. Ollama, running locally
brew install ollama   # or download from https://ollama.com
ollama serve &        # leave running in another terminal

# 3. A small model
ollama pull llama3.2  # ~2 GB; use llama3.2:1b for a 1.3GB option
```

Clone the framework if you haven't:

```sh
git clone https://github.com/laenenai/eventstore
cd eventstore
```

That's it. No Docker, no Postgres, no cloud KMS.

---

## First run

```sh
go run ./examples/conversations/cmd/chat \
    --tenant acme \
    --user alice \
    --model llama3.2 \
    --db ./chat.db
```

A new `chat.db` and `chat.db.kms.json` appear in your current
directory. Type a message at the `>` prompt. Tokens stream onto stdout
as the model emits them; a rollup line prints after the reply settles.
`:quit` closes cleanly. Re-run the same command and a fresh
conversation starts in the same database, isolated by a UUID.

Tests don't need Ollama — they use a stub LLM:

```sh
go test ./examples/conversations/...
# ok  github.com/laenenai/eventstore/examples/conversations  0.6s
# 11 tests pass (plus the file KMS conformance suite runs in adapters/kms/file)
```

Everything below explains what just happened.

---

## The aggregate, one step at a time

An eventstore aggregate is three things: a **state** message, a sealed
sum of **commands**, and a sealed sum of **events**. All three live in
one `.proto` file (we use `proto/myapp/conversation/v1/conversation.proto`).

### 1. The state

This is what's in the database row that gets updated transactionally
on every successful command:

```protobuf
message Conversation {
  option (es.v1.aggregate) = "conversation";

  string conversation_id = 1;
  string user_id         = 2 [(es.v1.subject_field) = true];     // ① ties this aggregate to a GDPR subject
  string model           = 3 [(es.v1.data_classification) = DATA_CLASSIFICATION_INTERNAL];
  string system_prompt   = 4 [(es.v1.data_classification) = DATA_CLASSIFICATION_INTERNAL];

  repeated Turn turns    = 5;                                     // ② the rolling history; bounded in the Decider

  int64  tokens_input    = 6;
  int64  tokens_output   = 7;

  bool   closed          = 8;
  string close_reason    = 9 [(es.v1.data_classification) = DATA_CLASSIFICATION_INTERNAL];
}

message Turn {
  enum Role { ROLE_UNSPECIFIED = 0; ROLE_USER = 1; ROLE_ASSISTANT = 2; }
  Role   role    = 1;
  string content = 2 [(es.v1.data_classification) = DATA_CLASSIFICATION_PERSONAL];  // ③ encrypted on disk
  int64  tokens  = 3;
}
```

Three annotations carry the framework's contract:

- **①** `subject_field` tells the codegen that `user_id` is the
  GDPR-relevant subject. Every PERSONAL field across every event in
  this stream gets encrypted under a per-(tenant, user_id) DEK.
- **②** `turns` is the conversation history, kept in the state so the
  LLM has something to send the model without replaying events. The
  Decider caps the slice at 200 (`MaxTurns`) so this JSONB column
  doesn't grow unbounded.
- **③** `DATA_CLASSIFICATION_PERSONAL` is the lever — the codegen
  emits `EncryptPII`/`DecryptPII` methods that the framework calls at
  write/read time. `INTERNAL` fields (model, system prompt) stay
  plaintext: they're useful for ops queries and not subject-specific.

### 2. The commands

Four commands, each producing one event:

```protobuf
message Start { string tenant_id = 1; string conversation_id = 2; string user_id = 3 [(es.v1.subject_field) = true]; string model = 4; string system_prompt = 5 [(es.v1.data_classification) = DATA_CLASSIFICATION_INTERNAL]; }
message AppendUserMessage      { string tenant_id = 1; string conversation_id = 2; string user_id = 3 [(es.v1.subject_field) = true]; string content = 4 [(es.v1.data_classification) = DATA_CLASSIFICATION_PERSONAL]; int64 tokens = 5; }
message AppendAssistantMessage { string tenant_id = 1; string conversation_id = 2; string user_id = 3 [(es.v1.subject_field) = true]; string content = 4 [(es.v1.data_classification) = DATA_CLASSIFICATION_PERSONAL]; int64 tokens = 5; }
message Close                  { string tenant_id = 1; string conversation_id = 2; string user_id = 3 [(es.v1.subject_field) = true]; string reason = 4 [(es.v1.data_classification) = DATA_CLASSIFICATION_INTERNAL]; }

message Commands {
  option (es.v1.sum_type) = "Command";
  oneof variant {
    Start                  start                    = 1;
    AppendUserMessage      append_user_message      = 2;
    AppendAssistantMessage append_assistant_message = 3;
    Close                  close                    = 4;
  }
}
```

Commands describe *intent*. The Decider decides whether the intent is
allowed and what events to record.

### 3. The events

Past-tense facts. Same shape as commands minus the tenant header (the
envelope carries that):

```protobuf
message Started                  { string conversation_id = 1; string user_id = 2 [(es.v1.subject_field) = true]; string model = 3; string system_prompt = 4; }
message UserMessageAppended      { string conversation_id = 1; string user_id = 2 [(es.v1.subject_field) = true]; string content = 3 [(es.v1.data_classification) = DATA_CLASSIFICATION_PERSONAL]; int64 tokens = 4; }
message AssistantMessageAppended { string conversation_id = 1; string user_id = 2 [(es.v1.subject_field) = true]; string content = 3 [(es.v1.data_classification) = DATA_CLASSIFICATION_PERSONAL]; int64 tokens = 4; }
message Closed                   { string conversation_id = 1; string user_id = 2 [(es.v1.subject_field) = true]; string reason = 3; }

message Events {
  option (es.v1.sum_type) = "Event";
  oneof variant { Started started = 1; UserMessageAppended user_message_appended = 2; AssistantMessageAppended assistant_message_appended = 3; Closed closed = 4; }
}
```

Now `task generate` (from the repo root) emits
`gen/myapp/conversation/v1/conversation_es.pb.go` — sealed sum types,
codec, PII encrypt/decrypt methods, and the projection interface
you'll meet below. None of that file is hand-written.

### 4. The Decider

This is the one piece of business logic the framework cannot guess.
Open `examples/conversations/decider.go` — three functions: `Initial`,
`Decide`, `Evolve`. Here's the heart of it, in our case the
`AppendUserMessage` branch of `Decide`:

```go
case *conversationv1.AppendUserMessage:
    if err := guardStarted(s, cmd.UserId); err != nil {
        return nil, nil, err                                     // ① already-closed / wrong-user / not-started
    }
    if strings.TrimSpace(cmd.Content) == "" {
        return nil, nil, ErrEmptyMessage                         // ② cheap guard against UI bugs
    }
    if MaxTokensBudget > 0 && s.TokensInput+s.TokensOutput+cmd.Tokens > MaxTokensBudget {
        return nil, nil, ErrTokenBudgetExceeded                  // ③ runaway-loop ceiling
    }
    events := []conversationv1.Event{
        &conversationv1.UserMessageAppended{
            ConversationId: s.ConversationId,
            UserId:         s.UserId,
            Content:        cmd.Content,
            Tokens:         cmd.Tokens,
        },
    }
    if len(s.Turns)+1 >= MaxTurns {
        events = append(events, &conversationv1.Closed{          // ④ auto-close at the turn cap
            ConversationId: s.ConversationId,
            UserId:         s.UserId,
            Reason:         "max_turns_reached",
        })
    }
    return events, nil, nil
```

`Decide` returns three things: events to append, optional constraint
ops (we don't use any here — see ADR 0015), and an error. **It must
be pure**: no clocks, no I/O, no `time.Now()`. Workflow replays call
`Decide` and `Evolve` with old inputs; non-determinism is a silent
data-corruption bomb. The framework relies on convention here — the
upcoming `eslint-pure-decider` checker from ADR 0013 will turn the
convention into a build-time gate.

`Evolve` is the other half: given a state and an event, return a new
state. Always called immediately after `Decide` (in the same
transaction) AND on every read (to fold the events that have happened
since the cached state).

```go
case *conversationv1.UserMessageAppended:
    out.Turns = append(out.Turns, &conversationv1.Turn{
        Role:    conversationv1.Turn_ROLE_USER,
        Content: evt.Content,
        Tokens:  evt.Tokens,
    })
    out.TokensInput += evt.Tokens
```

Constants `MaxTurns = 200` and `MaxTokensBudget = 100_000` are package
constants — defensive ceilings against runaway loops and prompt-token
explosions. Adopters tune in production via a config object that the
Decider closes over.

**Try this:** in another shell, send an empty user message via the
estest harness and watch `ErrEmptyMessage` bubble up:

```sh
go test ./examples/conversations/... -run TestConversation_EmptyMessageRejected -v
```

---

## Wiring the runtime

Open `examples/conversations/cmd/chat/main.go`. The bottom half of
`run()` is the assembly that makes the aggregate work against SQLite +
the file KMS. Stripped to the essentials:

```go
db, _ := sql.Open("sqlite", dbPath)               // 1. the SQLite file
adapter := sqliteadapter.New(db)
adapter.Migrate(ctx)                              //    creates events, state_cache, subject_keys, projection_checkpoint, ...

keystore, _ := filekms.New(kmsFile)               // 2. file-backed KEK store (more on this next)
shredder := shred.New(keystore, adapter)          //    Shredder owns DEK lifecycle + crypto

rt := &aggregate.Runtime[*conversationv1.Conversation, conversationv1.Command, conversationv1.Event]{
    Store:    adapter,                            // 3. the runtime — generic over (State, Command, Event)
    Decider:  conversations.Decider,              //    the only hand-written business logic
    Codec:    conversationv1.EventCodec{},        //    codegen-emitted
    Shredder: shredder,                           //    optional — wire only if you have PERSONAL fields
}
```

That's the whole framework integration in 8 lines. Everything else —
`HandleCmd` for commands, `Load` for state, OCC on append, the
state_cache update, encryption, the audit trail — is the runtime's
job.

The interesting bit is the runtime's type parameters: it's generic
over `(S, C, E)`. The Decider declares those three types; the codec
serializes/deserializes the event sum; the shredder hooks the
encrypt/decrypt path. **No central registry**, no global init, no
reflection. You can run two runtimes for two different aggregates in
the same process against the same DB.

---

## Crypto-shredding: the part that's actually hard

GDPR Article 17 ("right to erasure"). The user wants their data gone.
Traditional event-sourcing answer: replay the whole log filtering
their events out, rewrite history. At LLM-conversation scale (millions
of conversations × hundreds of turns × a few KB per turn) that's
unworkable.

The framework's answer is **crypto-shredding**: every PERSONAL field
is encrypted at write time under a per-(tenant, subject) DEK. The DEK
is wrapped under a per-tenant KEK held in a KMS. To erase the subject,
you destroy the DEK. The ciphertext stays in the event log forever
(audit trail preserved); the plaintext is computationally
unrecoverable.

This is O(1): one DEK delete, regardless of how many events the
subject has.

The framework gives you three pieces:

| | Lives in | Holds |
| --- | --- | --- |
| **DEK** | `subject_keys` table in the event store | One row per (tenant, subject); the *wrapped* DEK bytes |
| **KEK** | a `kms.KeyStore` adapter | The 32-byte AES key that wraps DEKs |
| **`Shredder`** | `shred.New(keystore, store)` | Orchestrates Wrap/Unwrap on every encrypt/decrypt |

The framework ships two KMS adapters: `inproc` (KEKs in memory only,
**for tests only**) and `adapters/kms/aws` (production). For the
tutorial we want something in the middle — **a local file-backed KMS
that survives CLI restarts**. That's what `filekms.go` in this
example provides. ~150 lines of Go:

- KEKs persist to `chat.db.kms.json` (next to the events file)
- AES-256-GCM seal/open identical to inproc
- Atomic temp-file + rename on every save so a crash mid-write can't
  strand the file half-encoded

**Threat model**: anyone with read access to the JSON file can decrypt
every wrapped DEK and from there every PERSONAL field. Use the file
adapter locally (`adapters/kms/file`); in production, wire AWS KMS,
GCP KMS, Vault, or another HSM-backed adapter.

There's a third moving part: a **startup consistency check** in the
chat CLI (`assertKMSMatchesStore`). On boot it asks: is there a
`subject_keys` row for this (tenant, user) that references a KEK
version this file doesn't have? If yes, it aborts with the exact `rm`
commands to recover. This catches the common adopter footgun: blew
away the KMS file but kept the DB, now the encrypted history is
forever lost — but at least the error message tells you that
explicitly instead of surfacing a cryptic `KEK version not available`.

**Try right-to-erasure live:**

```sh
# Have a short conversation first so there's PII to shred
go run ./examples/conversations/cmd/chat --tenant acme --user alice --model llama3.2 --db ./demo.db

# Look at the raw bytes — your messages are ciphertext on disk
sqlite3 ./demo.db "SELECT hex(payload) FROM events WHERE type_url LIKE '%UserMessageAppended' LIMIT 1;"
# (you'll see proto-encoded bytes; the PERSONAL field is base64'd ciphertext, not plaintext)

# Now run the ForgetSubject test — it exercises the framework code path
go test -v -run TestConversation_ForgetSubjectRedactsContent ./examples/conversations/...
```

After `ForgetSubject(ctx, "acme", "alice")`, `Load`-ing any of alice's
conversations returns turns whose `Content` is empty (the framework's
`OnRedacted` callback fires with the missing field name). The events
themselves are untouched — that's the audit-trail property — but the
message bodies are gone.

---

## Streaming responses without losing replay determinism

LLMs stream. The user wants to see tokens appear letter-by-letter, not
wait for the full reply. But replay-determinism wants exactly one
durable event per assistant turn. How do you square these?

The example's answer is `WithStreamCallback`:

```go
resp, err := llm.Chat(ctx, model, messages,
    conversations.WithStreamCallback(func(chunk string) {
        fmt.Print(chunk)               // ① UX: each token goes straight to stdout
    }),
)
// ② One event with the assembled content gets persisted after Chat returns.
rt.Handle(ctx, sid, &conversationv1.AppendAssistantMessage{
    Content: resp.Content,             //    full assembled text — NOT one chunk per event
    Tokens:  resp.TokensOutput,
})
```

The callback runs during the model's emission. By the time `Chat`
returns, `resp.Content` holds the assembled full reply. The aggregate
gets **one** `AssistantMessageAppended` event with that content. Replay
sees one event, not N chunks; the system is deterministic.

A real-world subtlety we discovered the hard way: Genkit's Ollama
plugin (v1.9.0) delivers the entire reply through the stream callback
*and leaves `resp.Text()` empty*. The example's `GenkitOllama.Chat`
assembles chunks in parallel and falls back to the assembled buffer
when the response text is empty. Adopters wiring other Genkit plugins
should keep the same belt-and-braces approach.

**Verify the one-event-per-turn property:**

```sh
go run ./examples/conversations/cmd/chat --tenant acme --user alice --model llama3.2 --db ./check.db
# Have one exchange, then :quit

sqlite3 ./check.db "SELECT type_url, COUNT(*) FROM events GROUP BY type_url;"
# myapp.conversation.v1.Started|1
# myapp.conversation.v1.UserMessageAppended|1
# myapp.conversation.v1.AssistantMessageAppended|1
# myapp.conversation.v1.Closed|1
```

One row per turn, regardless of how many tokens streamed.

---

## Reading what you wrote: the token-usage projection

So far we've only used **Tier 1** (the `state_cache`) — every command
updates the state JSONB in the same transaction as the events,
read-your-writes consistency comes free.

For dashboards and billing you want a **Tier 3** projection: an
asynchronous read-model that follows the event log, advances a
checkpoint, and materialises something query-shaped. This example
ships one — `token_usage` — that gives you per-conversation token
totals you can `SELECT … GROUP BY tenant_id, model` for cost
attribution.

Here's the whole handler (`projection_tokens.go`):

```go
type TokenUsageProjection struct{ DB *sql.DB }

// Codegen emits conversationv1.Projection as an EXHAUSTIVE interface
// — one method per event variant. Adding a new event variant fails
// the build until this projection implements it. ADR 0020 § 3a.
var _ conversationv1.Projection = (*TokenUsageProjection)(nil)

func (p *TokenUsageProjection) OnStarted(ctx context.Context, env es.Envelope, e *conversationv1.Started) error {
    // INSERT the conversation row with zero totals.
}
func (p *TokenUsageProjection) OnUserMessageAppended(ctx context.Context, env es.Envelope, e *conversationv1.UserMessageAppended) error {
    return p.bump(ctx, env, e.ConversationId, e.Tokens, 0)  // tokens_input += e.Tokens
}
func (p *TokenUsageProjection) OnAssistantMessageAppended(ctx context.Context, env es.Envelope, e *conversationv1.AssistantMessageAppended) error {
    return p.bump(ctx, env, e.ConversationId, 0, e.Tokens)  // tokens_output += e.Tokens
}
func (p *TokenUsageProjection) OnClosed(ctx context.Context, env es.Envelope, _ *conversationv1.Closed) error {
    // bump last_event_at; no token change
}
```

That's the whole logic. The runtime wiring in the chat CLI:

```go
db.ExecContext(ctx, conversations.TokenUsageSchema)        // 1. create token_usage table

proj := &conversations.TokenUsageProjection{DB: db}
projRuntime := &projection.Runtime{
    Name:       conversations.TokenUsage,                  // 2. checkpoint key
    Store:      adapter,                                   // 3. SQLite adapter doubles as
    Checkpoint: adapter,                                   //    Store AND Checkpoint
    Handler:    conversationv1.NewProjectionDispatcher(proj, projection.IgnoreUnknown()),
    Tenant:     tenantID,
    IdleSleep:  250 * time.Millisecond,                    // 4. chat-feel responsiveness
}
go projRuntime.Run(ctx)                                    // 5. fire-and-forget poll loop
```

`projection.IgnoreUnknown()` is the "we share this DB with other
aggregates" hint — without it, the dispatcher errors on TypeURLs it
doesn't recognise. ADR 0020 § 3b.

After running the chat for a few turns:

```sh
sqlite3 ./chat.db "SELECT conversation_id, model, tokens_input, tokens_output, turns FROM token_usage;"
# 9ae371d2-…|llama3.2|23|95|2
```

The checkpoint table tracks how far the projection has read:

```sh
sqlite3 ./chat.db "SELECT name, tenant_id, cursor FROM projection_checkpoint;"
# token-usage|acme|4
```

Stop the CLI, restart it, the projection resumes from cursor 4 — no
double-counting.

**Try a rebuild:** the framework ships `esctl projection reset` for
operational use:

```sh
go run ./cmd/esctl --db file:./chat.db --tenant acme projection reset --name token-usage --yes
sqlite3 ./chat.db "DELETE FROM token_usage;"
# Now restart the chat — the projection re-reads from gp=0 and rebuilds.
```

That's the ADR 0020 § 3g "operator-owned truncate, framework-owned
cursor" pattern in action.

---

## Multi-tenant: try to break it

The framework refuses to operate without a tenant. **Every** command,
**every** read goes through `es.WithTenant(ctx, tenantID)`. The
`StreamID` is `<tenant>:conversation:<id>` — tenant is part of the
identity, not metadata.

Open two terminals, run the chat with two tenants:

```sh
# Terminal 1
go run ./examples/conversations/cmd/chat --tenant acme  --user alice --model llama3.2 --db ./shared.db

# Terminal 2 (same DB file)
go run ./examples/conversations/cmd/chat --tenant globex --user alice --model llama3.2 --db ./shared.db
```

Both write to the same SQLite file. Both call alice a different
person, with a different DEK. The `events` table mixes them:

```sh
sqlite3 ./shared.db "SELECT tenant_id, stream_id, type_url FROM events LIMIT 10;"
# acme  |acme/conversation:…   |myapp.conversation.v1.UserMessageAppended
# globex|globex/conversation:…  |myapp.conversation.v1.UserMessageAppended
```

But neither chat can `Load` the other's stream. `TestConversation_TenantIsolation`
proves it:

```go
ctx_b := es.WithTenant(ctx, "tenant-b")
state, _, _ := rt.Load(ctx_b, mustStream("tenant-b", "shared-id"))
// state.ConversationId == "" — tenant-b sees nothing of tenant-a's stream
```

For production Postgres deployments, RLS (ADR 0032) bolts a second
layer: the `eventstore_app` Postgres role has no `BYPASSRLS`, and the
policy on `events` keys off `current_setting('app.tenant_id', false)`.
Even a SQL-injection escape can't cross tenants. SQLite has no RLS
equivalent — the framework's `es.WithTenant` enforcement is the only
gate. For local dev that's plenty.

---

## What this saved you from writing

The example weighs ~500 lines of new Go (proto + Decider + Ollama
client + projection + CLI + tests). The framework gives you
the rest of the iceberg: the SQLite migrations, the OCC append, the
state_cache, the projection runner, the checkpoint table, the shredder,
the codec emitter, the type-safe sum-types, the projection-interface
exhaustiveness check, the multi-tenant isolation, the `esctl` operator
CLI for free.

Doing this on raw Postgres-with-events you'd be writing:

- a migration story (~200 LOC + ~12 SQL files)
- an event-append wrapper that handles OCC, payload encoding, envelope
  metadata, hash chain (~300 LOC)
- a state-cache implementation including JSONB schema + upsert
  semantics (~150 LOC)
- a per-subject KMS abstraction + DEK lifecycle + crypto-shredding
  (~250 LOC, easy to get subtly wrong)
- a projection runner with checkpoint persistence, fail-stop semantics,
  optional DLQ (~200 LOC)
- a multi-tenant enforcement layer (~80 LOC)
- a generic codec / sum-type pattern, or you give up and use
  `interface{}` everywhere (~150 LOC)

That's a ~1300-LOC framework-shaped scaffolding even before you write
your first business rule. The wedge isn't "convenient ES library" —
it's "the regulated multi-tenant SaaS scaffolding solved once".

---

## What's next on this branch

Three follow-ups, all proto-additive (no breaking changes):

- **Tool-call / tool-result events.** Two new event variants in the
  sum. The Decider learns about outstanding-tool-call IDs; agents
  driving multi-step reasoning can now persist their structured
  thought process. Codegen handles the rest.
- **RAG embedding projection.** A second `projection.Runtime` that
  upserts each turn's embedding to a vector store (Pinecone / Qdrant /
  pgvector). The interesting design question is GDPR: embeddings of
  decrypted content outlive `ForgetSubject` unless you key the vector
  rows on `event_id` and listen for a shred-audit event. Documented
  inline when this lands.
- **HTTP/Connect edge.** The CLI is fine for the tutorial; production
  wants an HTTP endpoint. Cookbook 15 already covers the Connect
  integration pattern — this wraps `HandleCmd` and adds the streaming
  response path.

---

## The 11 tests, what they prove

| Test | What it proves |
| --- | --- |
| `TestConversation_Lifecycle` | Start → 2 turns → close; final state shape + token totals match |
| `TestConversation_PIIEncryptedAtRest` | Raw `Payload` bytes on disk don't contain the user's plaintext |
| `TestConversation_ForgetSubjectRedactsContent` | After `ForgetSubject`, every `Turn.Content` is gone; `OnRedacted` fires |
| `TestConversation_AppendBeforeStartRejected` | `ErrNotStarted` for a command against an unborn stream |
| `TestConversation_AppendAfterCloseRejected` | `es.ErrTerminal` after `IsTerminal` returns true (the framework's terminal-stream guard fires before the Decider's own check) |
| `TestConversation_UserMismatchRejected` | A command claiming a different `user_id` than the stream owner is rejected |
| `TestConversation_EmptyMessageRejected` | Empty / whitespace-only message bodies fail at `Decide` time |
| `TestConversation_StreamingDelivery` | Stream callback fires per chunk in order; assembled chunks equal `ChatResponse.Content`; persisted event uses the assembled content |
| `TestConversation_TenantIsolation` | tenant-b cannot `Load` tenant-a's stream with the same conversation_id |
| (file KMS coverage) | The framework's `estest.RunKMSConformance` suite — incl. `PersistsAcrossInstances` — runs against the `adapters/kms/file` adapter; see `adapters/kms/file/file_test.go` |
| `TestTokenUsageProjection_EndToEnd` | Codegen dispatcher routes events to the right `OnXxx`; read-model row converges with correct turn count + token totals |
| `TestTokenUsageProjection_RuntimePolls` | The polling integration works: `aggregate.Runtime` writes events, `projection.Runtime.RunOnce` reads them, the row converges |

Run them all with `go test ./examples/conversations/...`.

---

## Troubleshooting

**`KEK version N not available for tenant "X" (have 0 versions)`** —
the KMS sidecar doesn't have the key that was used to wrap a DEK
recorded in the event store. Most common cause: you deleted
`chat.db.kms.json` but kept `chat.db`. The encrypted PII in
`chat.db` is now unrecoverable (this is the crypto-shred property
doing its job; just unwanted here). Recovery:

```sh
rm -f chat.db chat.db.kms.json
```

Then re-run. The chat CLI will emit a clearer version of this error
at startup (with absolute paths) for any KMS↔store mismatch it
detects on boot.

**`message content cannot be empty` from Ollama** — the LLM returned
an empty reply, usually because the model is still warming up
(first request after `ollama serve`) or out of context. Retry; if it
persists, swap to a smaller model (`llama3.2:1b`) and try again.

**`ollama: POST … connection refused`** — Ollama isn't running.
`ollama serve &` in another terminal.

---

## Going deeper

- ADR 0003 — the Decider model (`Initial`/`Decide`/`Evolve`)
- ADR 0004 — sum-type representation (why the proto containers and
  marker methods look the way they do)
- ADR 0007 — first-class multi-tenancy
- ADR 0010 — crypto-shredding (the DEK/KEK design)
- ADR 0020 — projections and the three tiers
- ADR 0023 — `state_cache` (read-your-writes without replay)
- ADR 0027 — data classification (the enum + codegen behaviour)
- Cookbook 11 — crypto-shredding operator runbook
- Cookbook 21 — schema evolution / upcasters
- Cookbook 22 — sync read models with subscribers

If you only read one thing next: **ADR 0020**. The three-tier
projection model (state_cache → materialised views → custom
projections) is the load-bearing call for any non-trivial
event-sourced app and most adopters get it wrong on the first try.
