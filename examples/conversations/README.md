# Example: conversations

A worked example of an AI-application backend built on `eventstore`:
a multi-tenant **Conversation** aggregate that captures every turn of a
user/agent dialogue as an event, with per-tenant crypto-shredding,
token accounting, RAG embedding projection, and read-your-writes
streaming responses.

This README is the design contract; the MVP ships the load-bearing
pieces (proto + Decider + Ollama client + runnable CLI + the test
matrix that proves crypto-shred works end-to-end). Optional layers
flagged below land in follow-up commits without breaking changes.

## Run it

The LLM driver uses [Genkit](https://genkit.dev/go/docs/) with its
Ollama plugin. Swapping to Anthropic / Vertex / OpenAI in production
is one import + one model definition — the conversation aggregate
stays provider-agnostic via the framework-internal `LLM` interface.

```sh
# 1. Start Ollama and pull a small model
ollama serve &
ollama pull llama3.2

# 2. Run the chat. SQLite file is created if missing; same --user
#    + --conversation resumes the same stream via state_cache.
go run ./examples/conversations/cmd/chat \
    --tenant acme \
    --user alice \
    --model llama3.2 \
    --db ./chat.db

# 3. Talk. Type :quit to close cleanly.
```

Tests run without Ollama (they use a stub `LLM`):

```sh
go test ./examples/conversations/...
```

## Why this example exists

`eventstore` was built for regulated multi-tenant SaaS. AI applications
are the sharpest current expression of that shape:

- Conversations are append-only event streams by nature.
- Tool calls and tool results are the audit trail regulators ask for.
- PII inside user messages must be GDPR-erasable in O(1), not by
  re-walking petabytes of LLM transcripts.
- One platform, many tenants — each tenant's transcripts must be
  cryptographically isolated, not just logically.

Everything below is achievable today with the framework's primitives.
This example wires them up.

## What you get

| Capability                                   | Status   | Where it lives                                                  |
| -------------------------------------------- | -------- | --------------------------------------------------------------- |
| Append-only conversation history             | ✅ MVP   | `Conversation` aggregate, Decider pattern (ADR 0003)            |
| User PII encrypted per-tenant                | ✅ MVP   | `(es.v1.data_classification) = PERSONAL` on message bodies      |
| Right-to-erasure ("forget this user")        | ✅ MVP   | `shred.ForgetSubject(tenant, userID)` — O(1) DEK destruction    |
| Ollama-backed local LLM loop                 | ✅ MVP   | `examples/conversations/cmd/chat`                                |
| Multi-tenant isolation                       | ✅ MVP   | `es.WithTenant` + `StreamID` (proven in `TestConversation_TenantIsolation`) |
| Read-your-writes (no replay on every turn)   | ✅ MVP   | Tier-1 `state_cache` via `aggregate.Runtime` (ADR 0023)         |
| Token accounting per-tenant, per-model       | follow-up | Tier-3 projection `token_usage` table                           |
| RAG / semantic recall                        | follow-up | Tier-3 projection upserts embeddings to a vector store          |
| Streaming LLM responses (token-by-token)     | follow-up | Sync subscriber pattern (cookbook 22)                          |
| Tool-call / tool-result events               | follow-up | New event variants — proto-additive, no breaking change         |
| Cost attribution / billing                   | follow-up | Same `token_usage` projection, aggregated by `(tenant, month)`  |
| DSAR export                                  | framework-side | `shred.RunSubjectExport` + esctl raw mode (PR #26)          |
| Conversation rebuild after schema change     | framework-side | `RebuildStateCache` (ADR 0023)                              |

## The aggregate

### Shape

One stream per conversation: `tenant:conversation:<conversation_id>`.

State is the rolling shape of the dialogue plus per-tenant metadata
needed to make decisions deterministically:

```protobuf
// proto/myapp/conversation/v1/conversation.proto
syntax = "proto3";
package myapp.conversation.v1;

import "es/v1/options.proto";

option go_package = "github.com/laenenai/eventstore/examples/conversations/gen/myapp/conversation/v1;conversationv1";

// State - the rolling shape of one conversation. The slice of turns
// is bounded by Decider rules (max 1000 turns / 250k tokens) so the
// state cache row stays small enough to deserialize in microseconds.
message Conversation {
  option (es.v1.aggregate) = "conversation";

  string conversation_id = 1;
  string user_id         = 2 [(es.v1.subject_field) = true];
  string model           = 3;   // e.g. "claude-opus-4-7"
  string system_prompt   = 4 [(es.v1.data_classification) = DATA_CLASSIFICATION_INTERNAL];

  repeated Turn turns    = 5;

  int64  tokens_input    = 6;
  int64  tokens_output   = 7;
  int64  tokens_cached   = 8;
  bool   closed          = 9;
  string close_reason    = 10;  // "user_ended" | "policy_violation" | "token_budget_exhausted"
}

message Turn {
  enum Role {
    ROLE_UNSPECIFIED = 0;
    ROLE_USER        = 1;
    ROLE_ASSISTANT   = 2;
    ROLE_TOOL        = 3;
  }
  Role   role     = 1;
  string content  = 2 [(es.v1.data_classification) = DATA_CLASSIFICATION_PERSONAL];
  int64  tokens   = 3;
  string turn_id  = 4;  // ULID; matches the event_id of the event that produced it
}
```

### Commands

```protobuf
// Initialize the conversation. Sets the model, the system prompt, and
// the binding subject (user_id) - that subject's ForgetSubject call
// later erases every PERSONAL field across every event in this stream.
message StartConversation {
  string tenant_id       = 1;
  string conversation_id = 2;
  string user_id         = 3 [(es.v1.subject_field) = true];
  string model           = 4;
  string system_prompt   = 5 [(es.v1.data_classification) = DATA_CLASSIFICATION_INTERNAL];
}

// One user turn. The body is encrypted under the user's DEK per ADR 0010.
message AppendUserMessage {
  string tenant_id       = 1;
  string conversation_id = 2;
  string user_id         = 3 [(es.v1.subject_field) = true];
  string content         = 4 [(es.v1.data_classification) = DATA_CLASSIFICATION_PERSONAL];
  int64  tokens_input    = 5;
}

// The model invoked a tool. The arguments may carry PII so they're
// classified PERSONAL by default; downgrade per tool if your tools
// only see opaque IDs.
message RecordToolCall {
  string tenant_id       = 1;
  string conversation_id = 2;
  string user_id         = 3 [(es.v1.subject_field) = true];
  string tool_name       = 4 [(es.v1.data_classification) = DATA_CLASSIFICATION_INTERNAL];
  string tool_call_id    = 5;
  string arguments_json  = 6 [(es.v1.data_classification) = DATA_CLASSIFICATION_PERSONAL];
  int64  tokens_used     = 7;
}

// Tool returned a result. Result bodies often contain retrieved
// documents that may themselves carry PII - hence PERSONAL.
message RecordToolResult {
  string tenant_id       = 1;
  string conversation_id = 2;
  string user_id         = 3 [(es.v1.subject_field) = true];
  string tool_call_id    = 4;
  string result_json     = 5 [(es.v1.data_classification) = DATA_CLASSIFICATION_PERSONAL];
  bool   is_error        = 6;
}

// The final, durable record of the assistant's response after the
// streamed tokens have settled. See "Streaming responses" below for
// why this command exists separately from a streaming-token event.
message AppendAssistantMessage {
  string tenant_id       = 1;
  string conversation_id = 2;
  string user_id         = 3 [(es.v1.subject_field) = true];
  string content         = 4 [(es.v1.data_classification) = DATA_CLASSIFICATION_PERSONAL];
  int64  tokens_output   = 5;
  int64  tokens_cached   = 6;  // for prompt-cache hit accounting (ADR 0023 of the Claude API)
}

message CloseConversation {
  string tenant_id       = 1;
  string conversation_id = 2;
  string user_id         = 3 [(es.v1.subject_field) = true];
  string reason          = 4;
}
```

### Events

Each command produces exactly one event with the matching past-tense
name (`ConversationStarted`, `UserMessageAppended`, `ToolCallRecorded`,
`ToolResultRecorded`, `AssistantMessageAppended`, `ConversationClosed`).
PII annotations on event fields mirror the command they're produced
from.

### Decider rules

- **`StartConversation`** rejects an already-started stream
  (`ErrAlreadyStarted`). Validates `model` against an allowlist.
- **`AppendUserMessage`** rejects on a closed conversation
  (`ErrConversationClosed`); enforces per-tenant token budget if
  configured (`ErrTokenBudgetExceeded`).
- **`RecordToolCall` / `RecordToolResult`** must reference a tool call
  that was issued by a prior `AssistantMessage` turn for which the
  Decider tracked outstanding tool calls; rejects orphan
  results with `ErrUnknownToolCall`.
- **`AppendAssistantMessage`** rejects on a closed conversation; cuts
  the stream off at `MaxTurns` (default 1000) with an automatic
  `ConversationClosed{reason: "max_turns_reached"}`.
- **`CloseConversation`** is idempotent.

The Decider is pure: no clocks, no I/O, no `time.Now()`. The runtime
injects `Clock` via the framework's `Clock` interface (cookbook 18).

## Crypto-shredding

The aggregate declares `user_id` as the `subject_field`. Every event in
the stream carries that user_id as its subject. PERSONAL-classified
fields (`content`, `arguments_json`, `result_json`) are encrypted
under a per-(tenant, user_id) DEK at write time.

When the user invokes their right to erasure:

```go
shredder.ForgetSubject(ctx, tenantID, userID)
```

The DEK is destroyed. Every event in every conversation for that user
across every tenant remains in the event log (the audit trail is
preserved), but its PERSONAL fields are cryptographically
inaccessible. Reads return `RedactedFields` markers, not plaintext.

This is O(1) — one row in `subject_keys` per (tenant, subject) is
deleted. Compare to the alternative of walking petabytes of LLM
transcripts to physically erase PII.

See cookbook 11 for the operator runbook.

## Token accounting projection (Tier 3)

A `projection.Runtime` consuming this aggregate's events writes one
row per (tenant, conversation, model) to a `token_usage` read model
in the same Postgres:

```sql
CREATE TABLE token_usage (
    tenant_id        TEXT NOT NULL,
    conversation_id  TEXT NOT NULL,
    model            TEXT NOT NULL,
    tokens_input     BIGINT NOT NULL DEFAULT 0,
    tokens_output    BIGINT NOT NULL DEFAULT 0,
    tokens_cached    BIGINT NOT NULL DEFAULT 0,
    last_event_at    TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (tenant_id, conversation_id, model)
);
```

Billing aggregates this view by `(tenant_id, date_trunc('month', last_event_at))`.

Projection handler is exhaustively typed via codegen-emitted
`conversationv1.Projection` interface (ADR 0020 Decision 3a) — adding
a new event variant to the proto breaks the projection at compile
time until a new method is implemented.

## RAG / embedding projection (Tier 3)

A second projection upserts conversation turns into a vector store
(Pinecone / Qdrant / pgvector — adopter's choice) for later semantic
recall. Two design notes:

1. **The embedding upsert must be idempotent on `event_id`.** Vector
   stores rarely have transactional joins back to Postgres; if the
   projection retries from the framework checkpoint, you'll re-embed
   the same turn unless the upsert key is the `event_id`.

2. **Embeddings of decrypted content survive ForgetSubject.** That's
   a real GDPR consideration. The recommended pattern is to either:
   - Use the `event_id` as the vector store row key and add a
     `subject_id` column, so `ForgetSubject` triggers a vector-store
     deletion via a small companion projection that listens for the
     shred event (when ADR 0010's shred-audit-event lands), or
   - Embed only the assistant's responses (which are derived) and
     keep the user's raw turn out of the vector store, accepting a
     recall quality hit.

   This example uses option (1) and documents the tradeoff in the
   compliance addendum.

## Streaming LLM responses

The hot path that AI backends care about: token-by-token streaming
to the client while still ending with one durable event.

**Pattern:** the HTTP/2 (Connect) endpoint receives `AppendUserMessage`,
calls `HandleCmd` to persist it, then opens a streaming response. As
the LLM emits tokens, they go directly to the client over the stream.
When the stream completes, the endpoint constructs one
`AppendAssistantMessage` command with the final, settled content and
total token count, and calls `HandleCmd` a second time.

The intermediate token chunks are **not events**. Events represent
durable, replayable facts; partial-token chunks are ephemeral I/O.

```
HTTP/2 client
  ↓ POST /Conversation/Append → UserMessage
  HandleCmd (durable) ────────→ UserMessageAppended event
  ↓ stream tokens via Anthropic SDK
  ↑ ↑ ↑ ↑ ↑ ↑ chunks back to client
  ↓ stream done, full content known
  HandleCmd (durable) ────────→ AssistantMessageAppended event
  ↓ end response
```

Read-your-writes for the UI ("the last 5 turns I sent are visible
immediately on page reload") is delivered by Tier 1 `state_cache` —
the post-Decide state is mirrored synchronously in the same tx as
the events, so a `Load` after `HandleCmd` returns the same shape the
caller just produced.

## Multi-tenant isolation

Every command requires `es.WithTenant(ctx, tenantID)` (ADR 0007). The
StreamID is `<tenant>:conversation:<conversation_id>`. Production
deployments enable Postgres RLS (ADR 0032) so even a SQL-injection
escape can't cross tenants — the `eventstore_app` role has no
`BYPASSRLS`, and the policy on `events` keys off
`current_setting('app.tenant_id', false)`.

Test fixture demonstrates this with a multi-tenant set:

```go
t.Run("cross_tenant_isolation", func(t *testing.T) {
    ctx1 := es.WithTenant(ctx, "tenant-a")
    ctx2 := es.WithTenant(ctx, "tenant-b")
    // ... start conversations in both; assert tenant-a's runtime
    // can't observe tenant-b's stream via Load or ReadAll.
})
```

## The complete test matrix

The example ships with tests covering the full claim set:

| Test                                  | What it proves                                                       |
| ------------------------------------- | -------------------------------------------------------------------- |
| `TestConversation_Lifecycle`          | Start → append × N → close. Decider returns the right errors.        |
| `TestConversation_ToolCallSequence`   | Tool call → result correlation; orphan tool result rejected.         |
| `TestConversation_TokenBudget`        | Budget exhaustion auto-closes; final event records reason.           |
| `TestConversation_ForgetSubject`      | After `shred.ForgetSubject(user_id)`, every PERSONAL field is gone.  |
| `TestConversation_TenantIsolation`    | tenant-a cannot read tenant-b's stream (Load + ReadAll both refuse). |
| `TestConversation_StateCache`         | `Load` after `HandleCmd` returns the post-Decide state (no replay).  |
| `TestConversation_TokenUsageProjection` | Projection accumulates token totals correctly per (tenant, model). |
| `TestConversation_StreamingResponse`  | Two HandleCmd calls (user → assistant) produce two events.           |
| `TestConversation_RebuildAfterSchemaChange` | `RebuildStateCache` recovers correct state after Evolve fix.  |

Tests use `estest`'s `given/when/then` harness against both the SQLite
and Postgres adapters via the conformance pattern.

## Layout

```
examples/conversations/
├── README.md                  (this file)
├── go.mod                     own module
├── proto/myapp/conversation/v1/conversation.proto
├── gen/                       (generated; do not edit)
├── decider.go                 Initial / Decide / Evolve
├── errors.go                  sentinels
├── projection_tokens.go       token_usage projection handler
├── projection_embeddings.go   vector-store projection handler
├── http/                      Connect endpoint + streaming response
│   ├── server.go
│   └── stream.go
└── conversation_test.go       the matrix above
```

## What this example is NOT

- **Not a production LLM-agent framework.** This is a *backend* — a
  durable, auditable, GDPR-compliant event log for conversations.
  Agent orchestration (tool planning, multi-step reasoning) lives in
  the application code that calls the LLM SDK.
- **Not a vector-store implementation.** The RAG projection is a
  thin upsert; the vector store is your choice.
- **Not a payment/billing system.** `token_usage` is the data source
  for billing; the billing logic itself is out of scope.
- **Not an LLM-evaluation framework.** Tracing/scoring of LLM outputs
  is a separate concern that consumes this stream via a Tier-3
  projection.

## Related ADRs and recipes

- ADR 0003 — Decider aggregate model
- ADR 0007 — First-class multi-tenancy
- ADR 0010 — Crypto-shredding
- ADR 0020 — Projections and read models (the three tiers)
- ADR 0023 — `state_cache` (read-your-writes for the conversation UI)
- ADR 0027 — Data governance model (the classification semantics)
- ADR 0032 — Postgres RLS for tenant isolation
- Cookbook 11 — Crypto-shredding operator runbook
- Cookbook 14 — `cmdworkflow` deployment patterns
- Cookbook 15 — HTTP edge with Connect (the streaming endpoint)
- Cookbook 18 — Clock injection (deterministic tests)
- Cookbook 22 — Sync read models with subscribers
