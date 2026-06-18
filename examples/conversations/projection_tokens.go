package conversations

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/laenenai/eventstore/es"
	conversationv1 "github.com/laenenai/eventstore/gen/myapp/conversation/v1"
)

// TokenUsage is the projection name written into projection_checkpoint
// rows and the read-model table. Exposed so the CLI and tests can
// reference it without hard-coding the literal.
const TokenUsage = "token-usage"

// TokenUsageSchema is the SQL the example runs at startup to create
// the read-model table the projection writes to. Application-owned
// per ADR 0020 decision 3c — the framework codegens dispatch only,
// not read-model schemas. Kept in code rather than a migrations
// directory because the example is a single binary; production
// adopters move equivalent DDL into their app's migration tool.
const TokenUsageSchema = `
CREATE TABLE IF NOT EXISTS token_usage (
    tenant_id        TEXT NOT NULL,
    conversation_id  TEXT NOT NULL,
    model            TEXT NOT NULL,
    user_id          TEXT NOT NULL,
    tokens_input     INTEGER NOT NULL DEFAULT 0,
    tokens_output    INTEGER NOT NULL DEFAULT 0,
    turns            INTEGER NOT NULL DEFAULT 0,
    last_event_at    TEXT NOT NULL,
    PRIMARY KEY (tenant_id, conversation_id)
);
`

// TokenUsageRow is the application-facing shape returned by
// TokenUsageReader.Get. Adopters with billing pipelines join this
// against their per-tenant rate cards to compute spend.
type TokenUsageRow struct {
	TenantID       string
	ConversationID string
	Model          string
	UserID         string
	TokensInput    int64
	TokensOutput   int64
	Turns          int64
	LastEventAt    string
}

// TokenUsageProjection implements conversationv1.Projection by
// upserting into token_usage on every conversation event. Handler is
// idempotent on (tenant, conversation_id): re-applying the same event
// during projection replay produces a row whose totals reflect the
// SUM of events applied, so a stale checkpoint that re-runs the last
// batch double-counts. Production adopters using fail-stop mode (ADR
// 0020 decision 3d) rely on the checkpoint's last-success advance to
// avoid this; adopters using DLQ-skip mode wrap this handler with
// projection.WithDedup against processed_events.
//
// One TokenUsageProjection per aggregate per process. The *sql.DB
// is shared with the event store — same SQLite file, separate
// transaction per Save call. Safe for concurrent use because every
// statement is a single UPSERT.
type TokenUsageProjection struct {
	DB *sql.DB
}

// OnStarted creates the row on first observation so subsequent
// rollups can update unconditionally. We capture model + user_id
// here (the only event carrying both) and zero the token counters
// — they roll up via the message-appended events.
func (p *TokenUsageProjection) OnStarted(ctx context.Context, env es.Envelope, e *conversationv1.Started) error {
	_, err := p.DB.ExecContext(ctx, `
		INSERT INTO token_usage (
			tenant_id, conversation_id, model, user_id,
			tokens_input, tokens_output, turns, last_event_at
		) VALUES (?, ?, ?, ?, 0, 0, 0, ?)
		ON CONFLICT(tenant_id, conversation_id) DO UPDATE SET
			model = excluded.model,
			user_id = excluded.user_id,
			last_event_at = excluded.last_event_at
	`,
		env.TenantID, e.ConversationId, e.Model, e.UserId,
		env.RecordedAt.UTC().Format("2006-01-02T15:04:05.999Z"),
	)
	if err != nil {
		return fmt.Errorf("token_usage: OnStarted upsert: %w", err)
	}
	return nil
}

// OnUserMessageAppended adds the message's token estimate to
// tokens_input and bumps the turn counter.
func (p *TokenUsageProjection) OnUserMessageAppended(ctx context.Context, env es.Envelope, e *conversationv1.UserMessageAppended) error {
	return p.bump(ctx, env, e.ConversationId, e.Tokens, 0)
}

// OnAssistantMessageAppended adds the reply's token estimate to
// tokens_output and bumps the turn counter.
func (p *TokenUsageProjection) OnAssistantMessageAppended(ctx context.Context, env es.Envelope, e *conversationv1.AssistantMessageAppended) error {
	return p.bump(ctx, env, e.ConversationId, 0, e.Tokens)
}

// OnClosed is a no-op for token usage — closing the conversation
// doesn't change the token totals, only the lifecycle. Still
// implemented because the codegen-emitted Projection interface is
// exhaustive: a missing method would fail the build, which is
// exactly the safety property ADR 0020 decision 3a wants. Updating
// last_event_at on close keeps the projection's "freshness" cursor
// honest.
func (p *TokenUsageProjection) OnClosed(ctx context.Context, env es.Envelope, _ *conversationv1.Closed) error {
	_, err := p.DB.ExecContext(ctx, `
		UPDATE token_usage
		   SET last_event_at = ?
		 WHERE tenant_id = ?
		   AND conversation_id = ?
	`,
		env.RecordedAt.UTC().Format("2006-01-02T15:04:05.999Z"),
		env.TenantID, env.StreamID.ID,
	)
	if err != nil {
		return fmt.Errorf("token_usage: OnClosed update: %w", err)
	}
	return nil
}

// bump applies a delta to one of the token columns and increments
// the turn counter. The ON CONFLICT clause handles out-of-order
// replays where OnUserMessageAppended fires before OnStarted has
// landed (unlikely but possible if the event store is read at the
// exact moment OnStarted is being persisted in a parallel write
// path; defensive).
func (p *TokenUsageProjection) bump(ctx context.Context, env es.Envelope, conversationID string, dIn, dOut int64) error {
	_, err := p.DB.ExecContext(ctx, `
		INSERT INTO token_usage (
			tenant_id, conversation_id, model, user_id,
			tokens_input, tokens_output, turns, last_event_at
		) VALUES (?, ?, ?, ?, ?, ?, 1, ?)
		ON CONFLICT(tenant_id, conversation_id) DO UPDATE SET
			tokens_input  = tokens_input  + ?,
			tokens_output = tokens_output + ?,
			turns         = turns + 1,
			last_event_at = excluded.last_event_at
	`,
		env.TenantID, conversationID, "", "", dIn, dOut,
		env.RecordedAt.UTC().Format("2006-01-02T15:04:05.999Z"),
		dIn, dOut,
	)
	if err != nil {
		return fmt.Errorf("token_usage: bump: %w", err)
	}
	return nil
}

// Compile-time assertion: TokenUsageProjection satisfies the
// codegen-emitted exhaustive interface. If a new event variant lands
// in the proto, this line fails the build until OnXxx is implemented
// — the safety property ADR 0020 decision 3a promises.
var _ conversationv1.Projection = (*TokenUsageProjection)(nil)

// TokenUsageReader is the query-side surface. Adopters writing
// dashboards / billing exports against the read model use this rather
// than reaching into the projection itself.
type TokenUsageReader struct {
	DB *sql.DB
}

// Get returns the token-usage row for one conversation. Returns
// (zero, sql.ErrNoRows) if the projection hasn't observed any events
// for it yet.
func (r *TokenUsageReader) Get(ctx context.Context, tenantID, conversationID string) (TokenUsageRow, error) {
	row := TokenUsageRow{}
	err := r.DB.QueryRowContext(ctx, `
		SELECT tenant_id, conversation_id, model, user_id,
		       tokens_input, tokens_output, turns, last_event_at
		  FROM token_usage
		 WHERE tenant_id = ?
		   AND conversation_id = ?
	`, tenantID, conversationID).Scan(
		&row.TenantID, &row.ConversationID, &row.Model, &row.UserID,
		&row.TokensInput, &row.TokensOutput, &row.Turns, &row.LastEventAt,
	)
	return row, err
}

// ListByTenant returns all rows for a tenant ordered by last activity
// descending — the natural shape for "recent conversations" dashboards.
func (r *TokenUsageReader) ListByTenant(ctx context.Context, tenantID string, limit int) ([]TokenUsageRow, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := r.DB.QueryContext(ctx, `
		SELECT tenant_id, conversation_id, model, user_id,
		       tokens_input, tokens_output, turns, last_event_at
		  FROM token_usage
		 WHERE tenant_id = ?
		 ORDER BY last_event_at DESC
		 LIMIT ?
	`, tenantID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []TokenUsageRow
	for rows.Next() {
		var row TokenUsageRow
		if err := rows.Scan(
			&row.TenantID, &row.ConversationID, &row.Model, &row.UserID,
			&row.TokensInput, &row.TokensOutput, &row.Turns, &row.LastEventAt,
		); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}
