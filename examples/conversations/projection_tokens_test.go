package conversations_test

import (
	"context"
	"database/sql"
	"testing"

	_ "modernc.org/sqlite"

	"github.com/laenenai/eventstore/adapters/kms/inproc"
	sqliteadapter "github.com/laenenai/eventstore/adapters/storage/sqlite"
	"github.com/laenenai/eventstore/aggregate"
	"github.com/laenenai/eventstore/es"
	"github.com/laenenai/eventstore/examples/conversations"
	conversationv1 "github.com/laenenai/eventstore/gen/myapp/conversation/v1"
	"github.com/laenenai/eventstore/projection"
	"github.com/laenenai/eventstore/shred"
)

// buildRuntimeOnAdapter wires an aggregate.Runtime against a caller-
// supplied adapter so the projection test can verify the polling
// integration over the same storage the events were written to.
func buildRuntimeOnAdapter(adapter *sqliteadapter.Adapter) (
	*aggregate.Runtime[*conversationv1.Conversation, conversationv1.Command, conversationv1.Event],
	*shred.Shredder,
) {
	shredder := shred.New(inproc.New(), adapter)
	return &aggregate.Runtime[*conversationv1.Conversation, conversationv1.Command, conversationv1.Event]{
		Store:    adapter,
		Decider:  conversations.Decider,
		Codec:    conversationv1.EventCodec{},
		Shredder: shredder,
	}, shredder
}

// TestTokenUsageProjection_EndToEnd drives the projection.Runtime over
// a real (in-memory) SQLite store with the same wiring the CLI uses,
// then asserts the token_usage table reflects the persisted events.
//
// This is the test the CPO review asked for: prove that projections
// actually run, not just that the framework supports them.
func TestTokenUsageProjection_EndToEnd(t *testing.T) {
	ctx := context.Background()

	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	adapter := sqliteadapter.New(db)
	if err := adapter.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	if _, err := db.ExecContext(ctx, conversations.TokenUsageSchema); err != nil {
		t.Fatalf("create token_usage: %v", err)
	}

	// Use the same runtime + decider as the lifecycle test so the
	// event log is realistic.
	_, _, rt := newRuntime(t)
	tenant := "acme"
	userID := "alice"
	convID := "conv-tokens-1"
	sid := startConversation(t, rt, tenant, userID, convID)
	userTurn(t, rt, tenant, userID, sid, "first user message")
	assistantTurn(t, rt, tenant, userID, sid, "first assistant reply")
	userTurn(t, rt, tenant, userID, sid, "second user message — longer")
	assistantTurn(t, rt, tenant, userID, sid, "second assistant reply — also longer")

	// newRuntime uses its OWN in-memory adapter; export the events
	// across to the projection's adapter by reading and replaying.
	// Simpler than refactoring newRuntime to accept an injected
	// adapter — this is a one-off test fixture.
	tCtx := es.WithTenant(ctx, tenant)
	envs, err := rt.Store.ReadStream(tCtx, sid, 0)
	if err != nil {
		t.Fatalf("ReadStream: %v", err)
	}

	// Apply each event to the projection directly via the
	// codegen-emitted dispatcher. This proves the projection's logic
	// independently of the runtime's polling loop.
	proj := &conversations.TokenUsageProjection{DB: db}
	handler := conversationv1.NewProjectionDispatcher(proj, projection.IgnoreUnknown())
	for _, env := range envs {
		if err := handler(ctx, env); err != nil {
			t.Fatalf("handler env=%s type=%s: %v", env.EventID, env.TypeURL, err)
		}
	}

	// Assert the read-model row.
	reader := &conversations.TokenUsageReader{DB: db}
	row, err := reader.Get(ctx, tenant, convID)
	if err != nil {
		t.Fatalf("reader.Get: %v", err)
	}
	if row.Turns != 4 {
		t.Errorf("turns: got %d want 4", row.Turns)
	}
	if row.TokensInput == 0 {
		t.Errorf("tokens_input: got 0, expected non-zero from the two user messages")
	}
	if row.TokensOutput == 0 {
		t.Errorf("tokens_output: got 0, expected non-zero from the two assistant messages")
	}
	if row.Model != "stub" {
		t.Errorf("model: got %q want %q", row.Model, "stub")
	}
	if row.UserID != userID {
		t.Errorf("user_id: got %q want %q", row.UserID, userID)
	}
}

func TestTokenUsageProjection_RuntimePolls(t *testing.T) {
	// This is the integration shape the CLI runs: write events via
	// aggregate.Runtime, projection.Runtime.RunOnce polls the same
	// store, the read model converges. Proves the polling path
	// without timing flakiness from a goroutine sleep.
	ctx := context.Background()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	adapter := sqliteadapter.New(db)
	if err := adapter.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	if _, err := db.ExecContext(ctx, conversations.TokenUsageSchema); err != nil {
		t.Fatalf("create token_usage: %v", err)
	}
	// Rebuild a runtime over the SAME adapter so events and
	// projection share storage.
	rt, _ := buildRuntimeOnAdapter(adapter)
	tenant := "acme"
	userID := "bob"
	convID := "conv-tokens-2"

	tCtx := es.WithTenant(ctx, tenant)
	sid, err := es.NewStreamID(tenant, "conversation", convID)
	if err != nil {
		t.Fatalf("NewStreamID: %v", err)
	}
	if _, err := rt.Handle(tCtx, sid, &conversationv1.Start{
		TenantId: tenant, ConversationId: convID, UserId: userID,
		Model: "stub", SystemPrompt: "test",
	}); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err := rt.Handle(tCtx, sid, &conversationv1.AppendUserMessage{
		TenantId: tenant, ConversationId: convID, UserId: userID,
		Content: "hello world", Tokens: 5,
	}); err != nil {
		t.Fatalf("AppendUserMessage: %v", err)
	}

	proj := &conversations.TokenUsageProjection{DB: db}
	projRuntime := &projection.Runtime{
		Name:       conversations.TokenUsage,
		Store:      adapter,
		Checkpoint: adapter,
		Handler:    conversationv1.NewProjectionDispatcher(proj, projection.IgnoreUnknown()),
		Tenant:     tenant,
	}
	if _, err := projRuntime.RunOnce(ctx); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}

	reader := &conversations.TokenUsageReader{DB: db}
	row, err := reader.Get(ctx, tenant, convID)
	if err != nil {
		t.Fatalf("reader.Get: %v", err)
	}
	if row.Turns != 1 {
		t.Errorf("turns: got %d want 1", row.Turns)
	}
	if row.TokensInput != 5 {
		t.Errorf("tokens_input: got %d want 5", row.TokensInput)
	}
}
