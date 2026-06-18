package conversations_test

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	_ "modernc.org/sqlite"

	"github.com/laenenai/eventstore/adapters/kms/inproc"
	sqliteadapter "github.com/laenenai/eventstore/adapters/storage/sqlite"
	"github.com/laenenai/eventstore/aggregate"
	"github.com/laenenai/eventstore/es"
	"github.com/laenenai/eventstore/examples/conversations"
	conversationv1 "github.com/laenenai/eventstore/gen/myapp/conversation/v1"
	"github.com/laenenai/eventstore/shred"
)

// newRuntime builds the same wiring the CLI uses: in-memory SQLite +
// in-process KMS + Shredder + aggregate runtime. Returns the store
// (for raw-payload assertions), shredder (for ForgetSubject), and
// the typed runtime.
func newRuntime(t *testing.T) (
	es.Store, *shred.Shredder,
	*aggregate.Runtime[*conversationv1.Conversation, conversationv1.Command, conversationv1.Event],
) {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	a := sqliteadapter.New(db)
	if err := a.Migrate(context.Background()); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	s := shred.New(inproc.New(), a)
	rt := &aggregate.Runtime[*conversationv1.Conversation, conversationv1.Command, conversationv1.Event]{
		Store:    a,
		Decider:  conversations.Decider,
		Codec:    conversationv1.EventCodec{},
		Shredder: s,
	}
	return a, s, rt
}

func mustStream(t *testing.T, tenant, conversationID string) es.StreamID {
	t.Helper()
	sid, err := es.NewStreamID(tenant, "conversation", conversationID)
	if err != nil {
		t.Fatalf("NewStreamID: %v", err)
	}
	return sid
}

// stubLLM lets the lifecycle test exercise the full Decider matrix
// without an Ollama server. Reply is a deterministic transform of
// the last user message so the assertion can check the persisted
// content matches what the "model" said.
//
// The stub also honors WithStreamCallback: if one is attached, the
// reply is split into 3 chunks and delivered with a short
// inter-chunk delay so the streaming test can observe the callback
// fire before the full Chat returns. The final ChatResponse.Content
// is the assembled full reply — same contract as the real client.
type stubLLM struct {
	replies []string
	// chunkSize, when > 0, splits each reply into chunks of this many
	// bytes for the streaming callback. Zero defaults to "one chunk
	// per word" (split on space).
	chunkSize int
}

func (s *stubLLM) Chat(_ context.Context, _ string, messages []conversations.ChatMessage, opts ...conversations.ChatOption) (conversations.ChatResponse, error) {
	if len(messages) == 0 {
		return conversations.ChatResponse{}, errors.New("no messages")
	}
	last := messages[len(messages)-1]
	reply := "echo: " + last.Content
	if len(s.replies) > 0 {
		reply = s.replies[0]
		s.replies = s.replies[1:]
	}

	// Honor streaming callback if attached.
	cfg := struct {
		onChunk func(string)
	}{}
	for _, opt := range opts {
		// We can't read the unexported chatConfig from outside the
		// conversations package, so we use the documented constructor's
		// observable behavior: WithStreamCallback installs a function
		// that conversations.GenkitOllama invokes per chunk. For the
		// stub, we mimic by inspecting via a side channel — there
		// isn't one. Instead: install our own callback collector by
		// invoking the option against a tiny shim that captures it.
		_ = opt
	}
	// Streaming callback extraction: since chatConfig is unexported,
	// the stub uses a small helper exported by the package for tests.
	if cb := conversations.StreamCallbackFromOptions(opts); cb != nil {
		cfg.onChunk = cb
	}
	if cfg.onChunk != nil {
		chunks := splitForStream(reply, s.chunkSize)
		for _, c := range chunks {
			cfg.onChunk(c)
		}
	}

	return conversations.ChatResponse{
		Content:      reply,
		TokensInput:  int64(len(last.Content) / 4),
		TokensOutput: int64(len(reply) / 4),
	}, nil
}

// splitForStream chops s into chunks roughly the requested size, or
// per-word when chunkSize <= 0. Pure helper used only by tests.
func splitForStream(s string, chunkSize int) []string {
	if chunkSize > 0 {
		var out []string
		for len(s) > chunkSize {
			out = append(out, s[:chunkSize])
			s = s[chunkSize:]
		}
		if s != "" {
			out = append(out, s)
		}
		return out
	}
	return strings.SplitAfter(s, " ")
}

// Helpers — the exposed driver loop is in cmd/chat/main.go and
// tangles I/O with the aggregate; these helpers replicate the
// orchestration without stdin so tests can drive turns directly.

func startConversation(
	t *testing.T,
	rt *aggregate.Runtime[*conversationv1.Conversation, conversationv1.Command, conversationv1.Event],
	tenant, userID, conversationID string,
) es.StreamID {
	t.Helper()
	ctx := es.WithTenant(context.Background(), tenant)
	sid := mustStream(t, tenant, conversationID)
	if _, err := rt.Handle(ctx, sid, &conversationv1.Start{
		TenantId:       tenant,
		ConversationId: conversationID,
		UserId:         userID,
		Model:          "stub",
		SystemPrompt:   "test system",
	}); err != nil {
		t.Fatalf("Start: %v", err)
	}
	return sid
}

func userTurn(
	t *testing.T,
	rt *aggregate.Runtime[*conversationv1.Conversation, conversationv1.Command, conversationv1.Event],
	tenant, userID string, sid es.StreamID, content string,
) {
	t.Helper()
	ctx := es.WithTenant(context.Background(), tenant)
	if _, err := rt.Handle(ctx, sid, &conversationv1.AppendUserMessage{
		TenantId:       tenant,
		ConversationId: sid.ID,
		UserId:         userID,
		Content:        content,
		Tokens:         int64(len(content) / 4),
	}); err != nil {
		t.Fatalf("AppendUserMessage: %v", err)
	}
}

func assistantTurn(
	t *testing.T,
	rt *aggregate.Runtime[*conversationv1.Conversation, conversationv1.Command, conversationv1.Event],
	tenant, userID string, sid es.StreamID, content string,
) {
	t.Helper()
	ctx := es.WithTenant(context.Background(), tenant)
	if _, err := rt.Handle(ctx, sid, &conversationv1.AppendAssistantMessage{
		TenantId:       tenant,
		ConversationId: sid.ID,
		UserId:         userID,
		Content:        content,
		Tokens:         int64(len(content) / 4),
	}); err != nil {
		t.Fatalf("AppendAssistantMessage: %v", err)
	}
}

func TestConversation_Lifecycle(t *testing.T) {
	_, _, rt := newRuntime(t)
	tenant := "acme"
	userID := "alice"
	convID := "conv-1"

	sid := startConversation(t, rt, tenant, userID, convID)

	llm := &stubLLM{replies: []string{
		"Hi Alice, how can I help?",
		"42, of course.",
	}}

	// Two full turns: user -> assistant -> user -> assistant.
	for _, prompt := range []string{"hello", "what is the answer?"} {
		userTurn(t, rt, tenant, userID, sid, prompt)

		ctx := es.WithTenant(context.Background(), tenant)
		state, _, err := rt.Load(ctx, sid)
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		resp, err := llm.Chat(ctx, "stub", conversations.MessagesFromConversation(state))
		if err != nil {
			t.Fatalf("LLM: %v", err)
		}
		assistantTurn(t, rt, tenant, userID, sid, resp.Content)
	}

	// Close.
	ctx := es.WithTenant(context.Background(), tenant)
	if _, err := rt.Handle(ctx, sid, &conversationv1.Close{
		TenantId:       tenant,
		ConversationId: convID,
		UserId:         userID,
		Reason:         "user_ended",
	}); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// State must reflect: 4 turns, closed, plausible token totals.
	state, _, err := rt.Load(ctx, sid)
	if err != nil {
		t.Fatalf("final Load: %v", err)
	}
	if !state.GetClosed() {
		t.Fatalf("expected Closed=true, got false")
	}
	if got := len(state.GetTurns()); got != 4 {
		t.Fatalf("turn count: got %d want 4", got)
	}
	if state.GetTokensInput() == 0 || state.GetTokensOutput() == 0 {
		t.Errorf("token totals should be non-zero, got in=%d out=%d",
			state.GetTokensInput(), state.GetTokensOutput())
	}
}

func TestConversation_PIIEncryptedAtRest(t *testing.T) {
	store, _, rt := newRuntime(t)
	tenant := "acme"
	userID := "bob"
	convID := "conv-2"
	sid := startConversation(t, rt, tenant, userID, convID)

	const secret = "my-credit-card-is-4111-1111-1111-1111"
	userTurn(t, rt, tenant, userID, sid, secret)

	envs, err := store.ReadStream(context.Background(), sid, 0)
	if err != nil {
		t.Fatalf("ReadStream: %v", err)
	}
	if len(envs) != 2 {
		t.Fatalf("envelopes: got %d want 2", len(envs))
	}
	// The Started envelope carries no PERSONAL fields; the
	// UserMessageAppended one carries the content which MUST be
	// ciphertext, never plaintext, on disk.
	if !bytes.Contains(envs[0].Payload, []byte(tenant)) && !bytes.Contains(envs[0].Payload, []byte("test system")) {
		// nothing to assert about Started; just sanity-touch it.
	}
	if bytes.Contains(envs[1].Payload, []byte(secret)) {
		t.Errorf("UserMessageAppended payload leaks plaintext content (secret found in raw bytes)")
	}
}

func TestConversation_ForgetSubjectRedactsContent(t *testing.T) {
	_, shredder, rt := newRuntime(t)
	tenant := "acme"
	userID := "charlie"
	convID := "conv-3"
	sid := startConversation(t, rt, tenant, userID, convID)

	const userSecret = "this is a private user message"
	const reply = "this is a private assistant reply"
	userTurn(t, rt, tenant, userID, sid, userSecret)
	assistantTurn(t, rt, tenant, userID, sid, reply)

	// Right-to-erasure.
	if err := shredder.ForgetSubject(context.Background(), tenant, userID); err != nil {
		t.Fatalf("ForgetSubject: %v", err)
	}

	// After shred, Load reports the user's PERSONAL fields as
	// redacted. The framework calls OnRedacted with the missing
	// fields each time a shredded event flows through Decode.
	var redacted []shred.RedactedFields
	rt.OnRedacted = func(r shred.RedactedFields) {
		redacted = append(redacted, r)
	}

	ctx := es.WithTenant(context.Background(), tenant)
	state, _, err := rt.Load(ctx, sid)
	if err != nil {
		t.Fatalf("Load post-shred: %v", err)
	}
	if len(redacted) == 0 {
		t.Fatalf("expected at least one RedactedFields callback after ForgetSubject")
	}

	// Reconstructed state must not carry the original plaintext on
	// any of its message bodies — every Turn.Content for the shredded
	// subject must be zero-valued or empty.
	for i, turn := range state.GetTurns() {
		if strings.Contains(turn.Content, userSecret) {
			t.Errorf("turn[%d].Content leaks userSecret after ForgetSubject", i)
		}
		if strings.Contains(turn.Content, reply) {
			t.Errorf("turn[%d].Content leaks reply after ForgetSubject", i)
		}
	}
}

func TestConversation_AppendBeforeStartRejected(t *testing.T) {
	_, _, rt := newRuntime(t)
	tenant := "acme"
	sid := mustStream(t, tenant, "ghost")
	ctx := es.WithTenant(context.Background(), tenant)

	_, err := rt.Handle(ctx, sid, &conversationv1.AppendUserMessage{
		TenantId:       tenant,
		ConversationId: "ghost",
		UserId:         "nobody",
		Content:        "hello?",
		Tokens:         1,
	})
	if !errors.Is(err, conversations.ErrNotStarted) {
		t.Fatalf("got %v want ErrNotStarted", err)
	}
}

func TestConversation_AppendAfterCloseRejected(t *testing.T) {
	_, _, rt := newRuntime(t)
	tenant := "acme"
	userID := "dana"
	convID := "conv-4"
	sid := startConversation(t, rt, tenant, userID, convID)
	ctx := es.WithTenant(context.Background(), tenant)

	if _, err := rt.Handle(ctx, sid, &conversationv1.Close{
		TenantId:       tenant,
		ConversationId: convID,
		UserId:         userID,
		Reason:         "test",
	}); err != nil {
		t.Fatalf("Close: %v", err)
	}
	_, err := rt.Handle(ctx, sid, &conversationv1.AppendUserMessage{
		TenantId:       tenant,
		ConversationId: convID,
		UserId:         userID,
		Content:        "ping",
		Tokens:         1,
	})
	// IsTerminal is set on the Decider, so the framework short-circuits
	// with es.ErrTerminal before the Decider's own ErrConversationClosed
	// can fire. Both are correct rejections of an append after close;
	// the framework's terminal-stream guard is the one users see.
	if !errors.Is(err, es.ErrTerminal) {
		t.Fatalf("got %v want es.ErrTerminal", err)
	}
}

func TestConversation_UserMismatchRejected(t *testing.T) {
	_, _, rt := newRuntime(t)
	tenant := "acme"
	startConversation(t, rt, tenant, "owner", "conv-5")
	sid := mustStream(t, tenant, "conv-5")
	ctx := es.WithTenant(context.Background(), tenant)

	_, err := rt.Handle(ctx, sid, &conversationv1.AppendUserMessage{
		TenantId:       tenant,
		ConversationId: "conv-5",
		UserId:         "intruder",
		Content:        "hi",
		Tokens:         1,
	})
	if !errors.Is(err, conversations.ErrUserMismatch) {
		t.Fatalf("got %v want ErrUserMismatch", err)
	}
}

func TestConversation_EmptyMessageRejected(t *testing.T) {
	_, _, rt := newRuntime(t)
	tenant := "acme"
	userID := "eve"
	sid := startConversation(t, rt, tenant, userID, "conv-6")
	ctx := es.WithTenant(context.Background(), tenant)

	_, err := rt.Handle(ctx, sid, &conversationv1.AppendUserMessage{
		TenantId:       tenant,
		ConversationId: "conv-6",
		UserId:         userID,
		Content:        "   ",
		Tokens:         0,
	})
	if !errors.Is(err, conversations.ErrEmptyMessage) {
		t.Fatalf("got %v want ErrEmptyMessage", err)
	}
}

func TestConversation_StreamingDelivery(t *testing.T) {
	// Streaming asserts: chunks arrive in order; the assembled string
	// equals ChatResponse.Content; the persisted event carries the
	// SAME assembled content (one durable event, not one per chunk).
	_, _, rt := newRuntime(t)
	tenant := "acme"
	userID := "fran"
	convID := "conv-stream"
	sid := startConversation(t, rt, tenant, userID, convID)

	const reply = "The streaming reply is split into several chunks for UX."
	llm := &stubLLM{replies: []string{reply}}

	userTurn(t, rt, tenant, userID, sid, "stream please")

	ctx := es.WithTenant(context.Background(), tenant)
	state, _, err := rt.Load(ctx, sid)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	var receivedChunks []string
	resp, err := llm.Chat(ctx, "stub", conversations.MessagesFromConversation(state),
		conversations.WithStreamCallback(func(chunk string) {
			receivedChunks = append(receivedChunks, chunk)
		}),
	)
	if err != nil {
		t.Fatalf("Chat: %v", err)
	}

	// At least 2 chunks must have arrived for the assertion to be
	// meaningful (the stub splits on word boundaries; the reply has
	// 9 words).
	if len(receivedChunks) < 2 {
		t.Fatalf("expected >=2 chunks, got %d", len(receivedChunks))
	}
	if assembled := strings.Join(receivedChunks, ""); assembled != reply {
		t.Errorf("chunks did not assemble to reply\n got %q\nwant %q", assembled, reply)
	}
	if resp.Content != reply {
		t.Errorf("ChatResponse.Content: got %q want %q", resp.Content, reply)
	}

	// Persist the assistant turn with the assembled content — same
	// content the chunks delivered, exactly one event.
	assistantTurn(t, rt, tenant, userID, sid, resp.Content)

	final, _, err := rt.Load(ctx, sid)
	if err != nil {
		t.Fatalf("Load post-stream: %v", err)
	}
	turns := final.GetTurns()
	if len(turns) != 2 {
		t.Fatalf("turns: got %d want 2 (user + assistant)", len(turns))
	}
	if turns[1].Content != reply {
		t.Errorf("persisted assistant turn: got %q want %q", turns[1].Content, reply)
	}
}

func TestConversation_TenantIsolation(t *testing.T) {
	_, _, rt := newRuntime(t)
	startConversation(t, rt, "tenant-a", "alice", "shared-id")
	sid := mustStream(t, "tenant-b", "shared-id")
	ctxB := es.WithTenant(context.Background(), "tenant-b")

	// A Load from tenant-b for the same conversation id must NOT
	// see tenant-a's state.
	state, _, err := rt.Load(ctxB, sid)
	if err != nil {
		t.Fatalf("Load tenant-b: %v", err)
	}
	if state.GetConversationId() != "" {
		t.Fatalf("tenant-b saw tenant-a's conversation: %#v", state)
	}
}
