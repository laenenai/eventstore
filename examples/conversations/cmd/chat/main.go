// Command chat is the runnable end-to-end demo of the conversations
// example: a SQLite-backed Conversation aggregate driven by a local
// Ollama LLM. Multi-tenant, crypto-shredded, read-your-writes via the
// Tier-1 state_cache.
//
// Run:
//
//	# 1. Start Ollama and pull a small model
//	ollama serve &
//	ollama pull llama3.2
//
//	# 2. Run the chat
//	go run ./examples/conversations/cmd/chat \
//	    --tenant acme \
//	    --user alice \
//	    --model llama3.2 \
//	    --db ./chat.db
//
//	# 3. Talk. Type :quit to close cleanly. Re-run with the same
//	#    --user to resume the same conversation (uses state_cache,
//	#    not replay).
package main

import (
	"bufio"
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	_ "modernc.org/sqlite"

	"github.com/laenenai/eventstore/adapters/kms/inproc"
	sqliteadapter "github.com/laenenai/eventstore/adapters/storage/sqlite"
	"github.com/laenenai/eventstore/aggregate"
	"github.com/laenenai/eventstore/es"
	"github.com/laenenai/eventstore/examples/conversations"
	conversationv1 "github.com/laenenai/eventstore/gen/myapp/conversation/v1"
	"github.com/laenenai/eventstore/shred"
)

func main() {
	var (
		tenant       = flag.String("tenant", "demo", "Tenant id")
		userID       = flag.String("user", "", "User id (required; used as crypto-shred subject)")
		model        = flag.String("model", "llama3.2", "Ollama model to chat with (must be pulled locally)")
		dbPath       = flag.String("db", "./chat.db", "SQLite file (created if missing)")
		systemPrompt = flag.String("system", "You are a helpful assistant. Be concise.", "System prompt")
		ollamaURL    = flag.String("ollama", conversations.DefaultOllamaURL, "Ollama base URL")
		conversation = flag.String("conversation", "", "Resume an existing conversation id; empty starts a new one")
	)
	flag.Parse()
	if *userID == "" {
		fmt.Fprintln(os.Stderr, "ERROR: --user is required (the crypto-shred subject)")
		os.Exit(2)
	}

	ctx, cancel := signal.NotifyContext(context.Background(),
		os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := run(ctx, *tenant, *userID, *model, *dbPath, *systemPrompt, *ollamaURL, *conversation); err != nil {
		if errors.Is(err, context.Canceled) {
			fmt.Fprintln(os.Stderr, "\nbye.")
			return
		}
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run(
	ctx context.Context,
	tenantID, userID, model, dbPath, systemPrompt, ollamaURL, resumeID string,
) error {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return fmt.Errorf("open sqlite %s: %w", dbPath, err)
	}
	defer db.Close()

	adapter := sqliteadapter.New(db)
	if err := adapter.Migrate(ctx); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}

	shredder := shred.New(inproc.New(), adapter)

	rt := &aggregate.Runtime[*conversationv1.Conversation, conversationv1.Command, conversationv1.Event]{
		Store:    adapter,
		Decider:  conversations.Decider,
		Codec:    conversationv1.EventCodec{},
		Shredder: shredder,
	}

	conversationID := resumeID
	if conversationID == "" {
		conversationID = uuid.NewString()
	}

	tenantCtx := es.WithTenant(ctx, tenantID)
	sid, err := es.NewStreamID(tenantID, "conversation", conversationID)
	if err != nil {
		return fmt.Errorf("build stream id: %w", err)
	}

	// Start the conversation if it doesn't exist yet. Load first;
	// only Start when the load returns Initial-equivalent state.
	state, _, err := rt.Load(tenantCtx, sid)
	if err != nil {
		return fmt.Errorf("load conversation: %w", err)
	}
	if state.GetConversationId() == "" {
		fmt.Printf("starting new conversation %s (tenant=%s user=%s model=%s)\n",
			conversationID, tenantID, userID, model)
		if _, err := rt.Handle(tenantCtx, sid, &conversationv1.Start{
			TenantId:       tenantID,
			ConversationId: conversationID,
			UserId:         userID,
			Model:          model,
			SystemPrompt:   systemPrompt,
		}); err != nil {
			return fmt.Errorf("start conversation: %w", err)
		}
	} else {
		fmt.Printf("resumed conversation %s (%d prior turns)\n",
			conversationID, len(state.GetTurns()))
	}

	llm := conversations.NewOllama(ollamaURL)
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		fmt.Print("\n> ")
		if !scanner.Scan() {
			if err := scanner.Err(); err != nil {
				return fmt.Errorf("stdin: %w", err)
			}
			// EOF — graceful close.
			break
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if line == ":quit" || line == ":q" {
			break
		}

		// Append the user's turn. Token count is a rough estimate;
		// real adopters call a tokenizer (tiktoken-go, ollama's
		// /api/embeddings count endpoint, etc.) for accuracy.
		userTokens := approxTokens(line)
		if _, err := rt.Handle(tenantCtx, sid, &conversationv1.AppendUserMessage{
			TenantId:       tenantID,
			ConversationId: conversationID,
			UserId:         userID,
			Content:        line,
			Tokens:         userTokens,
		}); err != nil {
			fmt.Fprintf(os.Stderr, "append user message: %v\n", err)
			continue
		}

		// Read post-decide state for the LLM call — this is the
		// state_cache (Tier-1) hit; no replay.
		state, _, err := rt.Load(tenantCtx, sid)
		if err != nil {
			return fmt.Errorf("load state for LLM: %w", err)
		}
		messages := conversations.MessagesFromConversation(state)

		// LLM call with a per-request deadline so a stuck Ollama
		// server doesn't block forever.
		callCtx, cancel := context.WithTimeout(tenantCtx, 2*time.Minute)
		resp, err := llm.Chat(callCtx, model, messages)
		cancel()
		if err != nil {
			fmt.Fprintf(os.Stderr, "llm: %v\n", err)
			continue
		}

		// Persist the assistant's reply. Token counts come from
		// Ollama itself; fall back to the estimator if Ollama
		// reported 0 (older models occasionally do).
		outTokens := resp.TokensOutput
		if outTokens == 0 {
			outTokens = approxTokens(resp.Content)
		}
		if _, err := rt.Handle(tenantCtx, sid, &conversationv1.AppendAssistantMessage{
			TenantId:       tenantID,
			ConversationId: conversationID,
			UserId:         userID,
			Content:        resp.Content,
			Tokens:         outTokens,
		}); err != nil {
			fmt.Fprintf(os.Stderr, "append assistant message: %v\n", err)
			continue
		}

		fmt.Println()
		fmt.Println(resp.Content)
	}

	// Clean close.
	if _, err := rt.Handle(tenantCtx, sid, &conversationv1.Close{
		TenantId:       tenantID,
		ConversationId: conversationID,
		UserId:         userID,
		Reason:         "user_ended",
	}); err != nil {
		return fmt.Errorf("close: %w", err)
	}
	fmt.Printf("\nclosed conversation %s\n", conversationID)
	return nil
}

// approxTokens is the placeholder token estimator: ~4 chars per token,
// matching the rough OpenAI / Anthropic guidance for English. Adopters
// who need accurate accounting wire a real tokenizer; this is just so
// the budget check in the Decider has a non-zero number to work with.
func approxTokens(s string) int64 {
	if s == "" {
		return 0
	}
	t := int64(len(s) / 4)
	if t == 0 {
		return 1
	}
	return t
}
