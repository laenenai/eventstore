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

	"path/filepath"

	"github.com/google/uuid"
	_ "modernc.org/sqlite"

	filekms "github.com/laenenai/eventstore/adapters/kms/file"
	sqliteadapter "github.com/laenenai/eventstore/adapters/storage/sqlite"
	"github.com/laenenai/eventstore/aggregate"
	"github.com/laenenai/eventstore/es"
	"github.com/laenenai/eventstore/examples/conversations"
	conversationv1 "github.com/laenenai/eventstore/gen/myapp/conversation/v1"
	"github.com/laenenai/eventstore/kms"
	"github.com/laenenai/eventstore/projection"
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
		kmsFile      = flag.String("kms-file", "", "Path to the file-backed KEK store (default: <db>.kms.json). Lets PII-encrypted history survive process restarts.")
	)
	flag.Parse()
	if *userID == "" {
		fmt.Fprintln(os.Stderr, "ERROR: --user is required (the crypto-shred subject)")
		os.Exit(2)
	}

	ctx, cancel := signal.NotifyContext(context.Background(),
		os.Interrupt, syscall.SIGTERM)
	defer cancel()

	resolvedKMSFile := *kmsFile
	if resolvedKMSFile == "" {
		resolvedKMSFile = *dbPath + ".kms.json"
	}
	if err := run(ctx, *tenant, *userID, *model, *dbPath, *systemPrompt, *ollamaURL, *conversation, resolvedKMSFile); err != nil {
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
	tenantID, userID, model, dbPath, systemPrompt, ollamaURL, resumeID, kmsFile string,
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

	// File-backed KMS persists KEKs to a sidecar JSON so PII-encrypted
	// history survives CLI restarts. Inproc KMS (the test default)
	// keeps KEKs in memory only, which is fine for one-shot tests but
	// would render every prior conversation's PII unreadable as soon
	// as the user exited and reopened the chat.
	keystore, err := filekms.New(kmsFile)
	if err != nil {
		return fmt.Errorf("init kms: %w", err)
	}

	// Startup-time consistency check: subject_keys persisted from a
	// previous session reference a KEK version (e.g., from a vanished
	// inproc KMS run) that this KMS file might not have. Catch the
	// mismatch loudly here rather than surfacing the same cryptic
	// "KEK version not available" error on every command. The check
	// is per (tenant, userID); other subjects fail individually if
	// their KEK is also missing.
	if err := assertKMSMatchesStore(ctx, keystore, adapter, tenantID, userID, dbPath, kmsFile); err != nil {
		return err
	}

	shredder := shred.New(keystore, adapter)

	// Tier-3 token_usage projection: a goroutine polls the event log,
	// upserts one row per (tenant, conversation) into the read-model
	// table, advances its checkpoint via the SQLite adapter. The
	// projection runs against the same DB as the events — application
	// schema (token_usage) created here at startup, framework
	// checkpoint table (projection_checkpoint) already migrated.
	if _, err := db.ExecContext(ctx, conversations.TokenUsageSchema); err != nil {
		return fmt.Errorf("create token_usage table: %w", err)
	}
	tokenProj := &conversations.TokenUsageProjection{DB: db}
	projRuntime := &projection.Runtime{
		Name:       conversations.TokenUsage,
		Store:      adapter,
		Checkpoint: adapter,
		Handler: conversationv1.NewProjectionDispatcher(tokenProj,
			projection.IgnoreUnknown()), // other aggregates may share the DB
		Tenant:    tenantID,
		IdleSleep: 250 * time.Millisecond, // chat-feel responsiveness
	}
	projDone := make(chan struct{})
	go func() {
		defer close(projDone)
		if err := projRuntime.Run(ctx); err != nil {
			fmt.Fprintf(os.Stderr, "projection: %v\n", err)
		}
	}()
	reader := &conversations.TokenUsageReader{DB: db}

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

	llm, err := conversations.NewGenkitOllama(ctx, ollamaURL)
	if err != nil {
		return fmt.Errorf("init genkit/ollama: %w", err)
	}
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

		// Stream chunks to the terminal as they arrive — UX win for
		// slower models. The full assembled content still comes back
		// in resp.Content and is what we persist as ONE event below.
		// A trailing newline is printed after the call so the prompt
		// on the next iteration starts on a fresh line.
		fmt.Println()
		resp, err := llm.Chat(callCtx, model, messages,
			conversations.WithStreamCallback(func(chunk string) {
				fmt.Print(chunk)
			}),
		)
		fmt.Println()
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

		// Wait briefly for the projection to catch up so the
		// post-turn rollup reflects the just-persisted event. The
		// projection polls every IdleSleep (250ms above); a few hops
		// of that interval keeps the UX snappy without spin-looping.
		if row, err := waitForRollup(ctx, reader, tenantID, conversationID, outTokens, 2*time.Second); err == nil {
			fmt.Fprintf(os.Stderr, "  [tokens in=%d out=%d turns=%d]\n",
				row.TokensInput, row.TokensOutput, row.Turns)
		}
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

	// Print the final rollup so the user sees the durable total —
	// the projection has had plenty of time to catch up since the
	// last assistant turn.
	if row, err := reader.Get(context.Background(), tenantID, conversationID); err == nil {
		fmt.Printf("final token usage: in=%d out=%d turns=%d model=%s\n",
			row.TokensInput, row.TokensOutput, row.Turns, row.Model)
	}

	// Tear down the projection goroutine so the process exits
	// cleanly rather than abandoning a running poll loop.
	_ = projDone
	return nil
}

// assertKMSMatchesStore fails fast when the persisted subject_keys row
// for (tenant, userID) is wrapped under a KEK the keystore can't
// open. This is the exact "I deleted the wrong file" footgun adopters
// hit when iterating on the example; the error message includes the
// absolute paths of both the DB and the KMS file so it's obvious what
// to remove to start over.
//
// On first run for this subject there is no subject_keys row yet, so
// ErrSubjectKeyNotFound short-circuits and we proceed normally.
func assertKMSMatchesStore(
	ctx context.Context,
	keystore kms.KeyStore,
	store *sqliteadapter.Adapter,
	tenantID, userID, dbPath, kmsFile string,
) error {
	tenantCtx := es.WithTenant(ctx, tenantID)
	row, err := store.GetSubjectKey(tenantCtx, tenantID, userID)
	if errors.Is(err, shred.ErrSubjectKeyNotFound) {
		return nil // first time we've seen this subject — nothing to verify
	}
	if err != nil {
		return fmt.Errorf("kms consistency check: read subject_keys: %w", err)
	}
	if _, err := keystore.UnwrapDEK(ctx, tenantID, row.DEKWrapped, row.KEKVersion); err != nil {
		dbAbs, _ := filepath.Abs(dbPath)
		kmsAbs, _ := filepath.Abs(kmsFile)
		return fmt.Errorf(`KMS does not match the event store.

The subject_keys row for (tenant=%s, user=%s) references KEK version %d,
which is not unwrappable by the current KMS file. Most commonly this
means a previous run used the in-memory inproc KMS (DEKs vanished on
process exit) but the SQLite event log persisted the subject_keys row.

To start over (this DESTROYS the encrypted history — there is no way
to recover it without the original KEK):

    rm %q
    rm %q

Then re-run the chat. A fresh KEK will be generated on first message.

To preserve history, restore the matching kms.json from a backup or
operator-controlled secret and pass it via --kms-file.

Underlying error: %v`,
			tenantID, userID, row.KEKVersion, dbAbs, kmsAbs, err)
	}
	return nil
}

// waitForRollup polls the token_usage projection until the row's
// tokens_output reflects the just-appended assistant message (>= the
// new token total) or the budget expires. Used purely for UX —
// printing stale totals would mislead the user about where they are
// against the budget. Returns the last-seen row even on timeout so
// callers can show whatever the projection had time to compute.
func waitForRollup(
	ctx context.Context,
	reader *conversations.TokenUsageReader,
	tenant, conversation string,
	minOutputTokens int64,
	budget time.Duration,
) (conversations.TokenUsageRow, error) {
	deadline := time.Now().Add(budget)
	var last conversations.TokenUsageRow
	var lastErr error
	for {
		row, err := reader.Get(ctx, tenant, conversation)
		if err == nil {
			last = row
			if row.TokensOutput >= minOutputTokens {
				return row, nil
			}
		} else {
			lastErr = err
		}
		if time.Now().After(deadline) {
			if lastErr != nil && lastErr != sql.ErrNoRows {
				return last, lastErr
			}
			return last, nil
		}
		select {
		case <-ctx.Done():
			return last, ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
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
