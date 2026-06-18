package conversations

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/firebase/genkit/go/ai"
	"github.com/firebase/genkit/go/genkit"
	"github.com/firebase/genkit/go/plugins/ollama"

	conversationv1 "github.com/laenenai/eventstore/gen/myapp/conversation/v1"
)

// DefaultOllamaURL is the local-development Ollama endpoint.
const DefaultOllamaURL = "http://127.0.0.1:11434"

// LLM is the narrow surface the conversation driver needs. Production
// adopters wire their preferred model provider through Genkit's plugin
// surface (Anthropic, Vertex, OpenAI, Bedrock, …) — the example uses
// the Ollama plugin for a zero-cost local loop. Tests inject a stub
// so the Decider matrix is exercised without a running model server.
type LLM interface {
	// Chat sends the full message history (incl. system prompt) and
	// returns the assistant's reply text plus a token-count estimate.
	// Honors ctx cancellation; deadline errors propagate verbatim.
	//
	// Options carry orthogonal knobs (streaming callback today;
	// temperature / max-tokens / tool registries in follow-up commits).
	// The same content is returned in ChatResponse even when a
	// streaming callback is attached — chunks deliver partial text live
	// for UX, but the final persisted event uses the assembled
	// ChatResponse.Content so replay is deterministic.
	Chat(ctx context.Context, model string, messages []ChatMessage, opts ...ChatOption) (ChatResponse, error)
}

// ChatOption configures one Chat call. Variadic options keep the LLM
// interface stable as the example grows (streaming today; sampling
// parameters, tool registries, JSON-mode in future).
type ChatOption func(*chatConfig)

// chatConfig is the internal aggregator the LLM implementation reads.
// Unexported so adopters can only set fields via the documented
// WithXxx constructors — keeps the contract surface small.
type chatConfig struct {
	OnChunk func(chunk string)
}

// WithStreamCallback attaches a token-delivery callback. The function
// is invoked once per chunk the model emits (typically a few tokens at
// a time on Ollama; provider-dependent). The callback runs on the
// goroutine driving Chat — fast, non-blocking work only. Slow work
// (network writes to a downstream client, file I/O, etc.) must hand
// off to another goroutine via a channel.
//
// Streaming is purely a UX concern. The aggregate persists ONE
// AssistantMessageAppended event with the assembled content after
// Chat returns; intermediate chunks are NOT events. This keeps the
// event log clean and replay-deterministic — the cookbook recipe 22
// pattern.
func WithStreamCallback(fn func(chunk string)) ChatOption {
	return func(c *chatConfig) { c.OnChunk = fn }
}

// StreamCallbackFromOptions resolves a stream callback out of an
// opaque ChatOption slice. Useful for adopters writing their own LLM
// implementations or test stubs that need to honor a caller's
// streaming preference without depending on the unexported chatConfig
// shape. Returns nil when no callback was attached.
func StreamCallbackFromOptions(opts []ChatOption) func(chunk string) {
	cfg := chatConfig{}
	for _, opt := range opts {
		opt(&cfg)
	}
	return cfg.OnChunk
}

// ChatMessage matches the chat wire shape: role + content. Role is
// one of "system", "user", "assistant" — translated to Genkit's
// internal Role enum inside the GenkitOllama client.
type ChatMessage struct {
	Role    string
	Content string
}

// ChatResponse is the trimmed-down result the driver consumes. Token
// counts may be zero on models that don't emit usage; the CLI falls
// back to a character-length estimator in that case.
type ChatResponse struct {
	Content      string
	TokensInput  int64
	TokensOutput int64
}

// MessagesFromConversation flattens the aggregate state into the wire
// shape: optional system prompt + every turn in order. Pure function
// so it's trivial to unit-test against fixture states.
func MessagesFromConversation(s *conversationv1.Conversation) []ChatMessage {
	out := make([]ChatMessage, 0, len(s.Turns)+1)
	if s.SystemPrompt != "" {
		out = append(out, ChatMessage{Role: "system", Content: s.SystemPrompt})
	}
	for _, turn := range s.Turns {
		role := "user"
		if turn.Role == conversationv1.Turn_ROLE_ASSISTANT {
			role = "assistant"
		}
		out = append(out, ChatMessage{Role: role, Content: turn.Content})
	}
	return out
}

// GenkitOllama drives Genkit's Ollama plugin. One instance per process
// — Genkit's plugin registry is stateful at the *genkit.Genkit value,
// so reusing one instance amortizes plugin init and model registration
// across calls. Safe for concurrent use; the model cache is mutex-
// guarded.
//
// Why Genkit instead of a hand-rolled HTTP client: Genkit is the
// canonical Go surface for multi-provider GenAI work. Swapping Ollama
// for Anthropic / Vertex / OpenAI is one import + one model definition
// in production deployments — not a rewrite. The conversation
// aggregate stays provider-agnostic via the LLM interface above.
type GenkitOllama struct {
	g      *genkit.Genkit
	plugin *ollama.Ollama

	// Models are registered lazily on first use by name. A real
	// deployment would pre-register the known model set during startup;
	// the example uses the lazy path so the CLI can take --model as a
	// runtime flag.
	mu     sync.Mutex
	models map[string]ai.Model
}

// NewGenkitOllama constructs a client pointed at baseURL (defaulting
// to DefaultOllamaURL). The returned *GenkitOllama is ready to handle
// Chat calls; models register on first reference.
func NewGenkitOllama(ctx context.Context, baseURL string) (*GenkitOllama, error) {
	if baseURL == "" {
		baseURL = DefaultOllamaURL
	}
	plugin := &ollama.Ollama{
		ServerAddress: baseURL,
		Timeout:       60,
	}
	g := genkit.Init(ctx, genkit.WithPlugins(plugin))
	return &GenkitOllama{
		g:      g,
		plugin: plugin,
		models: make(map[string]ai.Model),
	}, nil
}

// modelFor returns a registered model handle, defining one against the
// Genkit plugin the first time a name is seen. The chat-type and
// system-role/multiturn capabilities match the default expectations
// for an Ollama chat model; tool calls and media are deferred until
// the tool-call commit (see the README's follow-up list).
func (l *GenkitOllama) modelFor(name string) ai.Model {
	l.mu.Lock()
	defer l.mu.Unlock()
	if m, ok := l.models[name]; ok {
		return m
	}
	m := l.plugin.DefineModel(
		l.g,
		ollama.ModelDefinition{Name: name, Type: "chat"},
		&ai.ModelOptions{
			Supports: &ai.ModelSupports{
				Multiturn:  true,
				SystemRole: true,
				Tools:      false,
				Media:      false,
			},
		},
	)
	l.models[name] = m
	return m
}

// Chat implements LLM via genkit.Generate. The message history is
// translated from our flat ChatMessage shape into Genkit's typed
// constructors (NewSystemTextMessage / NewUserTextMessage /
// NewModelTextMessage). Errors flow through verbatim; the runtime's
// deadline drives both Genkit's own retries and the Ollama HTTP
// timeout.
//
// Streaming: if a callback is attached via WithStreamCallback, each
// model chunk's Text() is delivered to the callback as it arrives.
// genkit.Generate still returns a *ModelResponse carrying the
// assembled full text, so the assignment to ChatResponse.Content is
// unchanged — the caller persists ONE assistant-message event with
// the full content regardless of how the UX rendered intermediate
// chunks.
func (l *GenkitOllama) Chat(ctx context.Context, modelName string, messages []ChatMessage, opts ...ChatOption) (ChatResponse, error) {
	if modelName == "" {
		return ChatResponse{}, fmt.Errorf("ollama: model name is required")
	}
	cfg := chatConfig{}
	for _, opt := range opts {
		opt(&cfg)
	}
	model := l.modelFor(modelName)

	aiMsgs := make([]*ai.Message, 0, len(messages))
	for _, m := range messages {
		switch m.Role {
		case "system":
			aiMsgs = append(aiMsgs, ai.NewSystemTextMessage(m.Content))
		case "user":
			aiMsgs = append(aiMsgs, ai.NewUserTextMessage(m.Content))
		case "assistant", "model":
			aiMsgs = append(aiMsgs, ai.NewModelTextMessage(m.Content))
		default:
			return ChatResponse{}, fmt.Errorf("ollama: unknown role %q", m.Role)
		}
	}

	genOpts := []ai.GenerateOption{
		ai.WithModel(model),
		ai.WithMessages(aiMsgs...),
	}

	// Always assemble chunks ourselves when streaming is configured —
	// Genkit's Ollama plugin (as of v1.9.0) delivers the full reply
	// through stream chunks AND leaves *ModelResponse.Text() empty on
	// the terminal frame. Without our own assembly, the conversation
	// driver would persist an empty AssistantMessageAppended and the
	// Decider would (correctly) reject it as ErrEmptyMessage. Building
	// the buffer in-band gives us a reliable fallback regardless of
	// what the underlying plugin chooses to set on the response.
	var assembled strings.Builder
	if cfg.OnChunk != nil {
		genOpts = append(genOpts, ai.WithStreaming(func(_ context.Context, chunk *ai.ModelResponseChunk) error {
			if t := chunk.Text(); t != "" {
				assembled.WriteString(t)
				cfg.OnChunk(t)
			}
			return nil
		}))
	}

	resp, err := genkit.Generate(ctx, l.g, genOpts...)
	if err != nil {
		return ChatResponse{}, fmt.Errorf("ollama: generate: %w", err)
	}

	// Prefer the final ModelResponse.Text(); fall back to the
	// assembled stream buffer when it's empty. This makes the
	// contract — ChatResponse.Content always carries the assembled
	// reply — robust against plugin behaviour drift.
	content := resp.Text()
	if content == "" {
		content = assembled.String()
	}
	if content == "" {
		return ChatResponse{}, fmt.Errorf("ollama: generate returned empty content (model %q produced no text)", modelName)
	}

	out := ChatResponse{Content: content}
	// Genkit exposes usage on *ModelResponse when the underlying model
	// reports it. Older Ollama models don't always return prompt /
	// completion counts; the CLI falls back to an estimator in that
	// case, so a zero here is acceptable rather than fatal.
	if u := resp.Usage; u != nil {
		out.TokensInput = int64(u.InputTokens)
		out.TokensOutput = int64(u.OutputTokens)
	}
	return out, nil
}
