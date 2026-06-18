package conversations

import (
	"context"
	"fmt"
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
	Chat(ctx context.Context, model string, messages []ChatMessage) (ChatResponse, error)
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
func (l *GenkitOllama) Chat(ctx context.Context, modelName string, messages []ChatMessage) (ChatResponse, error) {
	if modelName == "" {
		return ChatResponse{}, fmt.Errorf("ollama: model name is required")
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

	resp, err := genkit.Generate(ctx, l.g,
		ai.WithModel(model),
		ai.WithMessages(aiMsgs...),
	)
	if err != nil {
		return ChatResponse{}, fmt.Errorf("ollama: generate: %w", err)
	}

	out := ChatResponse{Content: resp.Text()}
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
