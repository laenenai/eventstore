package conversations

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	conversationv1 "github.com/laenenai/eventstore/gen/myapp/conversation/v1"
)

// DefaultOllamaURL is the local-development Ollama endpoint.
const DefaultOllamaURL = "http://localhost:11434"

// LLM is the narrow surface the conversation driver needs. Production
// adopters wire Anthropic, OpenAI, Bedrock, etc. via their respective
// SDKs; the example uses Ollama for a zero-cost local loop. Tests
// inject a stub so the Decider is exercised without a real LLM.
type LLM interface {
	// Chat sends the full message history (incl. system prompt) and
	// returns the assistant's reply text plus a token-count estimate.
	// Honors ctx cancellation; deadline errors propagate verbatim.
	Chat(ctx context.Context, model string, messages []ChatMessage) (ChatResponse, error)
}

// ChatMessage matches Ollama's /api/chat shape: role + content.
// Role is one of "system", "user", "assistant".
type ChatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// ChatResponse is the trimmed-down result the driver consumes.
type ChatResponse struct {
	Content      string
	TokensInput  int64 // prompt-eval count from Ollama's response
	TokensOutput int64 // eval count from Ollama's response
}

// MessagesFromConversation flattens the aggregate state into the
// Ollama wire shape: optional system prompt + every turn in order.
// Stays a pure function so it's trivial to unit-test against fixture
// states.
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

// Ollama is the HTTP client. BaseURL defaults to DefaultOllamaURL; the
// http.Client is reused so connections pool. Safe for concurrent use.
type Ollama struct {
	BaseURL string
	HTTP    *http.Client
}

// NewOllama returns a client with a sensible default timeout. The
// 5-minute deadline accommodates first-token latency on cold-start
// models; production drivers should set their own per-request
// deadlines via context.
func NewOllama(baseURL string) *Ollama {
	if baseURL == "" {
		baseURL = DefaultOllamaURL
	}
	return &Ollama{
		BaseURL: baseURL,
		HTTP:    &http.Client{Timeout: 5 * time.Minute},
	}
}

// ollamaChatRequest is the wire payload we send to /api/chat.
// `stream: false` collapses Ollama's NDJSON streaming response into
// one final JSON object — simpler for the MVP; streaming lands in a
// follow-up commit alongside the Connect HTTP edge.
type ollamaChatRequest struct {
	Model    string        `json:"model"`
	Messages []ChatMessage `json:"messages"`
	Stream   bool          `json:"stream"`
}

// ollamaChatResponse is the trimmed-down shape we read back. Ollama
// returns more fields (total_duration, load_duration, etc.) that we
// ignore on purpose to keep the contract narrow.
type ollamaChatResponse struct {
	Message struct {
		Role    string `json:"role"`
		Content string `json:"content"`
	} `json:"message"`
	Done            bool   `json:"done"`
	PromptEvalCount int64  `json:"prompt_eval_count"`
	EvalCount       int64  `json:"eval_count"`
	Error           string `json:"error"`
}

// Chat implements LLM by POSTing to /api/chat. Errors fall into four
// buckets: build/transport errors (wrapped with the URL for ops
// triage), non-200 responses (wrapped with the status + first 200
// bytes of body to surface Ollama's own error message), JSON decode
// failures (likely an Ollama version mismatch), and explicit error
// fields in a 200 response.
func (o *Ollama) Chat(ctx context.Context, model string, messages []ChatMessage) (ChatResponse, error) {
	body, err := json.Marshal(ollamaChatRequest{
		Model:    model,
		Messages: messages,
		Stream:   false,
	})
	if err != nil {
		return ChatResponse{}, fmt.Errorf("ollama: marshal request: %w", err)
	}

	url := o.BaseURL + "/api/chat"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return ChatResponse{}, fmt.Errorf("ollama: build request %s: %w", url, err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := o.HTTP.Do(req)
	if err != nil {
		return ChatResponse{}, fmt.Errorf("ollama: POST %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		// Surface up to 256 bytes of Ollama's body so the operator
		// can see "model not found" / "out of memory" without
		// switching to curl.
		preview, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
		return ChatResponse{}, fmt.Errorf("ollama: %s returned %s: %s",
			url, resp.Status, bytes.TrimSpace(preview))
	}

	var parsed ollamaChatResponse
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return ChatResponse{}, fmt.Errorf("ollama: decode response: %w", err)
	}
	if parsed.Error != "" {
		return ChatResponse{}, fmt.Errorf("ollama: server error: %s", parsed.Error)
	}

	return ChatResponse{
		Content:      parsed.Message.Content,
		TokensInput:  parsed.PromptEvalCount,
		TokensOutput: parsed.EvalCount,
	}, nil
}
