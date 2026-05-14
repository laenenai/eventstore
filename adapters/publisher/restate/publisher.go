package restate

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/laenenai/eventstore/es"
	"github.com/laenenai/eventstore/publisher"
)

// Config controls a Restate Publisher.
type Config struct {
	// IngressURL is the base URL of the Restate ingress (no trailing
	// slash). Required.
	IngressURL string

	// Service is the Restate service name that receives events.
	// Required.
	Service string

	// Handler is the handler name within the service. Required.
	Handler string

	// HTTPClient is used for the POST. When nil, http.DefaultClient
	// with a 30-second timeout is used.
	HTTPClient *http.Client

	// AuthToken is optionally added as `Authorization: Bearer <token>`.
	// Use when Restate is fronted by an auth gateway.
	AuthToken string
}

// Publisher implements publisher.Publisher by POSTing each envelope to
// a Restate ingress. Idempotency is provided by Restate using the
// idempotency-key = event_id header.
type Publisher struct {
	cfg    Config
	client *http.Client
	url    string // pre-computed full URL: IngressURL/Service/Handler
}

// New returns a Publisher configured against the given Restate ingress.
// Validates that IngressURL, Service, and Handler are set.
func New(cfg Config) (*Publisher, error) {
	if cfg.IngressURL == "" {
		return nil, errors.New("restate: Config.IngressURL is required")
	}
	if cfg.Service == "" {
		return nil, errors.New("restate: Config.Service is required")
	}
	if cfg.Handler == "" {
		return nil, errors.New("restate: Config.Handler is required")
	}
	client := cfg.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	base := strings.TrimRight(cfg.IngressURL, "/")
	return &Publisher{
		cfg:    cfg,
		client: client,
		url:    fmt.Sprintf("%s/%s/%s", base, cfg.Service, cfg.Handler),
	}, nil
}

// Publish implements publisher.Publisher.
func (p *Publisher) Publish(ctx context.Context, env es.Envelope) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.url, bytes.NewReader(env.Payload))
	if err != nil {
		return fmt.Errorf("restate: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Idempotency-Key", env.EventID.String())
	req.Header.Set("X-EventStore-Tenant", env.TenantID)
	req.Header.Set("X-EventStore-Stream", env.StreamID.Canonical())
	req.Header.Set("X-EventStore-Version", strconv.FormatUint(env.Version, 10))
	req.Header.Set("X-EventStore-Global-Position", strconv.FormatUint(env.GlobalPosition, 10))
	req.Header.Set("X-EventStore-Type-URL", env.TypeURL)
	req.Header.Set("X-EventStore-Schema-Version", strconv.FormatUint(uint64(env.SchemaVersion), 10))
	if p.cfg.AuthToken != "" {
		req.Header.Set("Authorization", "Bearer "+p.cfg.AuthToken)
	}

	resp, err := p.client.Do(req)
	if err != nil {
		return fmt.Errorf("restate: POST %s: %w", p.url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		// Drain body so connection can be reused.
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil
	}

	// Cap body to 4 KiB so a misbehaving Restate doesn't fill our error.
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	return fmt.Errorf("restate: POST %s: status %d: %s",
		p.url, resp.StatusCode, strings.TrimSpace(string(body)))
}

// Compile-time check.
var _ publisher.Publisher = (*Publisher)(nil)
