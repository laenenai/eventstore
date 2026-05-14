package restate_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"

	"github.com/laenenai/eventstore/es"
	"github.com/laenenai/eventstore/adapters/publisher/restate"
)

func mustStreamID(t *testing.T) es.StreamID {
	t.Helper()
	sid, err := es.ParseCanonical("t-restate", "counter:1")
	if err != nil {
		t.Fatalf("ParseCanonical: %v", err)
	}
	return sid
}

func sampleEnvelope(t *testing.T) es.Envelope {
	return es.Envelope{
		EventID:        uuid.MustParse("0193f3a2-0001-7000-8000-000000000001"),
		TenantID:       "t-restate",
		StreamID:       mustStreamID(t),
		Version:        7,
		GlobalPosition: 42,
		TypeURL:        "myapp.counter.v1.Incremented",
		SchemaVersion:  3,
		Payload:        []byte("\x08\x01"), // dummy proto bytes
	}
}

// TestPublisher_PostsToIngressWithHeaders verifies envelope → HTTP
// POST mapping: URL, idempotency-key, EventStore headers, body.
func TestPublisher_PostsToIngressWithHeaders(t *testing.T) {
	var (
		mu       sync.Mutex
		gotPath  string
		gotMeta  http.Header
		gotBody  []byte
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		gotPath = r.URL.Path
		gotMeta = r.Header.Clone()
		gotBody = body
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	pub, err := restate.New(restate.Config{
		IngressURL: srv.URL,
		Service:    "events",
		Handler:    "OnEvent",
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	env := sampleEnvelope(t)
	if err := pub.Publish(context.Background(), env); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if gotPath != "/events/OnEvent" {
		t.Errorf("path: got %q want /events/OnEvent", gotPath)
	}
	if gotMeta.Get("Idempotency-Key") != env.EventID.String() {
		t.Errorf("Idempotency-Key: got %q want %q",
			gotMeta.Get("Idempotency-Key"), env.EventID.String())
	}
	if gotMeta.Get("X-EventStore-Tenant") != env.TenantID {
		t.Errorf("X-EventStore-Tenant: got %q", gotMeta.Get("X-EventStore-Tenant"))
	}
	if gotMeta.Get("X-EventStore-Type-URL") != env.TypeURL {
		t.Errorf("X-EventStore-Type-URL: got %q", gotMeta.Get("X-EventStore-Type-URL"))
	}
	if gotMeta.Get("X-EventStore-Global-Position") != "42" {
		t.Errorf("X-EventStore-Global-Position: got %q", gotMeta.Get("X-EventStore-Global-Position"))
	}
	if gotMeta.Get("Content-Type") != "application/x-protobuf" {
		t.Errorf("Content-Type: got %q", gotMeta.Get("Content-Type"))
	}
	if string(gotBody) != string(env.Payload) {
		t.Errorf("body: got %q want %q", gotBody, env.Payload)
	}
}

// TestPublisher_Non2xxReturnsError so the outbox drain knows to retry.
func TestPublisher_Non2xxReturnsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "subscriber crashed", http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)

	pub, err := restate.New(restate.Config{
		IngressURL: srv.URL,
		Service:    "events",
		Handler:    "OnEvent",
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	err = pub.Publish(context.Background(), sampleEnvelope(t))
	if err == nil {
		t.Fatalf("expected error on 500, got nil")
	}
	if !strings.Contains(err.Error(), "500") {
		t.Errorf("error should mention status: %v", err)
	}
}

// TestPublisher_AuthTokenIsForwarded confirms the bearer header.
func TestPublisher_AuthTokenIsForwarded(t *testing.T) {
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	pub, _ := restate.New(restate.Config{
		IngressURL: srv.URL, Service: "events", Handler: "OnEvent",
		AuthToken: "secret-token",
	})
	if err := pub.Publish(context.Background(), sampleEnvelope(t)); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if gotAuth != "Bearer secret-token" {
		t.Errorf("auth header: got %q", gotAuth)
	}
}

// TestPublisher_Validation checks required-field errors.
func TestPublisher_Validation(t *testing.T) {
	cases := []struct {
		name string
		cfg  restate.Config
	}{
		{"missing IngressURL", restate.Config{Service: "s", Handler: "h"}},
		{"missing Service", restate.Config{IngressURL: "http://x", Handler: "h"}},
		{"missing Handler", restate.Config{IngressURL: "http://x", Service: "s"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := restate.New(tc.cfg)
			if err == nil {
				t.Errorf("expected validation error")
			}
		})
	}
}

// TestPublisher_NetworkErrorPropagates with a deliberately closed server.
func TestPublisher_NetworkErrorPropagates(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	url := srv.URL
	srv.Close() // immediately

	pub, _ := restate.New(restate.Config{IngressURL: url, Service: "s", Handler: "h"})
	err := pub.Publish(context.Background(), sampleEnvelope(t))
	if err == nil {
		t.Fatalf("expected network error, got nil")
	}
}
