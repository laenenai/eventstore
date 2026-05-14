package testsupport

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	restatesdk "github.com/restatedev/sdk-go"
	"github.com/restatedev/sdk-go/ingress"
	"github.com/restatedev/sdk-go/server"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	restateAdminPort   = "9070"
	restateIngressPort = "8080"
	defaultImage       = "docker.io/restatedev/restate:latest"
)

// Env holds the running Restate testcontainer + the in-process SDK
// HTTP server. Both are torn down via t.Cleanup.
type Env struct {
	t             *testing.T
	srv           *httptest.Server
	container     testcontainers.Container
	adminPort     int
	ingressPort   int
	ingressClient *ingress.Client
}

// Start launches a Restate container, starts an in-process SDK HTTP
// server that hosts the given services, and registers the server's
// URL with Restate's admin endpoint. Returns an Env exposing the
// ingress client and ports.
//
// The container, SDK server, and HTTP listener are all cleaned up
// when the test ends.
func Start(t *testing.T, services ...restatesdk.ServiceDefinition) *Env {
	t.Helper()
	return StartWithImage(t, defaultImage, services...)
}

// StartWithImage is Start with an explicit container image.
func StartWithImage(t *testing.T, image string, services ...restatesdk.ServiceDefinition) *Env {
	t.Helper()
	ctx := context.Background()

	// 1. SDK server in-process. HTTP/2 cleartext required by the
	//    Restate protocol.
	restateSrv := server.NewRestate()
	for _, s := range services {
		restateSrv.Bind(s)
	}
	handler, err := restateSrv.Handler()
	if err != nil {
		t.Fatalf("restate Handler: %v", err)
	}
	srv := httptest.NewUnstartedServer(handler)
	var protocols http.Protocols
	protocols.SetUnencryptedHTTP2(true)
	srv.Config.Protocols = &protocols
	srv.EnableHTTP2 = true
	srv.Start()
	t.Cleanup(func() { srv.Close() })

	sdkPort, err := strconv.Atoi(strings.Split(srv.URL, ":")[2])
	if err != nil {
		t.Fatalf("parse SDK server port: %v", err)
	}

	// 2. Restate container — admin + ingress ports + host-port-access
	//    so the container can reach the SDK server at host.docker.internal.
	container, err := testcontainers.Run(ctx, image,
		testcontainers.WithEnv(map[string]string{
			"RUST_LOG": "warn",
			"RESTATE_META__REST_ADDRESS":           "0.0.0.0:" + restateAdminPort,
			"RESTATE_WORKER__INGRESS__BIND_ADDRESS": "0.0.0.0:" + restateIngressPort,
		}),
		testcontainers.WithExposedPorts(restateIngressPort+"/tcp", restateAdminPort+"/tcp"),
		testcontainers.WithWaitStrategyAndDeadline(time.Minute,
			wait.ForAll(
				wait.ForHTTP("/health").WithPort(restateAdminPort+"/tcp"),
				wait.ForHTTP("/restate/health").WithPort(restateIngressPort+"/tcp"),
			),
		),
		testcontainers.WithHostPortAccess(sdkPort),
	)
	testcontainers.CleanupContainer(t, container)
	if err != nil {
		t.Fatalf("start Restate container: %v", err)
	}

	adminMapped, err := container.MappedPort(ctx, restateAdminPort)
	if err != nil {
		t.Fatalf("admin MappedPort: %v", err)
	}
	ingressMapped, err := container.MappedPort(ctx, restateIngressPort)
	if err != nil {
		t.Fatalf("ingress MappedPort: %v", err)
	}
	adminPort := int(adminMapped.Num())
	ingressPort := int(ingressMapped.Num())

	// 3. Self-register the SDK server with Restate's admin API.
	//    From inside the container, the host's address is
	//    testcontainers.HostInternal (host.docker.internal on
	//    Docker Desktop; host-gateway alias elsewhere).
	regURL := fmt.Sprintf("http://localhost:%d/deployments", adminPort)
	regBody := fmt.Sprintf(`{"uri":"http://%s:%d"}`, testcontainers.HostInternal, sdkPort)
	resp, err := http.Post(regURL, "application/json", bytes.NewBufferString(regBody))
	if err != nil {
		t.Fatalf("register SDK server: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("register SDK server: got %d", resp.StatusCode)
	}

	client := ingress.NewClient(fmt.Sprintf("http://localhost:%d", ingressPort))

	return &Env{
		t:             t,
		srv:           srv,
		container:     container,
		adminPort:     adminPort,
		ingressPort:   ingressPort,
		ingressClient: client,
	}
}

// Ingress returns the SDK's ingress client pointing at the running
// Restate container. Use this to invoke registered services.
func (e *Env) Ingress() *ingress.Client { return e.ingressClient }

// IngressPort returns the host-mapped ingress port (8080 inside the
// container).
func (e *Env) IngressPort() int { return e.ingressPort }

// AdminPort returns the host-mapped admin port (9070 inside the
// container). Use it for additional admin-API operations beyond the
// default registration.
func (e *Env) AdminPort() int { return e.adminPort }

// Container exposes the underlying testcontainer for tests that need
// to restart it (durability scenarios).
func (e *Env) Container() testcontainers.Container { return e.container }
