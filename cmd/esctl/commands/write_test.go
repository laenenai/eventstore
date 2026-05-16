package commands

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/laenenai/eventstore/es"
)

// fakeStore is a minimal Store that records calls to the write methods
// we exercise here. Only methods touched by the write commands need
// real behaviour; everything else returns zero values so the interface
// is satisfied.
type fakeStore struct {
	// captured calls
	resetCalls         []resetCall
	resetToCalls       []resetToCall
	replayDLQCalls     []replayDLQCall
	replayAllDLQCalls  []replayAllDLQCall
	abandonDLQCalls    []abandonDLQCall
	wipeStateCacheArgs []wipeCall

	// canned data
	listResp []es.ProjectionStatus

	// canned counts
	wipeReturn   int64
	replayAllRet int64
}

type resetCall struct{ name, tenant string }
type resetToCall struct {
	name, tenant string
	position     uint64
}
type replayDLQCall struct {
	tenant string
	pos    uint64
}
type replayAllDLQCall struct {
	tenant      string
	maxAttempts int32
}
type abandonDLQCall struct {
	tenant string
	pos    uint64
}
type wipeCall struct{ tenant, typeURL string }

// --- es.Store (no-op stubs) ----------------------------------------------

func (f *fakeStore) Append(context.Context, es.AppendParams) (es.AppendResult, error) {
	return es.AppendResult{}, nil
}
func (f *fakeStore) ReadStream(context.Context, es.StreamID, uint64) ([]es.Envelope, error) {
	return nil, nil
}
func (f *fakeStore) ReadAll(context.Context, uint64, int) ([]es.Envelope, error) {
	return nil, nil
}
func (f *fakeStore) ReadAllForTenant(context.Context, string, uint64, int) ([]es.Envelope, error) {
	return nil, nil
}
func (f *fakeStore) CurrentStreamVersion(context.Context, es.StreamID) (uint64, error) {
	return 0, nil
}
func (f *fakeStore) GetEventByID(context.Context, string, uuid.UUID) (es.Envelope, error) {
	return es.Envelope{}, nil
}

// --- es.StateCacheReader -------------------------------------------------

func (f *fakeStore) GetState(context.Context, string, string) (es.StateCacheRow, error) {
	return es.StateCacheRow{}, nil
}
func (f *fakeStore) ListStates(context.Context, string, string, string, int) ([]es.StateCacheRow, error) {
	return nil, nil
}

// --- es.StateCacheWriter -------------------------------------------------

func (f *fakeStore) WipeStateCacheForType(_ context.Context, tenant, typeURL string) (int64, error) {
	f.wipeStateCacheArgs = append(f.wipeStateCacheArgs, wipeCall{tenant: tenant, typeURL: typeURL})
	return f.wipeReturn, nil
}

// --- es.OutboxAdmin ------------------------------------------------------

func (f *fakeStore) CountPending(context.Context, string) (int64, error) { return 0, nil }
func (f *fakeStore) CountFailing(context.Context, string, int32) (int64, error) {
	return 0, nil
}
func (f *fakeStore) CountDLQ(context.Context, string, int32) (int64, error) {
	return 0, nil
}
func (f *fakeStore) ListDLQ(context.Context, string, int32, uint64, int) ([]es.DLQRow, error) {
	return nil, nil
}
func (f *fakeStore) ReplayDLQ(_ context.Context, tenant string, pos uint64) error {
	f.replayDLQCalls = append(f.replayDLQCalls, replayDLQCall{tenant: tenant, pos: pos})
	return nil
}
func (f *fakeStore) AbandonDLQ(_ context.Context, tenant string, pos uint64) error {
	f.abandonDLQCalls = append(f.abandonDLQCalls, abandonDLQCall{tenant: tenant, pos: pos})
	return nil
}
func (f *fakeStore) ReplayAllDLQ(_ context.Context, tenant string, maxAttempts int32) (int64, error) {
	f.replayAllDLQCalls = append(f.replayAllDLQCalls, replayAllDLQCall{tenant: tenant, maxAttempts: maxAttempts})
	return f.replayAllRet, nil
}

// --- es.ProjectionAdmin --------------------------------------------------

func (f *fakeStore) Status(context.Context, string, string) (es.ProjectionStatus, error) {
	return es.ProjectionStatus{}, nil
}
func (f *fakeStore) Reset(_ context.Context, name, tenant string) error {
	f.resetCalls = append(f.resetCalls, resetCall{name: name, tenant: tenant})
	return nil
}
func (f *fakeStore) ResetTo(_ context.Context, name, tenant string, pos uint64) error {
	f.resetToCalls = append(f.resetToCalls, resetToCall{name: name, tenant: tenant, position: pos})
	return nil
}
func (f *fakeStore) List(context.Context) ([]es.ProjectionStatus, error) {
	return f.listResp, nil
}

// installFakeStore swaps the global hook for the duration of one test.
func installFakeStore(t *testing.T, f *fakeStore) {
	t.Helper()
	prev := storeOverride
	storeOverride = f
	t.Cleanup(func() { storeOverride = prev })
}

// captureOutputs redirects audit + dry-run writers to in-memory buffers
// and restores them on cleanup.
func captureOutputs(t *testing.T) (*bytes.Buffer, *bytes.Buffer) {
	t.Helper()
	stdoutBuf := &bytes.Buffer{}
	stderrBuf := &bytes.Buffer{}
	prevDry := dryRunOut
	prevAudit := auditOut
	dryRunOut = stdoutBuf
	auditOut = stderrBuf
	t.Cleanup(func() {
		dryRunOut = prevDry
		auditOut = prevAudit
	})
	return stdoutBuf, stderrBuf
}

// runCmd assembles a root command equivalent to main's, parses the
// given args, and runs the action. Returns the captured stdout/stderr
// and any error.
func runCmd(t *testing.T, args ...string) (string, string, error) {
	t.Helper()
	stdout, stderr := captureOutputs(t)
	root := newTestRoot()
	err := root.Run(context.Background(), append([]string{"esctl", "--db", "file:./ignored"}, args...))
	return stdout.String(), stderr.String(), err
}

// --- projection reset ----------------------------------------------------

func TestProjectionReset_DryRun(t *testing.T) {
	f := &fakeStore{}
	installFakeStore(t, f)
	stdout, stderr, err := runCmd(t,
		"--tenant", "acme",
		"projection", "reset", "--name", "billing")
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(f.resetCalls) != 0 {
		t.Errorf("expected no Reset calls, got %+v", f.resetCalls)
	}
	if !strings.Contains(stdout, "DRY RUN: would projection reset") {
		t.Errorf("missing DRY RUN line in stdout: %q", stdout)
	}
	if !strings.Contains(stdout, "name=billing") {
		t.Errorf("missing args in DRY RUN line: %q", stdout)
	}
	if stderr != "" {
		t.Errorf("expected empty stderr on dry-run, got %q", stderr)
	}
}

func TestProjectionReset_Yes(t *testing.T) {
	f := &fakeStore{}
	installFakeStore(t, f)
	stdout, stderr, err := runCmd(t,
		"--yes", "--tenant", "acme",
		"projection", "reset", "--name", "billing")
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if got := f.resetCalls; len(got) != 1 || got[0] != (resetCall{name: "billing", tenant: "acme"}) {
		t.Errorf("expected one Reset(billing,acme), got %+v", got)
	}
	if !strings.Contains(stdout, "OK reset cursor") {
		t.Errorf("missing OK message in stdout: %q", stdout)
	}
	if !strings.Contains(stderr, "[esctl-write]") || !strings.Contains(stderr, "projection reset") {
		t.Errorf("expected audit line on stderr, got %q", stderr)
	}
	if !strings.Contains(stderr, "tenant=acme") {
		t.Errorf("audit line missing tenant: %q", stderr)
	}
}

func TestProjectionReset_AllTenants(t *testing.T) {
	f := &fakeStore{
		listResp: []es.ProjectionStatus{
			{Name: "billing", TenantID: "acme", Cursor: 100},
			{Name: "billing", TenantID: "beta", Cursor: 50},
			{Name: "other", TenantID: "acme", Cursor: 200},
		},
	}
	installFakeStore(t, f)
	stdout, stderr, err := runCmd(t,
		"--yes",
		"projection", "reset", "--name", "billing", "--all-tenants")
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if got := len(f.resetCalls); got != 2 {
		t.Errorf("expected 2 Reset calls (one per matching tenant), got %d (%+v)",
			got, f.resetCalls)
	}
	if !strings.Contains(stdout, "OK reset 2 tenant cursor") {
		t.Errorf("missing OK message: %q", stdout)
	}
	if !strings.Contains(stderr, "[esctl-write] projection reset") {
		t.Errorf("missing audit line: %q", stderr)
	}
}

// --- projection reset-to -------------------------------------------------

func TestProjectionResetTo_DryRun(t *testing.T) {
	f := &fakeStore{}
	installFakeStore(t, f)
	stdout, _, err := runCmd(t,
		"--tenant", "acme",
		"projection", "reset-to", "--name", "billing", "--position", "1234")
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(f.resetToCalls) != 0 {
		t.Errorf("expected no ResetTo calls, got %+v", f.resetToCalls)
	}
	if !strings.Contains(stdout, "DRY RUN: would projection reset-to") {
		t.Errorf("missing DRY RUN line: %q", stdout)
	}
	if !strings.Contains(stdout, "position=1234") {
		t.Errorf("missing position arg: %q", stdout)
	}
}

func TestProjectionResetTo_Yes(t *testing.T) {
	f := &fakeStore{}
	installFakeStore(t, f)
	stdout, stderr, err := runCmd(t,
		"--yes", "--tenant", "acme",
		"projection", "reset-to", "--name", "billing", "--position", "1234")
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	want := resetToCall{name: "billing", tenant: "acme", position: 1234}
	if got := f.resetToCalls; len(got) != 1 || got[0] != want {
		t.Errorf("expected ResetTo %+v, got %+v", want, got)
	}
	if !strings.Contains(stdout, "OK reset cursor for billing/acme to gp=1234") {
		t.Errorf("missing OK message: %q", stdout)
	}
	if !strings.Contains(stderr, "[esctl-write] projection reset-to") {
		t.Errorf("missing audit line: %q", stderr)
	}
}

// --- outbox retry --------------------------------------------------------

func TestOutboxRetry_DryRun(t *testing.T) {
	f := &fakeStore{}
	installFakeStore(t, f)
	stdout, _, err := runCmd(t,
		"--tenant", "acme",
		"outbox", "retry", "--position", "42")
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(f.replayDLQCalls) != 0 {
		t.Errorf("expected no ReplayDLQ calls, got %+v", f.replayDLQCalls)
	}
	if !strings.Contains(stdout, "DRY RUN: would outbox retry") {
		t.Errorf("missing DRY RUN line: %q", stdout)
	}
}

func TestOutboxRetry_Yes(t *testing.T) {
	f := &fakeStore{}
	installFakeStore(t, f)
	_, stderr, err := runCmd(t,
		"--yes", "--tenant", "acme",
		"outbox", "retry", "--position", "42")
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	want := replayDLQCall{tenant: "acme", pos: 42}
	if got := f.replayDLQCalls; len(got) != 1 || got[0] != want {
		t.Errorf("expected ReplayDLQ %+v, got %+v", want, got)
	}
	if !strings.Contains(stderr, "[esctl-write] outbox retry") {
		t.Errorf("missing audit line: %q", stderr)
	}
}

// --- outbox retry-all ----------------------------------------------------

func TestOutboxRetryAll_DryRun(t *testing.T) {
	f := &fakeStore{}
	installFakeStore(t, f)
	stdout, _, err := runCmd(t,
		"--tenant", "acme",
		"outbox", "retry-all", "--max-attempts", "10")
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(f.replayAllDLQCalls) != 0 {
		t.Errorf("expected no ReplayAllDLQ calls, got %+v", f.replayAllDLQCalls)
	}
	if !strings.Contains(stdout, "DRY RUN: would outbox retry-all") {
		t.Errorf("missing DRY RUN line: %q", stdout)
	}
}

func TestOutboxRetryAll_Yes(t *testing.T) {
	f := &fakeStore{replayAllRet: 7}
	installFakeStore(t, f)
	stdout, stderr, err := runCmd(t,
		"--yes", "--tenant", "acme",
		"outbox", "retry-all", "--max-attempts", "10")
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	want := replayAllDLQCall{tenant: "acme", maxAttempts: 10}
	if got := f.replayAllDLQCalls; len(got) != 1 || got[0] != want {
		t.Errorf("expected ReplayAllDLQ %+v, got %+v", want, got)
	}
	if !strings.Contains(stdout, "OK queued 7 DLQ row(s)") {
		t.Errorf("missing OK message: %q", stdout)
	}
	if !strings.Contains(stderr, "[esctl-write] outbox retry-all") {
		t.Errorf("missing audit line: %q", stderr)
	}
}

// --- outbox abandon ------------------------------------------------------

func TestOutboxAbandon_DryRun(t *testing.T) {
	f := &fakeStore{}
	installFakeStore(t, f)
	stdout, _, err := runCmd(t,
		"--tenant", "acme",
		"outbox", "abandon", "--position", "99")
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(f.abandonDLQCalls) != 0 {
		t.Errorf("expected no AbandonDLQ calls, got %+v", f.abandonDLQCalls)
	}
	if !strings.Contains(stdout, "DRY RUN: would outbox abandon") {
		t.Errorf("missing DRY RUN line: %q", stdout)
	}
}

func TestOutboxAbandon_Yes(t *testing.T) {
	f := &fakeStore{}
	installFakeStore(t, f)
	_, stderr, err := runCmd(t,
		"--yes", "--tenant", "acme",
		"outbox", "abandon", "--position", "99")
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	want := abandonDLQCall{tenant: "acme", pos: 99}
	if got := f.abandonDLQCalls; len(got) != 1 || got[0] != want {
		t.Errorf("expected AbandonDLQ %+v, got %+v", want, got)
	}
	if !strings.Contains(stderr, "[esctl-write] outbox abandon") {
		t.Errorf("missing audit line: %q", stderr)
	}
}

// --- state-cache rebuild -------------------------------------------------

func TestStateCacheRebuild_DryRun(t *testing.T) {
	f := &fakeStore{}
	installFakeStore(t, f)
	stdout, _, err := runCmd(t,
		"--tenant", "acme",
		"state-cache", "rebuild", "--type", "myapp.employee.v1.Employee")
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(f.wipeStateCacheArgs) != 0 {
		t.Errorf("expected no WipeStateCacheForType calls, got %+v", f.wipeStateCacheArgs)
	}
	if !strings.Contains(stdout, "DRY RUN: would state-cache rebuild") {
		t.Errorf("missing DRY RUN line: %q", stdout)
	}
}

func TestStateCacheRebuild_Yes(t *testing.T) {
	f := &fakeStore{wipeReturn: 42}
	installFakeStore(t, f)
	stdout, stderr, err := runCmd(t,
		"--yes", "--tenant", "acme",
		"state-cache", "rebuild", "--type", "myapp.employee.v1.Employee")
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	want := wipeCall{tenant: "acme", typeURL: "myapp.employee.v1.Employee"}
	if got := f.wipeStateCacheArgs; len(got) != 1 || got[0] != want {
		t.Errorf("expected WipeStateCacheForType %+v, got %+v", want, got)
	}
	if !strings.Contains(stdout, "OK wiped 42 state_cache row(s)") {
		t.Errorf("missing OK message: %q", stdout)
	}
	if !strings.Contains(stderr, "[esctl-write] state-cache rebuild") {
		t.Errorf("missing audit line: %q", stderr)
	}
}
