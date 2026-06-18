package shred

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/laenenai/eventstore/es"
)

// fakeSource serves a fixed slice of envelopes, slicing by FromPosition
// on each call. Mirrors the semantic shape of es.Store.ReadAllForTenant
// without dragging in a real adapter.
type fakeSource struct {
	envs []es.Envelope
}

func (f *fakeSource) ReadAllForTenant(_ context.Context, tenantID string, fromPosition uint64, limit int) ([]es.Envelope, error) {
	var out []es.Envelope
	for _, env := range f.envs {
		if env.TenantID != tenantID {
			continue
		}
		if env.GlobalPosition <= fromPosition {
			continue
		}
		out = append(out, env)
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out, nil
}

// subjectKeyInspector flags envelopes whose TypeURL is in the set and
// whose envelope's StreamID contains the subject. The body is a JSON
// dict with type and stream — enough to verify ordering and payload
// shape without standing up a real codec.
type subjectKeyInspector struct {
	wantTypes map[string]bool
}

func (s subjectKeyInspector) Inspect(_ context.Context, env es.Envelope, subject string) ([]byte, error) {
	if !s.wantTypes[env.TypeURL] {
		return nil, nil
	}
	if env.StreamID.ID != subject {
		return nil, nil
	}
	return json.Marshal(map[string]string{
		"type":   env.TypeURL,
		"stream": env.StreamID.Canonical(),
	})
}

func mkEnv(t *testing.T, tenant, streamType, streamID, typeURL string, gp uint64) es.Envelope {
	t.Helper()
	sid, err := es.NewStreamID(tenant, streamType, streamID)
	if err != nil {
		t.Fatalf("NewStreamID: %v", err)
	}
	return es.Envelope{
		EventID:        uuid.New(),
		TenantID:       tenant,
		StreamID:       sid,
		Version:        1,
		GlobalPosition: gp,
		TypeURL:        typeURL,
		SchemaVersion:  1,
		OccurredAt:     time.Date(2026, 6, 18, 12, 0, 0, 0, time.UTC),
		RecordedAt:     time.Date(2026, 6, 18, 12, 0, 0, 0, time.UTC),
	}
}

func TestRunSubjectExport_FiltersToSubjectAndChronological(t *testing.T) {
	envs := []es.Envelope{
		mkEnv(t, "tenant-a", "conversation", "alice", "myapp.conversation.v1.UserMessageAppended", 10),
		mkEnv(t, "tenant-a", "conversation", "bob", "myapp.conversation.v1.UserMessageAppended", 11),
		mkEnv(t, "tenant-a", "conversation", "alice", "myapp.conversation.v1.AssistantMessageAppended", 12),
		mkEnv(t, "tenant-a", "order", "alice", "myapp.order.v1.OrderPlaced", 13),
		mkEnv(t, "tenant-b", "conversation", "alice", "myapp.conversation.v1.UserMessageAppended", 14), // wrong tenant
		mkEnv(t, "tenant-a", "conversation", "alice", "myapp.conversation.v1.ConversationClosed", 15),
	}
	src := &fakeSource{envs: envs}
	insp := subjectKeyInspector{wantTypes: map[string]bool{
		"myapp.conversation.v1.UserMessageAppended":      true,
		"myapp.conversation.v1.AssistantMessageAppended": true,
		"myapp.conversation.v1.ConversationClosed":       true,
		"myapp.order.v1.OrderPlaced":                     true,
	}}

	got, err := RunSubjectExport(context.Background(), src, insp, SubjectExportRequest{
		TenantID: "tenant-a",
		Subject:  "alice",
	})
	if err != nil {
		t.Fatalf("RunSubjectExport: %v", err)
	}
	if len(got.Records) != 4 {
		t.Fatalf("records: got %d want 4 (alice's 3 conversation events + 1 order)", len(got.Records))
	}
	wantPositions := []uint64{10, 12, 13, 15}
	for i, r := range got.Records {
		if r.GlobalPosition != wantPositions[i] {
			t.Errorf("record[%d].GlobalPosition: got %d want %d", i, r.GlobalPosition, wantPositions[i])
		}
	}
	if got.LastPosition != 15 {
		t.Errorf("LastPosition: got %d want 15", got.LastPosition)
	}
	if got.Truncated {
		t.Error("Truncated: got true want false")
	}
}

func TestRunSubjectExport_MaxRecordsTruncates(t *testing.T) {
	envs := []es.Envelope{
		mkEnv(t, "tenant-a", "conversation", "alice", "T1", 1),
		mkEnv(t, "tenant-a", "conversation", "alice", "T1", 2),
		mkEnv(t, "tenant-a", "conversation", "alice", "T1", 3),
		mkEnv(t, "tenant-a", "conversation", "alice", "T1", 4),
	}
	src := &fakeSource{envs: envs}
	insp := subjectKeyInspector{wantTypes: map[string]bool{"T1": true}}

	got, err := RunSubjectExport(context.Background(), src, insp, SubjectExportRequest{
		TenantID:   "tenant-a",
		Subject:    "alice",
		MaxRecords: 2,
	})
	if err != nil {
		t.Fatalf("RunSubjectExport: %v", err)
	}
	if len(got.Records) != 2 {
		t.Fatalf("records: got %d want 2", len(got.Records))
	}
	if !got.Truncated {
		t.Error("Truncated: want true")
	}
	if got.LastPosition != 2 {
		t.Errorf("LastPosition: got %d want 2", got.LastPosition)
	}
}

func TestRunSubjectExport_ResumeFromCursor(t *testing.T) {
	envs := []es.Envelope{
		mkEnv(t, "tenant-a", "conversation", "alice", "T1", 1),
		mkEnv(t, "tenant-a", "conversation", "alice", "T1", 2),
		mkEnv(t, "tenant-a", "conversation", "alice", "T1", 3),
		mkEnv(t, "tenant-a", "conversation", "alice", "T1", 4),
	}
	src := &fakeSource{envs: envs}
	insp := subjectKeyInspector{wantTypes: map[string]bool{"T1": true}}

	got, err := RunSubjectExport(context.Background(), src, insp, SubjectExportRequest{
		TenantID:     "tenant-a",
		Subject:      "alice",
		FromPosition: 2, // resume after position 2
	})
	if err != nil {
		t.Fatalf("RunSubjectExport: %v", err)
	}
	if len(got.Records) != 2 {
		t.Fatalf("records: got %d want 2 (positions 3 and 4)", len(got.Records))
	}
	if got.Records[0].GlobalPosition != 3 || got.Records[1].GlobalPosition != 4 {
		t.Errorf("expected positions [3,4], got [%d,%d]",
			got.Records[0].GlobalPosition, got.Records[1].GlobalPosition)
	}
}

func TestRunSubjectExport_InspectorErrorAborts(t *testing.T) {
	envs := []es.Envelope{
		mkEnv(t, "tenant-a", "conversation", "alice", "T1", 1),
	}
	src := &fakeSource{envs: envs}
	sentinel := errors.New("decode failed")
	insp := errInspector{err: sentinel}
	_, err := RunSubjectExport(context.Background(), src, insp, SubjectExportRequest{
		TenantID: "tenant-a",
		Subject:  "alice",
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("err: got %v want chain containing sentinel", err)
	}
}

type errInspector struct{ err error }

func (e errInspector) Inspect(context.Context, es.Envelope, string) ([]byte, error) {
	return nil, e.err
}

func TestRunSubjectExport_ChainPicksFirstHit(t *testing.T) {
	envs := []es.Envelope{
		mkEnv(t, "tenant-a", "conversation", "alice", "T1", 1),
		mkEnv(t, "tenant-a", "order", "alice", "T2", 2),
	}
	src := &fakeSource{envs: envs}
	chain := SubjectInspectorChain{
		subjectKeyInspector{wantTypes: map[string]bool{"T1": true}},
		subjectKeyInspector{wantTypes: map[string]bool{"T2": true}},
	}
	got, err := RunSubjectExport(context.Background(), src, chain, SubjectExportRequest{
		TenantID: "tenant-a",
		Subject:  "alice",
	})
	if err != nil {
		t.Fatalf("RunSubjectExport: %v", err)
	}
	if len(got.Records) != 2 {
		t.Fatalf("records: got %d want 2", len(got.Records))
	}
}

func TestRunSubjectExport_Validation(t *testing.T) {
	cases := []struct {
		name string
		req  SubjectExportRequest
	}{
		{"missing tenant", SubjectExportRequest{Subject: "alice"}},
		{"missing subject", SubjectExportRequest{TenantID: "tenant-a"}},
	}
	insp := subjectKeyInspector{}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := RunSubjectExport(context.Background(), &fakeSource{}, insp, c.req)
			if err == nil {
				t.Fatalf("expected validation error")
			}
		})
	}
	// inspector nil is its own error path
	_, err := RunSubjectExport(context.Background(), &fakeSource{}, nil, SubjectExportRequest{
		TenantID: "tenant-a", Subject: "alice",
	})
	if err == nil {
		t.Fatalf("expected error on nil inspector")
	}
}

func TestRunSubjectExport_PayloadIsJSONAndReturnedRedacted(t *testing.T) {
	// The Inspector controls the payload bytes. The runner must
	// preserve them as RawMessage so the regulator-facing output
	// keeps the Inspector's choice of redaction (verbatim).
	envs := []es.Envelope{
		mkEnv(t, "tenant-a", "conversation", "alice", "T1", 1),
	}
	src := &fakeSource{envs: envs}
	insp := subjectKeyInspector{wantTypes: map[string]bool{"T1": true}}

	got, err := RunSubjectExport(context.Background(), src, insp, SubjectExportRequest{
		TenantID: "tenant-a",
		Subject:  "alice",
	})
	if err != nil {
		t.Fatalf("RunSubjectExport: %v", err)
	}
	var parsed map[string]string
	if err := json.Unmarshal(got.Records[0].Payload, &parsed); err != nil {
		t.Fatalf("payload not JSON: %v", err)
	}
	if parsed["stream"] != "conversation:alice" {
		t.Errorf("payload.stream: got %q want %q (canonical type:id form)", parsed["stream"], "conversation:alice")
	}
	_ = fmt.Sprintf("%v", parsed) // touch fmt to ensure import survives
}
