package es_test

import (
	"context"
	"crypto/sha256"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/laenenai/eventstore/es"
)

// fakeStreamReader is the minimal stub used by VerifyStreamChain.
type fakeStreamReader struct {
	envs []es.Envelope
	err  error
}

func (f *fakeStreamReader) ReadStream(_ context.Context, _ es.StreamID, _ uint64) ([]es.Envelope, error) {
	return f.envs, f.err
}

func mustSID(t *testing.T) es.StreamID {
	t.Helper()
	sid, err := es.ParseCanonical("t-1", "x:s-1")
	if err != nil {
		t.Fatalf("ParseCanonical: %v", err)
	}
	return sid
}

// buildChained appends n events into a slice with computed hashes,
// the way the storage adapter would.
func buildChained(t *testing.T, sid es.StreamID, n int) []es.Envelope {
	t.Helper()
	out := make([]es.Envelope, 0, n)
	prev := es.ZeroHash
	occurred := time.Date(2026, 5, 15, 12, 0, 0, 0, time.UTC)
	for i := 0; i < n; i++ {
		e := es.Envelope{
			EventID:       uuid.New(),
			TenantID:      sid.Tenant,
			StreamID:      sid,
			Version:       uint64(i) + 1,
			TypeURL:       "myapp.test.v1.Thing",
			SchemaVersion: 1,
			OccurredAt:    occurred.Add(time.Duration(i) * time.Second),
			Payload:       []byte{byte(i), 0xaa, 0xbb},
			Actor:         es.Actor{Kind: es.ActorUser, Principal: "alice"},
		}
		h, err := es.ComputeChainHash(prev, &e)
		if err != nil {
			t.Fatalf("ComputeChainHash[%d]: %v", i, err)
		}
		e.Hash = h
		e.PrevHash = prev
		out = append(out, e)
		prev = h
	}
	return out
}

func TestZeroHash_Size(t *testing.T) {
	if got := len(es.ZeroHash); got != sha256.Size {
		t.Errorf("ZeroHash length: got %d want %d", got, sha256.Size)
	}
	for i, b := range es.ZeroHash {
		if b != 0 {
			t.Errorf("ZeroHash[%d]=%x; want zero", i, b)
		}
	}
}

func TestComputeChainHash_Deterministic(t *testing.T) {
	sid := mustSID(t)
	envs := buildChained(t, sid, 1)
	h1, err := es.ComputeChainHash(es.ZeroHash, &envs[0])
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	h2, err := es.ComputeChainHash(es.ZeroHash, &envs[0])
	if err != nil {
		t.Fatalf("second: %v", err)
	}
	if string(h1) != string(h2) {
		t.Errorf("same input produced different hashes: %x vs %x", h1, h2)
	}
	if len(h1) != sha256.Size {
		t.Errorf("hash length: got %d want %d", len(h1), sha256.Size)
	}
}

func TestComputeChainHash_PrevHashAffectsResult(t *testing.T) {
	sid := mustSID(t)
	e := buildChained(t, sid, 1)[0]
	h1, _ := es.ComputeChainHash(es.ZeroHash, &e)
	other := make([]byte, sha256.Size)
	other[0] = 0xff
	h2, _ := es.ComputeChainHash(other, &e)
	if string(h1) == string(h2) {
		t.Error("different prevHash produced same chain hash — chain is not load-bearing")
	}
}

func TestComputeChainHash_PayloadAffectsResult(t *testing.T) {
	sid := mustSID(t)
	e := buildChained(t, sid, 1)[0]
	h1, _ := es.ComputeChainHash(es.ZeroHash, &e)
	e.Payload = append([]byte(nil), e.Payload...)
	e.Payload[0] ^= 0xff
	h2, _ := es.ComputeChainHash(es.ZeroHash, &e)
	if string(h1) == string(h2) {
		t.Error("payload mutation didn't change chain hash")
	}
}

func TestComputeChainHash_RejectsBadPrevLength(t *testing.T) {
	sid := mustSID(t)
	e := buildChained(t, sid, 1)[0]
	_, err := es.ComputeChainHash([]byte{0x00, 0x01}, &e)
	if err == nil {
		t.Error("expected error for short prev hash")
	}
}

func TestComputeChainHash_RejectsNilEnvelope(t *testing.T) {
	_, err := es.ComputeChainHash(es.ZeroHash, nil)
	if err == nil {
		t.Error("expected error for nil envelope")
	}
}

func TestVerifyStreamChain_HappyPath(t *testing.T) {
	sid := mustSID(t)
	envs := buildChained(t, sid, 4)
	r := &fakeStreamReader{envs: envs}
	if err := es.VerifyStreamChain(context.Background(), r, sid); err != nil {
		t.Errorf("VerifyStreamChain: %v", err)
	}
}

func TestVerifyStreamChain_EmptyStream(t *testing.T) {
	sid := mustSID(t)
	r := &fakeStreamReader{envs: nil}
	if err := es.VerifyStreamChain(context.Background(), r, sid); err != nil {
		t.Errorf("empty stream should verify: %v", err)
	}
}

func TestVerifyStreamChain_TamperedHash(t *testing.T) {
	sid := mustSID(t)
	envs := buildChained(t, sid, 3)
	// Flip a byte in the middle event's stored Hash.
	envs[1].Hash = append([]byte(nil), envs[1].Hash...)
	envs[1].Hash[0] ^= 0x01

	r := &fakeStreamReader{envs: envs}
	err := es.VerifyStreamChain(context.Background(), r, sid)
	if !errors.Is(err, es.ErrChainBroken) {
		t.Fatalf("want ErrChainBroken, got %v", err)
	}
}

func TestVerifyStreamChain_TamperedPayload(t *testing.T) {
	sid := mustSID(t)
	envs := buildChained(t, sid, 3)
	// Mutate a payload byte in the second event. The stored Hash
	// remains the old one; recompute will produce a different hash.
	envs[1].Payload = append([]byte(nil), envs[1].Payload...)
	envs[1].Payload[0] ^= 0xff

	r := &fakeStreamReader{envs: envs}
	err := es.VerifyStreamChain(context.Background(), r, sid)
	if !errors.Is(err, es.ErrChainBroken) {
		t.Fatalf("want ErrChainBroken on payload tamper, got %v", err)
	}
}

func TestVerifyStreamChain_TamperedFirstEvent(t *testing.T) {
	sid := mustSID(t)
	envs := buildChained(t, sid, 3)
	// The first event's prev_hash is ZeroHash; flip its payload so
	// recompute differs from stored.
	envs[0].Payload = append([]byte(nil), envs[0].Payload...)
	envs[0].Payload[0] ^= 0xff

	r := &fakeStreamReader{envs: envs}
	err := es.VerifyStreamChain(context.Background(), r, sid)
	if !errors.Is(err, es.ErrChainBroken) {
		t.Fatalf("want ErrChainBroken on genesis tamper, got %v", err)
	}
}

func TestVerifyStreamChain_PropagatesReadError(t *testing.T) {
	sid := mustSID(t)
	wantErr := errors.New("store: boom")
	r := &fakeStreamReader{err: wantErr}
	err := es.VerifyStreamChain(context.Background(), r, sid)
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected wrapped read error, got %v", err)
	}
}
