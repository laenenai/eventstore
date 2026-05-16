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

// fakeStreamChainRebuilder extends fakeStreamReader with an in-memory
// BackfillEventHash that mimics the adapter's "WHERE hash IS NULL"
// guard. Used to drive RebuildStreamChain unit tests without a real DB.
type fakeStreamChainRebuilder struct {
	envs []es.Envelope
	err  error

	// backfills records each call for assertions.
	backfills []backfillCall
	// failBackfill, when non-nil, is returned from BackfillEventHash.
	failBackfill error
}

type backfillCall struct {
	tenantID string
	eventID  uuid.UUID
	hash     []byte
	prevHash []byte
}

func (f *fakeStreamChainRebuilder) ReadStream(_ context.Context, _ es.StreamID, _ uint64) ([]es.Envelope, error) {
	return f.envs, f.err
}

func (f *fakeStreamChainRebuilder) BackfillEventHash(_ context.Context, tenantID string, eventID uuid.UUID, hash, prevHash []byte) error {
	if f.failBackfill != nil {
		return f.failBackfill
	}
	// Find the envelope and apply the same `IS NULL` semantics the SQL
	// guard would. Rows that already have a hash are a no-op + error.
	for i := range f.envs {
		if f.envs[i].EventID != eventID || f.envs[i].TenantID != tenantID {
			continue
		}
		if len(f.envs[i].Hash) != 0 {
			return errors.New("fake: row already has a hash")
		}
		f.envs[i].Hash = hash
		f.envs[i].PrevHash = prevHash
		f.backfills = append(f.backfills, backfillCall{
			tenantID: tenantID,
			eventID:  eventID,
			hash:     hash,
			prevHash: prevHash,
		})
		return nil
	}
	return errors.New("fake: event not found")
}

// stripChain returns the envelopes with Hash and PrevHash cleared,
// simulating pre-migration rows.
func stripChain(envs []es.Envelope) []es.Envelope {
	out := make([]es.Envelope, len(envs))
	for i := range envs {
		out[i] = envs[i]
		out[i].Hash = nil
		out[i].PrevHash = nil
	}
	return out
}

func TestRebuildStreamChain_BackfillsNullHashes(t *testing.T) {
	sid := mustSID(t)
	// Build a properly chained slice, then strip the chain columns to
	// simulate the pre-migration on-disk state.
	chained := buildChained(t, sid, 4)
	pre := stripChain(chained)

	r := &fakeStreamChainRebuilder{envs: pre}
	res, err := es.RebuildStreamChain(context.Background(), r, sid)
	if err != nil {
		t.Fatalf("RebuildStreamChain: %v", err)
	}
	if res.BackfilledCount != 4 || res.VerifiedCount != 0 {
		t.Errorf("counts: got backfilled=%d verified=%d; want backfilled=4 verified=0",
			res.BackfilledCount, res.VerifiedCount)
	}
	if len(r.backfills) != 4 {
		t.Fatalf("backfill calls: got %d, want 4", len(r.backfills))
	}
	// Every backfilled hash must match the canonical chained value.
	for i := range chained {
		if string(r.envs[i].Hash) != string(chained[i].Hash) {
			t.Errorf("v%d hash mismatch:\n  got  %x\n  want %x",
				chained[i].Version, r.envs[i].Hash, chained[i].Hash)
		}
		if string(r.envs[i].PrevHash) != string(chained[i].PrevHash) {
			t.Errorf("v%d prev_hash mismatch", chained[i].Version)
		}
	}
}

func TestRebuildStreamChain_VerifiesExistingHashes(t *testing.T) {
	sid := mustSID(t)
	chained := buildChained(t, sid, 3)
	r := &fakeStreamChainRebuilder{envs: chained}
	res, err := es.RebuildStreamChain(context.Background(), r, sid)
	if err != nil {
		t.Fatalf("RebuildStreamChain on already-chained: %v", err)
	}
	if res.BackfilledCount != 0 || res.VerifiedCount != 3 {
		t.Errorf("counts: got backfilled=%d verified=%d; want backfilled=0 verified=3",
			res.BackfilledCount, res.VerifiedCount)
	}
	if len(r.backfills) != 0 {
		t.Errorf("expected no backfill writes, got %d", len(r.backfills))
	}
}

func TestRebuildStreamChain_DetectsTampering(t *testing.T) {
	sid := mustSID(t)
	chained := buildChained(t, sid, 3)
	// Flip a byte in the second event's payload. The stored hash now
	// disagrees with the recompute, but the row still has a non-NULL
	// hash so RebuildStreamChain treats this as a verify mismatch.
	chained[1].Payload = append([]byte(nil), chained[1].Payload...)
	chained[1].Payload[0] ^= 0xff

	r := &fakeStreamChainRebuilder{envs: chained}
	_, err := es.RebuildStreamChain(context.Background(), r, sid)
	if !errors.Is(err, es.ErrChainBroken) {
		t.Fatalf("want ErrChainBroken on tampered row, got %v", err)
	}
}

func TestRebuildStreamChain_EmptyStream(t *testing.T) {
	sid := mustSID(t)
	r := &fakeStreamChainRebuilder{envs: nil}
	res, err := es.RebuildStreamChain(context.Background(), r, sid)
	if err != nil {
		t.Fatalf("empty stream should return no error: %v", err)
	}
	if res.BackfilledCount != 0 || res.VerifiedCount != 0 {
		t.Errorf("counts: got backfilled=%d verified=%d; want both 0",
			res.BackfilledCount, res.VerifiedCount)
	}
}

func TestRebuildStreamChain_PartialBackfill(t *testing.T) {
	sid := mustSID(t)
	chained := buildChained(t, sid, 5)
	// Mixed state: first three events still NULL (pre-migration),
	// last two already chained (events appended after the migration).
	mixed := make([]es.Envelope, 5)
	for i := 0; i < 5; i++ {
		mixed[i] = chained[i]
		if i < 3 {
			mixed[i].Hash = nil
			mixed[i].PrevHash = nil
		}
	}

	r := &fakeStreamChainRebuilder{envs: mixed}
	res, err := es.RebuildStreamChain(context.Background(), r, sid)
	if err != nil {
		t.Fatalf("RebuildStreamChain: %v", err)
	}
	if res.BackfilledCount != 3 || res.VerifiedCount != 2 {
		t.Errorf("counts: got backfilled=%d verified=%d; want backfilled=3 verified=2",
			res.BackfilledCount, res.VerifiedCount)
	}
	// After the rebuild the in-memory store should chain end-to-end.
	if err := es.VerifyStreamChain(context.Background(), r, sid); err != nil {
		t.Errorf("post-rebuild VerifyStreamChain: %v", err)
	}
}

func TestRebuildStreamChain_PropagatesReadError(t *testing.T) {
	sid := mustSID(t)
	wantErr := errors.New("store: boom")
	r := &fakeStreamChainRebuilder{err: wantErr}
	_, err := es.RebuildStreamChain(context.Background(), r, sid)
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected wrapped read error, got %v", err)
	}
}

func TestRebuildStreamChain_PropagatesBackfillError(t *testing.T) {
	sid := mustSID(t)
	pre := stripChain(buildChained(t, sid, 2))
	wantErr := errors.New("backfill: boom")
	r := &fakeStreamChainRebuilder{envs: pre, failBackfill: wantErr}
	_, err := es.RebuildStreamChain(context.Background(), r, sid)
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected wrapped backfill error, got %v", err)
	}
}

func TestRebuildStreamChain_Idempotent(t *testing.T) {
	sid := mustSID(t)
	pre := stripChain(buildChained(t, sid, 3))
	r := &fakeStreamChainRebuilder{envs: pre}
	// First pass fills in the chain.
	if _, err := es.RebuildStreamChain(context.Background(), r, sid); err != nil {
		t.Fatalf("first pass: %v", err)
	}
	// Second pass must be a pure-verify no-op.
	res, err := es.RebuildStreamChain(context.Background(), r, sid)
	if err != nil {
		t.Fatalf("second pass: %v", err)
	}
	if res.BackfilledCount != 0 || res.VerifiedCount != 3 {
		t.Errorf("second pass counts: got backfilled=%d verified=%d; want backfilled=0 verified=3",
			res.BackfilledCount, res.VerifiedCount)
	}
}
