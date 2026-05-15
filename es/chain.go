package es

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	envelopev1 "github.com/laenenai/eventstore/gen/eventstore/envelope/v1"
)

// ZeroHash is the prev_hash of the genesis event in any stream
// (version=1). 32 zero bytes.
var ZeroHash = make([]byte, sha256.Size)

// ErrChainBroken reports that an event's stored hash does not match
// the hash recomputed from its content + predecessor. Returned by
// VerifyStreamChain with a wrapper that names the offending version.
var ErrChainBroken = errors.New("eventstore: chain hash mismatch")

// ComputeChainHash returns the per-stream chain hash for an event.
// SHA-256 over (prevHash || canonical(envelope-minus-hash-fields)).
//
// "canonical" is the proto deterministic-marshal of an envelopev1.Envelope
// built from e, with the commit-time fields (recorded_at, global_position)
// AND the hash fields (hash, prev_hash) cleared. The hash subset is
// pinned by ADR 0028; see the ADR for the rationale.
//
// Called by storage adapters at append time. Callers SHOULD NOT mutate
// e — this function only reads.
func ComputeChainHash(prevHash []byte, e *Envelope) ([]byte, error) {
	if e == nil {
		return nil, errors.New("eventstore: nil envelope")
	}
	if len(prevHash) != sha256.Size {
		return nil, fmt.Errorf("eventstore: prev hash must be %d bytes, got %d",
			sha256.Size, len(prevHash))
	}
	canonical, err := canonicalEnvelopeBytes(e)
	if err != nil {
		return nil, err
	}
	h := sha256.New()
	_, _ = h.Write(prevHash)
	_, _ = h.Write(canonical)
	return h.Sum(nil), nil
}

// canonicalEnvelopeBytes returns the deterministic proto serialization
// of the hash-subset of the envelope. Commit-time fields and the hash
// fields are excluded.
func canonicalEnvelopeBytes(e *Envelope) ([]byte, error) {
	pb := &envelopev1.Envelope{
		EventId:       e.EventID.String(),
		TenantId:      e.TenantID,
		StreamId:      e.StreamID.Canonical(),
		Version:       e.Version,
		TypeUrl:       e.TypeURL,
		SchemaVersion: e.SchemaVersion,
		OccurredAt:    timestamppb.New(e.OccurredAt.UTC()),
		CorrelationId: e.CorrelationID.String(),
		CausationId:   e.CausationID.String(),
		CommandId:     e.CommandID.String(),
		Actor: &envelopev1.Actor{
			Kind:       envelopev1.Actor_Kind(e.Actor.Kind),
			Principal:  e.Actor.Principal,
			OnBehalfOf: e.Actor.OnBehalfOf,
			ApiKeyId:   e.Actor.APIKeyID,
			Attributes: e.Actor.Attributes,
		},
		Payload:           e.Payload,
		EncryptionKeyRefs: e.KeyRefs,
		// GlobalPosition, RecordedAt, Hash, PrevHash deliberately omitted —
		// see ADR 0028 § 2 (canonical serialization) and § "hash includes
		// recorded_at and global_position" (alternative considered).
	}
	return proto.MarshalOptions{Deterministic: true}.Marshal(pb)
}

// VerifyStreamChain replays the chain for a single stream, recomputing
// each event's hash from its content + predecessor and comparing to
// the stored hash. Returns nil if the chain is intact, ErrChainBroken
// (wrapped with the offending version) on mismatch, or another error
// for store / encoding failures.
//
// The store must yield events in version order (which es.Store
// guarantees per ReadStream). Streams with no events return nil.
//
// O(stream length); auditors run this per-stream on the cadence they
// choose. See cookbook recipes 19 + 20 for periodic and external-witness
// extensions.
func VerifyStreamChain(ctx context.Context, store StreamReader, sid StreamID) error {
	envs, err := store.ReadStream(ctx, sid, 0)
	if err != nil {
		return err
	}
	prev := ZeroHash
	for i := range envs {
		got, err := ComputeChainHash(prev, &envs[i])
		if err != nil {
			return fmt.Errorf("recompute hash at version %d: %w", envs[i].Version, err)
		}
		if !equalHash(got, envs[i].Hash) {
			return fmt.Errorf("%w: version %d", ErrChainBroken, envs[i].Version)
		}
		prev = envs[i].Hash
	}
	return nil
}

// StreamReader is the minimal store surface VerifyStreamChain needs.
// es.Store satisfies it; tests can substitute a fake.
type StreamReader interface {
	ReadStream(ctx context.Context, sid StreamID, afterVersion uint64) ([]Envelope, error)
}

func equalHash(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	var diff byte
	for i := range a {
		diff |= a[i] ^ b[i]
	}
	return diff == 0
}

// Compile-time check: ensure crypto/sha256 is the hash we wire here,
// in case anyone factors out the New function.
var _ hash.Hash = sha256.New()
