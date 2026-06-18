package conversations_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"path/filepath"
	"testing"

	"github.com/laenenai/eventstore/examples/conversations"
)

// TestFileKMS_PersistsAcrossRestart proves the property the chat CLI
// depends on: a wrapped DEK produced by one FileKMS instance must
// still unwrap correctly via a fresh FileKMS that loads the same
// sidecar file. This is the scenario the user hit running the CLI
// twice in a row: process 1 wrapped under KEK v1, process 2 booted
// with an empty in-memory map and the unwrap blew up.
func TestFileKMS_PersistsAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "kms.json")
	ctx := context.Background()
	tenant := "acme"

	// Process 1 — generate a DEK, wrap it, persist to disk.
	first, err := conversations.NewFileKMS(path)
	if err != nil {
		t.Fatalf("first NewFileKMS: %v", err)
	}
	dek := make([]byte, 32)
	if _, err := rand.Read(dek); err != nil {
		t.Fatalf("rand: %v", err)
	}
	wrapped, version, err := first.WrapDEK(ctx, tenant, dek)
	if err != nil {
		t.Fatalf("WrapDEK: %v", err)
	}
	if version != 1 {
		t.Fatalf("first version: got %d want 1", version)
	}

	// Process 2 — fresh KeyStore against the same file. The wrapped
	// DEK from process 1 must round-trip.
	second, err := conversations.NewFileKMS(path)
	if err != nil {
		t.Fatalf("second NewFileKMS: %v", err)
	}
	unwrapped, err := second.UnwrapDEK(ctx, tenant, wrapped, version)
	if err != nil {
		t.Fatalf("UnwrapDEK across restart: %v", err)
	}
	if !bytes.Equal(unwrapped, dek) {
		t.Fatalf("unwrapped DEK differs from original")
	}

	got, err := second.CurrentKEKVersion(ctx, tenant)
	if err != nil {
		t.Fatalf("CurrentKEKVersion: %v", err)
	}
	if got != 1 {
		t.Errorf("CurrentKEKVersion after reload: got %d want 1", got)
	}
}

func TestFileKMS_UnknownVersionFails(t *testing.T) {
	path := filepath.Join(t.TempDir(), "kms.json")
	k, err := conversations.NewFileKMS(path)
	if err != nil {
		t.Fatalf("NewFileKMS: %v", err)
	}
	// No WrapDEK call → no KEK versions on disk yet.
	_, err = k.UnwrapDEK(context.Background(), "acme", []byte("garbage"), 1)
	if err == nil {
		t.Fatalf("expected UnwrapDEK to fail with no KEK versions")
	}
}
