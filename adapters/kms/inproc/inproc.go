package inproc

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"fmt"
	"sync"

	"github.com/laenenai/eventstore/kms"
)

// KeyStore is an in-process kms.KeyStore. Each tenant has a randomly
// generated AES-256 KEK held in memory. AES-256-GCM wraps DEKs.
//
// Not for production multi-tenant use: KEKs in memory survive only
// for the process lifetime, so DEKs become unreadable across restarts
// unless the keys are externally pinned (e.g., via SetKEK). Useful
// for tests, SQLite dev, and single-tenant single-binary deployments
// where key material lives in operator-controlled secrets.
type KeyStore struct {
	mu   sync.RWMutex
	keks map[string][][]byte // tenantID → [v0, v1, ...]
}

// New returns an empty in-process KeyStore. Tenants are created lazily
// on first WrapDEK; SetKEK overrides for tests.
func New() *KeyStore {
	return &KeyStore{keks: map[string][][]byte{}}
}

// SetKEK injects an exact 32-byte AES-256 KEK as the next version for
// the given tenant. Useful for tests with deterministic key material,
// or for operators pinning KEKs from external secrets.
func (k *KeyStore) SetKEK(tenantID string, kek []byte) error {
	if len(kek) != 32 {
		return fmt.Errorf("inproc kms: KEK must be 32 bytes, got %d", len(kek))
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	k.keks[tenantID] = append(k.keks[tenantID], append([]byte(nil), kek...))
	return nil
}

// RotateKEK generates a new random KEK and appends it as the next
// version for the tenant. Implements kms.KEKRotator. Subsequent
// WrapDEK calls use the new KEK; old wrappings still unwrap under
// their stored kek_version. Use shred.RewrapDEKs to migrate
// historical DEKs after rotation.
func (k *KeyStore) RotateKEK(_ context.Context, tenantID string) (uint32, error) {
	kek := make([]byte, 32)
	if _, err := rand.Read(kek); err != nil {
		return 0, fmt.Errorf("inproc kms: rotate KEK: %w", err)
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	k.keks[tenantID] = append(k.keks[tenantID], kek)
	return uint32(len(k.keks[tenantID])), nil
}

// CurrentKEKVersion implements kms.KeyStore.
func (k *KeyStore) CurrentKEKVersion(_ context.Context, tenantID string) (uint32, error) {
	k.mu.RLock()
	defer k.mu.RUnlock()
	v := uint32(len(k.keks[tenantID]))
	if v == 0 {
		return 0, nil
	}
	return v, nil
}

// WrapDEK implements kms.KeyStore via AES-256-GCM.
func (k *KeyStore) WrapDEK(ctx context.Context, tenantID string, dek []byte) ([]byte, uint32, error) {
	kek, version, err := k.activeKEK(tenantID)
	if err != nil {
		return nil, 0, err
	}
	wrapped, err := aesGCMSeal(kek, dek)
	if err != nil {
		return nil, 0, err
	}
	return wrapped, version, nil
}

// UnwrapDEK implements kms.KeyStore.
func (k *KeyStore) UnwrapDEK(_ context.Context, tenantID string, wrapped []byte, kekVersion uint32) ([]byte, error) {
	k.mu.RLock()
	keks := k.keks[tenantID]
	k.mu.RUnlock()

	if kekVersion == 0 || int(kekVersion) > len(keks) {
		return nil, errors.New("inproc kms: KEK version not available")
	}
	return aesGCMOpen(keks[kekVersion-1], wrapped)
}

// activeKEK returns the most-recent KEK for tenantID, lazily generating
// one if none exists.
func (k *KeyStore) activeKEK(tenantID string) ([]byte, uint32, error) {
	k.mu.RLock()
	keks := k.keks[tenantID]
	k.mu.RUnlock()
	if len(keks) > 0 {
		return keks[len(keks)-1], uint32(len(keks)), nil
	}

	// Lazily create a KEK for this tenant.
	kek := make([]byte, 32)
	if _, err := rand.Read(kek); err != nil {
		return nil, 0, fmt.Errorf("inproc kms: generate KEK: %w", err)
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	// Re-check under write lock in case a concurrent call created it.
	if existing := k.keks[tenantID]; len(existing) > 0 {
		return existing[len(existing)-1], uint32(len(existing)), nil
	}
	k.keks[tenantID] = [][]byte{kek}
	return kek, 1, nil
}

// aesGCMSeal encrypts plaintext under key (32 bytes) using AES-256-GCM
// with a fresh 12-byte nonce. Output: nonce || ciphertext || tag.
func aesGCMSeal(key, plaintext []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	out := gcm.Seal(nonce, nonce, plaintext, nil)
	return out, nil
}

// aesGCMOpen reverses aesGCMSeal.
func aesGCMOpen(key, sealed []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	if len(sealed) < gcm.NonceSize() {
		return nil, errors.New("inproc kms: ciphertext shorter than nonce")
	}
	nonce, ct := sealed[:gcm.NonceSize()], sealed[gcm.NonceSize():]
	return gcm.Open(nil, nonce, ct, nil)
}

// Compile-time check.
var (
	_ kms.KeyStore   = (*KeyStore)(nil)
	_ kms.KEKRotator = (*KeyStore)(nil)
)
