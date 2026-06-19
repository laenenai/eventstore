package file

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/laenenai/eventstore/kms"
)

// KeyStore implements kms.KeyStore against a JSON file on disk.
// Threading: safe for concurrent use; the mutex serializes any
// write that would persist (lazy KEK creation, rotation), readers
// take the read-half.
type KeyStore struct {
	path string

	mu   sync.RWMutex
	keks map[string][][]byte // tenantID → [v0, v1, ...]
}

// fileData is the on-disk shape. Kept separate from KeyStore so the
// JSON struct tags don't leak into the public API.
type fileData struct {
	// KEKs is keyed by tenant. Each entry is the ordered list of KEK
	// versions; index 0 is version 1. Encoded as raw byte slices —
	// encoding/json handles base64 round-tripping automatically.
	KEKs map[string][][]byte `json:"keks"`
}

// New loads or creates the KEK file at path. A missing file is fine
// on first run; subsequent runs reload the persisted state so every
// prior DEK wrapping unwraps cleanly.
func New(path string) (*KeyStore, error) {
	k := &KeyStore{
		path: path,
		keks: make(map[string][][]byte),
	}
	if err := k.load(); err != nil {
		return nil, fmt.Errorf("kms/file: load %s: %w", path, err)
	}
	return k, nil
}

// load reads the KEK file. Missing file is not an error — first run.
func (k *KeyStore) load() error {
	b, err := os.ReadFile(k.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if len(b) == 0 {
		return nil
	}
	var data fileData
	if err := json.Unmarshal(b, &data); err != nil {
		return fmt.Errorf("decode: %w", err)
	}
	if data.KEKs != nil {
		k.keks = data.KEKs
	}
	return nil
}

// save writes the KEK file via a temp-file + rename so a crash
// mid-write cannot strand the file in a half-encoded state that
// prevents the next process start.
func (k *KeyStore) save() error {
	if err := os.MkdirAll(filepath.Dir(k.path), 0o700); err != nil {
		return err
	}
	b, err := json.MarshalIndent(fileData{KEKs: k.keks}, "", "  ")
	if err != nil {
		return err
	}
	tmp := k.path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, k.path)
}

// CurrentKEKVersion implements kms.KeyStore.
func (k *KeyStore) CurrentKEKVersion(_ context.Context, tenantID string) (uint32, error) {
	k.mu.RLock()
	defer k.mu.RUnlock()
	return uint32(len(k.keks[tenantID])), nil
}

// WrapDEK implements kms.KeyStore via AES-256-GCM. Generates a
// tenant KEK lazily on first call; persists immediately so a crash
// before the caller's transaction commits cannot leave wrapped DEKs
// in the event store without a corresponding KEK on disk.
func (k *KeyStore) WrapDEK(_ context.Context, tenantID string, dek []byte) ([]byte, uint32, error) {
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
	if kekVersion == 0 {
		// "use most recent" — historic adapters that don't store the
		// version with the wrap. We don't have to support this but
		// the framework's KeyStore contract allows it; pick the
		// newest version if any.
		if len(keks) == 0 {
			return nil, fmt.Errorf("kms/file: no KEK versions for tenant %q", tenantID)
		}
		return aesGCMOpen(keks[len(keks)-1], wrapped)
	}
	if int(kekVersion) > len(keks) {
		return nil, fmt.Errorf("kms/file: KEK version %d not available for tenant %q (have %d versions)",
			kekVersion, tenantID, len(keks))
	}
	return aesGCMOpen(keks[kekVersion-1], wrapped)
}

// RotateKEK implements kms.KEKRotator. Generates a new random KEK
// and appends it as the next version for the tenant. Existing
// wrappings continue to unwrap under their stored kek_version; use
// shred.RewrapDEKs to migrate them.
func (k *KeyStore) RotateKEK(_ context.Context, tenantID string) (uint32, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	kek := make([]byte, 32)
	if _, err := rand.Read(kek); err != nil {
		return 0, fmt.Errorf("kms/file: generate KEK: %w", err)
	}
	k.keks[tenantID] = append(k.keks[tenantID], kek)
	if err := k.save(); err != nil {
		return 0, fmt.Errorf("kms/file: save after rotate: %w", err)
	}
	return uint32(len(k.keks[tenantID])), nil
}

// activeKEK returns the most-recent KEK for tenantID, lazily
// generating one (and persisting it) if none exists.
func (k *KeyStore) activeKEK(tenantID string) ([]byte, uint32, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	keks := k.keks[tenantID]
	if len(keks) > 0 {
		return keks[len(keks)-1], uint32(len(keks)), nil
	}
	kek := make([]byte, 32)
	if _, err := rand.Read(kek); err != nil {
		return nil, 0, fmt.Errorf("kms/file: generate KEK: %w", err)
	}
	k.keks[tenantID] = append(k.keks[tenantID], kek)
	if err := k.save(); err != nil {
		return nil, 0, fmt.Errorf("kms/file: save: %w", err)
	}
	return kek, 1, nil
}

// Compile-time interface assertions: this adapter implements the
// framework KeyStore contract and the optional KEKRotator extension.
var (
	_ kms.KeyStore   = (*KeyStore)(nil)
	_ kms.KEKRotator = (*KeyStore)(nil)
)

// aesGCMSeal encrypts plaintext under the 32-byte key. Output is
// nonce || ciphertext+tag, matching the wire format the framework's
// inproc adapter uses.
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
	ct := gcm.Seal(nil, nonce, plaintext, nil)
	return append(nonce, ct...), nil
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
		return nil, errors.New("kms/file: sealed payload too short")
	}
	nonce, ct := sealed[:gcm.NonceSize()], sealed[gcm.NonceSize():]
	return gcm.Open(nil, nonce, ct, nil)
}
