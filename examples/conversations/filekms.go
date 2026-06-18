package conversations

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
)

// FileKMS is a development-only kms.KeyStore whose KEK bytes are
// persisted to a sidecar JSON file. Exists because the framework's
// inproc.KeyStore keeps KEKs in memory only — perfect for one-shot
// tests, fatal for a runnable CLI that the user expects to survive
// restarts (subject_keys rows persist in SQLite, so their wrapped
// DEKs reference KEK versions that vanish when the process exits).
//
// Threat model: the KEK file is a flat secret. Anyone with read
// access to the file can decrypt every wrapped DEK and from there
// every PII field in the event log. Use this ONLY for local
// development with your own data. Production deployments wire AWS
// KMS, GCP KMS, Vault, or another HSM-backed adapter.
//
// File format: a small JSON object keyed by tenant, value an ordered
// array of base64-encoded 32-byte KEK versions. New KEKs append; old
// versions are retained forever so historical wrappings stay
// decryptable.
type FileKMS struct {
	path string

	mu   sync.RWMutex
	keks map[string][][]byte // tenantID → [v0, v1, ...]
}

// fileKMSData is the on-disk shape. Kept separate from FileKMS so the
// JSON tags don't leak into the public API.
type fileKMSData struct {
	// KEKs is keyed by tenant. Each entry is the ordered list of KEK
	// versions; index 0 is version 1. Encoded as raw byte slices —
	// encoding/json handles base64 round-tripping automatically.
	KEKs map[string][][]byte `json:"keks"`
}

// NewFileKMS loads or creates the KEK file at path. A missing file is
// fine on first run; subsequent runs reload the persisted state so
// every prior DEK wrapping unwraps cleanly.
func NewFileKMS(path string) (*FileKMS, error) {
	k := &FileKMS{
		path: path,
		keks: make(map[string][][]byte),
	}
	if err := k.load(); err != nil {
		return nil, fmt.Errorf("filekms: load %s: %w", path, err)
	}
	return k, nil
}

// load reads the KEK file. Missing file is not an error — first run.
func (k *FileKMS) load() error {
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
	var data fileKMSData
	if err := json.Unmarshal(b, &data); err != nil {
		return fmt.Errorf("decode: %w", err)
	}
	if data.KEKs != nil {
		k.keks = data.KEKs
	}
	return nil
}

// save writes the KEK file. Uses a temp-file + rename so a crash
// mid-write cannot leave a half-encoded file that prevents the next
// run from starting.
func (k *FileKMS) save() error {
	if err := os.MkdirAll(filepath.Dir(k.path), 0o700); err != nil {
		return err
	}
	b, err := json.MarshalIndent(fileKMSData{KEKs: k.keks}, "", "  ")
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
func (k *FileKMS) CurrentKEKVersion(_ context.Context, tenantID string) (uint32, error) {
	k.mu.RLock()
	defer k.mu.RUnlock()
	return uint32(len(k.keks[tenantID])), nil
}

// WrapDEK implements kms.KeyStore via AES-256-GCM. Generates a tenant
// KEK lazily on first call; persists immediately so a crash before the
// caller's transaction commits cannot leave wrapped DEKs in the event
// store without a corresponding KEK on disk.
func (k *FileKMS) WrapDEK(_ context.Context, tenantID string, dek []byte) ([]byte, uint32, error) {
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
func (k *FileKMS) UnwrapDEK(_ context.Context, tenantID string, wrapped []byte, kekVersion uint32) ([]byte, error) {
	k.mu.RLock()
	keks := k.keks[tenantID]
	k.mu.RUnlock()
	if kekVersion == 0 || int(kekVersion) > len(keks) {
		return nil, fmt.Errorf("filekms: KEK version %d not available for tenant %q (have %d versions)",
			kekVersion, tenantID, len(keks))
	}
	return aesGCMOpen(keks[kekVersion-1], wrapped)
}

// activeKEK returns the most-recent KEK for tenantID, lazily generating
// one (and persisting it) if none exists.
func (k *FileKMS) activeKEK(tenantID string) ([]byte, uint32, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	keks := k.keks[tenantID]
	if len(keks) > 0 {
		return keks[len(keks)-1], uint32(len(keks)), nil
	}
	kek := make([]byte, 32)
	if _, err := rand.Read(kek); err != nil {
		return nil, 0, fmt.Errorf("generate KEK: %w", err)
	}
	k.keks[tenantID] = append(k.keks[tenantID], kek)
	if err := k.save(); err != nil {
		return nil, 0, fmt.Errorf("save: %w", err)
	}
	return kek, 1, nil
}

// aesGCMSeal encrypts plaintext under the 32-byte key. The output is
// nonce || ciphertext || tag, matching the wire format used elsewhere
// in the framework.
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
		return nil, errors.New("filekms: sealed payload too short")
	}
	nonce, ct := sealed[:gcm.NonceSize()], sealed[gcm.NonceSize():]
	return gcm.Open(nil, nonce, ct, nil)
}
