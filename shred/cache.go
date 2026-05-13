package shred

import "sync"

// dekCache is a process-local cache of unwrapped DEKs. ADR 0010:
// "Hot read path: DEK fetched from subject_keys, unwrapped via KMS
// once, cached, used for AEAD operations."
//
// Concurrency-safe.
type dekCache struct {
	mu   sync.RWMutex
	keys map[string][]byte // (tenant + "|" + subject) → DEK
}

func newDEKCache() *dekCache {
	return &dekCache{keys: map[string][]byte{}}
}

func cacheKey(tenantID, subject string) string {
	return tenantID + "|" + subject
}

func (c *dekCache) get(tenantID, subject string) ([]byte, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	v, ok := c.keys[cacheKey(tenantID, subject)]
	return v, ok
}

func (c *dekCache) set(tenantID, subject string, dek []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	cp := append([]byte(nil), dek...)
	c.keys[cacheKey(tenantID, subject)] = cp
}

func (c *dekCache) forget(tenantID, subject string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.keys, cacheKey(tenantID, subject))
}
