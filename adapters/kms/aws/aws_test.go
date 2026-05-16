package aws

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	awskms "github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/aws/aws-sdk-go-v2/service/kms/types"
)

// fakeKMS is an in-memory stand-in for the AWS KMS API surface we use.
// Ciphertext blobs are framed as "len(keyID) || keyID || plaintext"
// so Decrypt can recover the key without needing real crypto. This
// mirrors AWS's invariant that the ciphertext blob carries enough
// metadata to identify the key without the caller providing it.
type fakeKMS struct {
	mu      sync.Mutex
	keys    map[string][]byte // keyId → "dummy material" (unused; presence signals existence)
	aliases map[string]string // alias name → keyId
	nextKey int               // monotonic counter for new KeyIds

	// Hooks for fault injection in specific tests.
	encryptErr     func(keyID string) error
	decryptErr     func(keyID string) error
	failCreateKey  bool
	failCreateName bool
}

func newFakeKMS() *fakeKMS {
	return &fakeKMS{
		keys:    map[string][]byte{},
		aliases: map[string]string{},
	}
}

// resolveKeyID dereferences an alias if the given identifier starts
// with "alias/"; otherwise treats it as a KeyId. Returns NotFound when
// the alias is unknown.
func (f *fakeKMS) resolveKeyID(id string) (string, error) {
	if strings.HasPrefix(id, "alias/") {
		k, ok := f.aliases[id]
		if !ok {
			return "", &types.NotFoundException{Message: awssdk.String("alias " + id + " not found")}
		}
		return k, nil
	}
	if _, ok := f.keys[id]; !ok {
		return "", &types.NotFoundException{Message: awssdk.String("key " + id + " not found")}
	}
	return id, nil
}

func (f *fakeKMS) CreateKey(_ context.Context, _ *awskms.CreateKeyInput, _ ...func(*awskms.Options)) (*awskms.CreateKeyOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failCreateKey {
		return nil, errors.New("fake: CreateKey forced failure")
	}
	f.nextKey++
	keyID := fmt.Sprintf("key-%04d", f.nextKey)
	material := make([]byte, 8)
	_, _ = rand.Read(material)
	f.keys[keyID] = material
	return &awskms.CreateKeyOutput{
		KeyMetadata: &types.KeyMetadata{KeyId: awssdk.String(keyID)},
	}, nil
}

func (f *fakeKMS) CreateAlias(_ context.Context, in *awskms.CreateAliasInput, _ ...func(*awskms.Options)) (*awskms.CreateAliasOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failCreateName {
		return nil, errors.New("fake: CreateAlias forced failure")
	}
	alias := awssdk.ToString(in.AliasName)
	target := awssdk.ToString(in.TargetKeyId)
	if _, exists := f.aliases[alias]; exists {
		return nil, &types.AlreadyExistsException{Message: awssdk.String("alias exists")}
	}
	if _, ok := f.keys[target]; !ok {
		return nil, &types.NotFoundException{Message: awssdk.String("target key not found")}
	}
	f.aliases[alias] = target
	return &awskms.CreateAliasOutput{}, nil
}

func (f *fakeKMS) UpdateAlias(_ context.Context, in *awskms.UpdateAliasInput, _ ...func(*awskms.Options)) (*awskms.UpdateAliasOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	alias := awssdk.ToString(in.AliasName)
	target := awssdk.ToString(in.TargetKeyId)
	if _, ok := f.aliases[alias]; !ok {
		return nil, &types.NotFoundException{Message: awssdk.String("alias not found")}
	}
	if _, ok := f.keys[target]; !ok {
		return nil, &types.NotFoundException{Message: awssdk.String("target key not found")}
	}
	f.aliases[alias] = target
	return &awskms.UpdateAliasOutput{}, nil
}

func (f *fakeKMS) DescribeKey(_ context.Context, in *awskms.DescribeKeyInput, _ ...func(*awskms.Options)) (*awskms.DescribeKeyOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	id, err := f.resolveKeyID(awssdk.ToString(in.KeyId))
	if err != nil {
		return nil, err
	}
	return &awskms.DescribeKeyOutput{
		KeyMetadata: &types.KeyMetadata{KeyId: awssdk.String(id)},
	}, nil
}

func (f *fakeKMS) Encrypt(_ context.Context, in *awskms.EncryptInput, _ ...func(*awskms.Options)) (*awskms.EncryptOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	id, err := f.resolveKeyID(awssdk.ToString(in.KeyId))
	if err != nil {
		return nil, err
	}
	if f.encryptErr != nil {
		if e := f.encryptErr(id); e != nil {
			return nil, e
		}
	}
	// Frame: u16 len(keyID) || keyID || plaintext.
	var buf bytes.Buffer
	idBytes := []byte(id)
	if err := binary.Write(&buf, binary.BigEndian, uint16(len(idBytes))); err != nil {
		return nil, err
	}
	buf.Write(idBytes)
	buf.Write(in.Plaintext)
	return &awskms.EncryptOutput{
		CiphertextBlob: buf.Bytes(),
		KeyId:          awssdk.String(id),
	}, nil
}

func (f *fakeKMS) Decrypt(_ context.Context, in *awskms.DecryptInput, _ ...func(*awskms.Options)) (*awskms.DecryptOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	blob := in.CiphertextBlob
	if len(blob) < 2 {
		return nil, errors.New("fake: ciphertext too short")
	}
	idLen := binary.BigEndian.Uint16(blob[:2])
	if len(blob) < int(2+idLen) {
		return nil, errors.New("fake: ciphertext truncated")
	}
	id := string(blob[2 : 2+idLen])
	plaintext := blob[2+idLen:]
	if _, ok := f.keys[id]; !ok {
		// Mirrors AWS behaviour when the key referenced by the
		// blob has been deleted.
		return nil, &types.KMSInvalidStateException{Message: awssdk.String("key " + id + " not found")}
	}
	if in.KeyId != nil && awssdk.ToString(in.KeyId) != id {
		// AWS rejects with IncorrectKeyException when the
		// caller-provided KeyId doesn't match the blob's
		// embedded KeyId.
		return nil, &types.IncorrectKeyException{Message: awssdk.String("blob keyId " + id + " != caller " + awssdk.ToString(in.KeyId))}
	}
	if f.decryptErr != nil {
		if e := f.decryptErr(id); e != nil {
			return nil, e
		}
	}
	return &awskms.DecryptOutput{
		Plaintext: append([]byte(nil), plaintext...),
		KeyId:     awssdk.String(id),
	}, nil
}

func newKS(t *testing.T) (*KeyStore, *fakeKMS) {
	t.Helper()
	fake := newFakeKMS()
	ks, err := New(Config{Client: fake})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return ks, fake
}

// --- tests -----------------------------------------------------------------

func TestNew_RequiresClient(t *testing.T) {
	if _, err := New(Config{}); err == nil {
		t.Fatal("expected error for nil client")
	}
}

// Round-trip: wrap then unwrap returns the original DEK.
func TestWrapUnwrap_RoundTrip(t *testing.T) {
	ctx := context.Background()
	ks, _ := newKS(t)

	dek := []byte("32-byte-dek-aaaaaaaaaaaaaaaaaaaa")
	wrapped, version, err := ks.WrapDEK(ctx, "tenant-a", dek)
	if err != nil {
		t.Fatalf("WrapDEK: %v", err)
	}
	if version != 1 {
		t.Errorf("first wrap should be version 1, got %d", version)
	}

	got, err := ks.UnwrapDEK(ctx, "tenant-a", wrapped, version)
	if err != nil {
		t.Fatalf("UnwrapDEK: %v", err)
	}
	if !bytes.Equal(got, dek) {
		t.Errorf("round-trip mismatch: got %x want %x", got, dek)
	}
}

// First WrapDEK must auto-create CMK + alias for a new tenant.
func TestWrapDEK_AutoCreatesTenantCMK(t *testing.T) {
	ctx := context.Background()
	ks, fake := newKS(t)

	if v, err := ks.CurrentKEKVersion(ctx, "tenant-new"); err != nil || v != 0 {
		t.Fatalf("expected version 0 for unknown tenant, got v=%d err=%v", v, err)
	}

	dek := []byte("dek-1234567890abcdef1234567890ab")
	_, version, err := ks.WrapDEK(ctx, "tenant-new", dek)
	if err != nil {
		t.Fatalf("WrapDEK: %v", err)
	}
	if version != 1 {
		t.Fatalf("expected version 1 after auto-create, got %d", version)
	}

	wantAlias := "alias/eventstore-tenant-new"
	fake.mu.Lock()
	_, hasAlias := fake.aliases[wantAlias]
	keyCount := len(fake.keys)
	fake.mu.Unlock()
	if !hasAlias {
		t.Errorf("expected alias %q to exist", wantAlias)
	}
	if keyCount != 1 {
		t.Errorf("expected exactly 1 CMK to be created, got %d", keyCount)
	}
}

// After rotation: new wraps use the new KEK, but old wrappings still
// unwrap under their original version.
func TestRotateKEK_NewVersionAndOldStillUnwraps(t *testing.T) {
	ctx := context.Background()
	ks, fake := newKS(t)
	tenant := "tenant-rot"

	dek1 := []byte("dek-v1-aaaaaaaaaaaaaaaaaaaaaaaa")
	wrapped1, v1, err := ks.WrapDEK(ctx, tenant, dek1)
	if err != nil {
		t.Fatalf("WrapDEK v1: %v", err)
	}
	if v1 != 1 {
		t.Fatalf("expected v1=1, got %d", v1)
	}

	v2Got, err := ks.RotateKEK(ctx, tenant)
	if err != nil {
		t.Fatalf("RotateKEK: %v", err)
	}
	if v2Got != 2 {
		t.Errorf("expected new version 2, got %d", v2Got)
	}

	dek2 := []byte("dek-v2-bbbbbbbbbbbbbbbbbbbbbbbb")
	wrapped2, v2, err := ks.WrapDEK(ctx, tenant, dek2)
	if err != nil {
		t.Fatalf("WrapDEK v2: %v", err)
	}
	if v2 != 2 {
		t.Errorf("post-rotation WrapDEK should report version 2, got %d", v2)
	}

	// Old wrapping still decrypts under its original version.
	got1, err := ks.UnwrapDEK(ctx, tenant, wrapped1, v1)
	if err != nil {
		t.Fatalf("UnwrapDEK v1 after rotation: %v", err)
	}
	if !bytes.Equal(got1, dek1) {
		t.Errorf("post-rotation unwrap-v1 mismatch")
	}

	// New wrapping decrypts under the new version.
	got2, err := ks.UnwrapDEK(ctx, tenant, wrapped2, v2)
	if err != nil {
		t.Fatalf("UnwrapDEK v2: %v", err)
	}
	if !bytes.Equal(got2, dek2) {
		t.Errorf("post-rotation unwrap-v2 mismatch")
	}

	// Sanity: two distinct CMKs now exist for this tenant.
	fake.mu.Lock()
	keyCount := len(fake.keys)
	fake.mu.Unlock()
	if keyCount != 2 {
		t.Errorf("expected 2 CMKs after rotation, got %d", keyCount)
	}

	if cur, err := ks.CurrentKEKVersion(ctx, tenant); err != nil || cur != 2 {
		t.Errorf("CurrentKEKVersion after rotation: got %d err %v", cur, err)
	}
}

// Unwrapping for a tenant with no registered key fails cleanly rather
// than silently calling AWS with garbage.
func TestUnwrapDEK_UnknownTenant(t *testing.T) {
	ctx := context.Background()
	ks, _ := newKS(t)

	_, err := ks.UnwrapDEK(ctx, "tenant-never-wrapped", []byte("blob"), 1)
	if err == nil {
		t.Fatal("expected error unwrapping for unknown tenant")
	}
	if !strings.Contains(err.Error(), "not available") {
		t.Errorf("expected 'not available' error, got %v", err)
	}
}

// UnwrapDEK with kekVersion==0 is invalid (the framework always
// records the version returned by WrapDEK, which is >=1).
func TestUnwrapDEK_VersionZeroIsRejected(t *testing.T) {
	ctx := context.Background()
	ks, _ := newKS(t)
	_, _, err := ks.WrapDEK(ctx, "tenant-z", []byte("dek-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"))
	if err != nil {
		t.Fatalf("WrapDEK: %v", err)
	}
	if _, err := ks.UnwrapDEK(ctx, "tenant-z", []byte("blob"), 0); err == nil {
		t.Fatal("expected error for kekVersion=0")
	}
}

// Cross-tenant isolation: a DEK wrapped under tenant A's key cannot be
// decrypted under tenant B's key, even at the same version number.
func TestCrossTenantIsolation(t *testing.T) {
	ctx := context.Background()
	ks, _ := newKS(t)

	dekA := []byte("tenant-a-dek-aaaaaaaaaaaaaaaaaaaa")
	wrappedA, vA, err := ks.WrapDEK(ctx, "tenant-a", dekA)
	if err != nil {
		t.Fatalf("WrapDEK A: %v", err)
	}

	dekB := []byte("tenant-b-dek-bbbbbbbbbbbbbbbbbbbb")
	_, vB, err := ks.WrapDEK(ctx, "tenant-b", dekB)
	if err != nil {
		t.Fatalf("WrapDEK B: %v", err)
	}
	if vA != 1 || vB != 1 {
		t.Fatalf("expected both first wraps to be version 1, got vA=%d vB=%d", vA, vB)
	}

	// Decrypting A's blob "as if it were B's" must fail. The
	// adapter pins KeyId on Decrypt, which trips
	// IncorrectKeyException in our fake (matching AWS).
	if _, err := ks.UnwrapDEK(ctx, "tenant-b", wrappedA, vB); err == nil {
		t.Fatal("expected cross-tenant unwrap to fail")
	}

	// Sanity: tenant A can still unwrap its own blob.
	got, err := ks.UnwrapDEK(ctx, "tenant-a", wrappedA, vA)
	if err != nil {
		t.Fatalf("UnwrapDEK A: %v", err)
	}
	if !bytes.Equal(got, dekA) {
		t.Error("self-unwrap mismatch")
	}
}

// CurrentKEKVersion repairs in-memory state by resolving the alias
// against AWS — covering process-restart scenarios.
func TestCurrentKEKVersion_ResolvesExistingAlias(t *testing.T) {
	ctx := context.Background()
	ks, fake := newKS(t)

	// Simulate a CMK + alias that was created by a previous
	// process (no in-memory state in ks.versions).
	keyOut, err := fake.CreateKey(ctx, &awskms.CreateKeyInput{})
	if err != nil {
		t.Fatalf("seed CreateKey: %v", err)
	}
	keyID := awssdk.ToString(keyOut.KeyMetadata.KeyId)
	if _, err := fake.CreateAlias(ctx, &awskms.CreateAliasInput{
		AliasName:   awssdk.String("alias/eventstore-tenant-existing"),
		TargetKeyId: awssdk.String(keyID),
	}); err != nil {
		t.Fatalf("seed CreateAlias: %v", err)
	}

	v, err := ks.CurrentKEKVersion(ctx, "tenant-existing")
	if err != nil {
		t.Fatalf("CurrentKEKVersion: %v", err)
	}
	if v != 1 {
		t.Errorf("expected version 1 after alias resolve, got %d", v)
	}
}

// Custom AliasPrefix is honoured.
func TestConfig_AliasPrefixOverride(t *testing.T) {
	ctx := context.Background()
	fake := newFakeKMS()
	ks, err := New(Config{Client: fake, AliasPrefix: "myco"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, _, err := ks.WrapDEK(ctx, "t1", []byte("dek-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")); err != nil {
		t.Fatalf("WrapDEK: %v", err)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if _, ok := fake.aliases["alias/myco-t1"]; !ok {
		t.Errorf("expected alias/myco-t1, got %#v", fake.aliases)
	}
}
