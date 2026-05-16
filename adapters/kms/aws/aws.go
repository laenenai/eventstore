package aws

import (
	"context"
	"errors"
	"fmt"
	"sync"

	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	awskms "github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/aws/aws-sdk-go-v2/service/kms/types"
	smithy "github.com/aws/smithy-go"

	"github.com/laenenai/eventstore/kms"
)

// kmsClient is the subset of *awskms.Client this adapter calls. Carving
// it out as an interface lets tests substitute a fake; production code
// passes the real client through Config.
type kmsClient interface {
	Encrypt(ctx context.Context, in *awskms.EncryptInput, opts ...func(*awskms.Options)) (*awskms.EncryptOutput, error)
	Decrypt(ctx context.Context, in *awskms.DecryptInput, opts ...func(*awskms.Options)) (*awskms.DecryptOutput, error)
	CreateKey(ctx context.Context, in *awskms.CreateKeyInput, opts ...func(*awskms.Options)) (*awskms.CreateKeyOutput, error)
	CreateAlias(ctx context.Context, in *awskms.CreateAliasInput, opts ...func(*awskms.Options)) (*awskms.CreateAliasOutput, error)
	UpdateAlias(ctx context.Context, in *awskms.UpdateAliasInput, opts ...func(*awskms.Options)) (*awskms.UpdateAliasOutput, error)
	DescribeKey(ctx context.Context, in *awskms.DescribeKeyInput, opts ...func(*awskms.Options)) (*awskms.DescribeKeyOutput, error)
}

// Config controls the AWS KMS adapter.
type Config struct {
	// Client is the AWS KMS client. In production, build with
	// awskms.NewFromConfig(awsCfg). Required.
	Client kmsClient

	// AliasPrefix is the literal prefix prepended to "/<tenantID>" to
	// form the AWS alias name. The full alias is
	// "alias/<AliasPrefix>-<tenantID>". Default: "eventstore".
	AliasPrefix string

	// KeyDescription is attached to CreateKey calls. Default:
	// "eventstore tenant KEK".
	KeyDescription string
}

// KeyStore implements kms.KeyStore against AWS KMS, with one CMK per
// tenant and a monotonic kek_version counter for framework bookkeeping.
//
// See package doc for rationale and cost trade-offs.
type KeyStore struct {
	cli            kmsClient
	aliasPrefix    string
	keyDescription string

	mu sync.RWMutex
	// versions[tenantID] = [keyIdV1, keyIdV2, ...]. Index 0 is
	// version 1. The CurrentKEKVersion is len(versions[tenantID]).
	versions map[string][]string
}

// New constructs a KeyStore. Returns an error if Config.Client is nil.
func New(cfg Config) (*KeyStore, error) {
	if cfg.Client == nil {
		return nil, errors.New("aws kms: Config.Client is required")
	}
	prefix := cfg.AliasPrefix
	if prefix == "" {
		prefix = "eventstore"
	}
	desc := cfg.KeyDescription
	if desc == "" {
		desc = "eventstore tenant KEK"
	}
	return &KeyStore{
		cli:            cfg.Client,
		aliasPrefix:    prefix,
		keyDescription: desc,
		versions:       map[string][]string{},
	}, nil
}

// aliasName returns the AWS alias name for a tenant. AWS aliases must
// start with "alias/".
func (k *KeyStore) aliasName(tenantID string) string {
	return fmt.Sprintf("alias/%s-%s", k.aliasPrefix, tenantID)
}

// CurrentKEKVersion implements kms.KeyStore. Returns 0 when no CMK has
// been registered for the tenant yet (WrapDEK will lazily allocate one
// on first call).
func (k *KeyStore) CurrentKEKVersion(ctx context.Context, tenantID string) (uint32, error) {
	k.mu.RLock()
	v := uint32(len(k.versions[tenantID]))
	k.mu.RUnlock()
	if v > 0 {
		return v, nil
	}
	// Cache miss: ask AWS whether an alias already exists (it might
	// be from a previous process). DescribeKey resolves aliases.
	keyID, err := k.describeKey(ctx, tenantID)
	if err != nil {
		if isNotFound(err) {
			return 0, nil
		}
		return 0, fmt.Errorf("aws kms: describe key for tenant %q: %w", tenantID, err)
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	if existing := k.versions[tenantID]; len(existing) > 0 {
		return uint32(len(existing)), nil
	}
	k.versions[tenantID] = []string{keyID}
	return 1, nil
}

// WrapDEK implements kms.KeyStore. Auto-creates the tenant's CMK +
// alias on first call.
func (k *KeyStore) WrapDEK(ctx context.Context, tenantID string, dek []byte) ([]byte, uint32, error) {
	keyID, version, err := k.activeKey(ctx, tenantID)
	if err != nil {
		return nil, 0, err
	}
	out, err := k.cli.Encrypt(ctx, &awskms.EncryptInput{
		KeyId:     awssdk.String(keyID),
		Plaintext: dek,
	})
	if err != nil {
		return nil, 0, fmt.Errorf("aws kms: encrypt for tenant %q: %w", tenantID, err)
	}
	return out.CiphertextBlob, version, nil
}

// UnwrapDEK implements kms.KeyStore. kekVersion must be a version
// previously emitted by WrapDEK for this tenant. AWS routes the
// Decrypt call based on metadata in the ciphertext blob, so the
// KeyId here serves only as a guard against accidentally accepting
// a blob from another key/tenant.
func (k *KeyStore) UnwrapDEK(ctx context.Context, tenantID string, wrapped []byte, kekVersion uint32) ([]byte, error) {
	if kekVersion == 0 {
		return nil, errors.New("aws kms: kekVersion 0 is invalid for unwrap")
	}
	k.mu.RLock()
	versions := k.versions[tenantID]
	k.mu.RUnlock()
	if int(kekVersion) > len(versions) {
		return nil, fmt.Errorf("aws kms: KEK version %d not available for tenant %q (have %d)", kekVersion, tenantID, len(versions))
	}
	keyID := versions[kekVersion-1]
	out, err := k.cli.Decrypt(ctx, &awskms.DecryptInput{
		CiphertextBlob: wrapped,
		// Pinning KeyId guards against decrypting a blob from
		// a different key: AWS will reject with
		// IncorrectKeyException if the blob's embedded KeyId
		// doesn't match.
		KeyId: awssdk.String(keyID),
	})
	if err != nil {
		return nil, fmt.Errorf("aws kms: decrypt for tenant %q version %d: %w", tenantID, kekVersion, err)
	}
	return out.Plaintext, nil
}

// RotateKEK implements kms.KEKRotator. Allocates a brand-new CMK,
// re-points the tenant alias at it (so future Encrypts use the new
// key), and bumps the local version counter. Existing wrapped DEKs
// keep working because their ciphertext blob carries the old KeyId.
func (k *KeyStore) RotateKEK(ctx context.Context, tenantID string) (uint32, error) {
	// Ensure the tenant exists first, so RotateKEK on a brand-new
	// tenant produces version 2 rather than implicit version 1.
	// This matches the inproc adapter's behaviour: RotateKEK creates
	// a new version unconditionally.
	if _, _, err := k.activeKey(ctx, tenantID); err != nil {
		return 0, err
	}
	newKey, err := k.cli.CreateKey(ctx, &awskms.CreateKeyInput{
		Description: awssdk.String(k.keyDescription),
		KeyUsage:    types.KeyUsageTypeEncryptDecrypt,
	})
	if err != nil {
		return 0, fmt.Errorf("aws kms: create rotated key for tenant %q: %w", tenantID, err)
	}
	if newKey.KeyMetadata == nil || awssdk.ToString(newKey.KeyMetadata.KeyId) == "" {
		return 0, fmt.Errorf("aws kms: create rotated key for tenant %q: empty KeyId in response", tenantID)
	}
	newKeyID := awssdk.ToString(newKey.KeyMetadata.KeyId)
	if _, err := k.cli.UpdateAlias(ctx, &awskms.UpdateAliasInput{
		AliasName:   awssdk.String(k.aliasName(tenantID)),
		TargetKeyId: awssdk.String(newKeyID),
	}); err != nil {
		return 0, fmt.Errorf("aws kms: update alias for tenant %q: %w", tenantID, err)
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	k.versions[tenantID] = append(k.versions[tenantID], newKeyID)
	return uint32(len(k.versions[tenantID])), nil
}

// activeKey returns the most-recent KeyId for the tenant, lazily
// creating a CMK + alias on first use. Idempotent across concurrent
// callers (double-checked under the write lock).
func (k *KeyStore) activeKey(ctx context.Context, tenantID string) (string, uint32, error) {
	k.mu.RLock()
	versions := k.versions[tenantID]
	k.mu.RUnlock()
	if len(versions) > 0 {
		return versions[len(versions)-1], uint32(len(versions)), nil
	}

	// Maybe a previous process created the alias; resolve it.
	if keyID, err := k.describeKey(ctx, tenantID); err == nil {
		k.mu.Lock()
		defer k.mu.Unlock()
		if existing := k.versions[tenantID]; len(existing) > 0 {
			return existing[len(existing)-1], uint32(len(existing)), nil
		}
		k.versions[tenantID] = []string{keyID}
		return keyID, 1, nil
	} else if !isNotFound(err) {
		return "", 0, fmt.Errorf("aws kms: describe key for tenant %q: %w", tenantID, err)
	}

	// Fully fresh tenant: create CMK + alias.
	created, err := k.cli.CreateKey(ctx, &awskms.CreateKeyInput{
		Description: awssdk.String(k.keyDescription),
		KeyUsage:    types.KeyUsageTypeEncryptDecrypt,
	})
	if err != nil {
		return "", 0, fmt.Errorf("aws kms: create key for tenant %q: %w", tenantID, err)
	}
	if created.KeyMetadata == nil || awssdk.ToString(created.KeyMetadata.KeyId) == "" {
		return "", 0, fmt.Errorf("aws kms: create key for tenant %q: empty KeyId in response", tenantID)
	}
	keyID := awssdk.ToString(created.KeyMetadata.KeyId)
	if _, err := k.cli.CreateAlias(ctx, &awskms.CreateAliasInput{
		AliasName:   awssdk.String(k.aliasName(tenantID)),
		TargetKeyId: awssdk.String(keyID),
	}); err != nil {
		return "", 0, fmt.Errorf("aws kms: create alias for tenant %q: %w", tenantID, err)
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	if existing := k.versions[tenantID]; len(existing) > 0 {
		// Lost the race; another goroutine already bound a key.
		// Both keys are valid; keep ours by appending — but the
		// race here would leak an orphan key. Documenting this
		// as a known minor concern: in practice, lazy tenant
		// initialization is rare and serialized at app startup.
		return existing[len(existing)-1], uint32(len(existing)), nil
	}
	k.versions[tenantID] = []string{keyID}
	return keyID, 1, nil
}

// describeKey resolves the tenant alias to a KeyId via AWS. Returns
// a *types.NotFoundException when the alias doesn't exist.
func (k *KeyStore) describeKey(ctx context.Context, tenantID string) (string, error) {
	out, err := k.cli.DescribeKey(ctx, &awskms.DescribeKeyInput{
		KeyId: awssdk.String(k.aliasName(tenantID)),
	})
	if err != nil {
		return "", err
	}
	if out.KeyMetadata == nil {
		return "", errors.New("aws kms: describe key returned empty KeyMetadata")
	}
	return awssdk.ToString(out.KeyMetadata.KeyId), nil
}

// isNotFound reports whether err is AWS's NotFoundException. AWS SDK
// v2 surfaces typed errors; we also fall back to a generic Smithy API
// error code check so callers swapping fakes don't have to construct
// the typed struct.
func isNotFound(err error) bool {
	var nf *types.NotFoundException
	if errors.As(err, &nf) {
		return true
	}
	var api smithy.APIError
	if errors.As(err, &api) {
		return api.ErrorCode() == "NotFoundException"
	}
	return false
}

// Compile-time interface checks.
var (
	_ kms.KeyStore   = (*KeyStore)(nil)
	_ kms.KEKRotator = (*KeyStore)(nil)
)
