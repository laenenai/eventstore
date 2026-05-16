// Package aws is a production kms.KeyStore backed by AWS KMS.
//
// Design: one Customer Master Key (CMK) per tenant, aliased
// "alias/<prefix>-<tenantID>" (prefix configurable; default
// "eventstore"). WrapDEK calls kms.Encrypt against the tenant's alias;
// UnwrapDEK calls kms.Decrypt against the AWS-returned ciphertext blob
// (AWS routes the call to the correct key from blob metadata). The
// kekVersion uint32 the framework expects is the adapter's own
// monotonic counter per tenant, persisted in-memory; RotateKEK
// allocates a new CMK + alias and bumps the counter.
//
// Why per-tenant CMK (not per-tenant data key under a shared CMK):
//
//   - Crypto-shredding at the tenant boundary: ScheduleKeyDeletion on
//     the tenant's CMK destroys access to every DEK ever wrapped under
//     it. Per-data-key designs require iterating subjects.
//   - IAM scoping: per-tenant key policies let you grant
//     tenant-scoped access without conditional policies on resource
//     tags.
//   - Audit: CloudTrail entries are pre-segmented by KeyId, which is
//     pre-segmented by tenant.
//
// Cost note: AWS KMS bills $1/month per CMK plus per-request charges.
// Per-tenant CMK is appropriate for B2B SaaS with O(100s)–O(1000s) of
// tenants; for many-tenants-per-customer (consumer-scale) workloads,
// consider a shared CMK with per-tenant data-key derivation in a
// follow-up adapter (out of scope for v1).
//
// Rotation: the adapter implements kms.KEKRotator. RotateKEK creates
// a brand-new CMK (CreateKey) and re-points the alias (UpdateAlias).
// Existing wrapped DEKs continue to decrypt under their original key
// because the ciphertext blob carries the KeyId; the kekVersion
// counter exists for the framework's bookkeeping, not for AWS routing.
// You can also rely on AWS's built-in annual rotation of CMK material
// — that doesn't bump our kekVersion (the alias still resolves) but
// covers cryptographic hygiene transparently.
//
// Not in v1:
//
//   - Live integration tests against AWS or a localstack/testcontainers
//     KMS. Unit tests use an in-package mock kmsClient. A follow-up PR
//     can add an integration suite gated by an env flag.
//   - Cross-region replication. Multi-Region keys can be plugged in by
//     passing a pre-existing key alias via Config.
//   - Encryption context. AWS KMS supports an EncryptionContext map
//     that becomes additional authenticated data. v1 omits this to
//     keep the surface minimal; add it if you need extra defence
//     against confused-deputy at the KMS boundary.
package aws
