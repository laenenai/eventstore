// Package shred hosts crypto-shredding logic for PII: field-level
// encryption under per-subject DEKs wrapped by tenant KEKs, with
// transparent decryption on read and a typed RedactedValue sentinel
// when the DEK is missing or shredded.
//
// See ADR 0010.
package shred
