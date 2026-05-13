// Package party is a worked-example aggregate demonstrating the
// framework's features end-to-end:
//
//   - First-class uniqueness (email is unique per tenant)
//   - PII annotations (name, email, phone, address default-encrypted)
//   - Maker-checker workflow for identity-critical fields (name, email,
//     date_of_birth) — propose -> approve/reject/withdraw
//   - Auto-apply workflow for correspondence fields (phone, address)
//   - Status state machine (active <-> suspended, -> closed)
//   - Generic pending-changes representation (one slice + oneof inside,
//     "at most one pending per change type" enforced by the decider)
//
// See README.md for the full walkthrough.
package party
