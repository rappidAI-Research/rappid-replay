# ADR-013: Foundation library selections

- Status: Accepted
- Date: 2026-08-27

## Context

Track A requires concrete implementations for UUIDv7 identifiers, SQLite, BLAKE3, zstd, and XChaCha20-Poly1305. Replay targets macOS, Linux, and Windows as Tier-1 platforms and should avoid making a C toolchain a prerequisite for the core build.

## Decision

Use these Go modules for the initial 1.0 implementation:

- `github.com/google/uuid` v1.6.0 for UUIDv7 generation and parsing.
- `modernc.org/sqlite` v1.57.0 as the `database/sql` SQLite driver. It is CGo-free.
- `lukechampine.com/blake3` v1.4.1 for BLAKE3 object identifiers.
- `github.com/klauspost/compress` v1.19.2 for zstd compression.
- `golang.org/x/crypto` v0.55.0 for XChaCha20-Poly1305 via `chacha20poly1305.NewX`.

These are implementation dependencies, not public API. Updating a dependency does not require a new ADR unless the observable storage, cryptographic, portability, or compatibility contract changes.

## Security constraints

- Object IDs are computed from canonical plaintext before compression or encryption.
- BLAKE3 object IDs are integrity/content identifiers, not authentication tags.
- Compression happens before encryption.
- XChaCha20-Poly1305 uses a 32-byte key supplied by the key-management boundary and a fresh random 24-byte nonce per stored payload.
- The object ID is bound as AEAD associated data and is re-verified after decryption/decompression.
- Replay must not construct SQLite `_pragma` values from untrusted input.
- The local master key remains outside SQLite and outside the object store; OS credential-store integration is a separate platform responsibility.

## Consequences

The core remains buildable without CGo across Tier-1 platforms. The storage codec can be tested independently of OS keychain integration, and the deterministic content identity remains independent of compression and encryption randomness.
