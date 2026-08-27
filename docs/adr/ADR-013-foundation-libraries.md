# ADR-013: Foundation library selections

- Status: Accepted
- Date: 2026-08-27

## Context

Track A requires concrete implementations for UUIDv7 identifiers, SQLite, BLAKE3, zstd, XChaCha20-Poly1305, OS credential-store access, and cross-process first-run locking. Replay targets macOS, Linux, and Windows as Tier-1 platforms and should avoid making a C toolchain a prerequisite for the core build.

## Decision

Use these Go modules for the initial 1.0 implementation:

- `github.com/google/uuid` v1.6.0 for UUIDv7 generation and parsing.
- `modernc.org/sqlite` v1.57.0 as the `database/sql` SQLite driver. It is CGo-free.
- `lukechampine.com/blake3` v1.4.1 for BLAKE3 object identifiers.
- `github.com/klauspost/compress` v1.19.2 for zstd compression.
- `golang.org/x/crypto` v0.55.0 for XChaCha20-Poly1305 via `chacha20poly1305.NewX`.
- `go.yaml.in/yaml/v3` v3.0.5 for strict project/user YAML configuration parsing.
- `github.com/zalando/go-keyring` v0.2.8 for macOS Keychain, Windows Credential Manager, and Linux Secret Service integration.
- `github.com/gofrs/flock` v0.13.0 for cross-platform first-run master-key initialization locking.

These are implementation dependencies, not public API. Updating a dependency does not require a new ADR unless the observable storage, cryptographic, portability, or compatibility contract changes.

## Security constraints

- Object IDs are computed from canonical plaintext before compression or encryption.
- BLAKE3 object IDs are integrity/content identifiers, not authentication tags.
- Compression happens before encryption.
- XChaCha20-Poly1305 uses a 32-byte key supplied by the key-management boundary and a fresh random 24-byte nonce per stored payload.
- The object ID is bound as AEAD associated data and is re-verified after decryption/decompression.
- Replay must not construct SQLite `_pragma` values from untrusted input.
- The local master key remains outside SQLite and outside the object store and must fail closed when the OS credential store is unavailable.
- The key-management boundary must not silently fall back to an unencrypted or plaintext filesystem credential store.

## Consequences

The core remains buildable without CGo across Tier-1 platforms. The storage codec can be tested independently of the real OS keychain through an injected credential-store interface, and deterministic content identity remains independent of compression and encryption randomness.
