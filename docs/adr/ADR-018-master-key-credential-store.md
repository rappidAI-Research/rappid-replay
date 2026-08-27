# ADR-018: OS-backed local CAS master key

- Status: Accepted
- Date: 2026-08-27

## Context

Replay's local content-addressed store encrypts every stored object with XChaCha20-Poly1305. Architecture v1.0 requires the 256-bit local master key to live in the operating-system credential store rather than SQLite, project configuration, or the object-store filesystem.

The first process that initializes Replay must also avoid a race in which two processes generate different keys and alternately overwrite the same credential. Headless Linux systems may not have a Secret Service provider available; silently falling back to a plaintext key file would violate the security model.

## Decision

Replay uses one versioned per-user local CAS master key identified by credential service `rappidAI Replay` and account `local-cas-master-key-v1`.

- Generate exactly 32 random bytes with `crypto/rand` when the credential is absent.
- Store the key as unpadded base64 in the native OS credential store.
- Use `github.com/zalando/go-keyring` v0.2.8 as the cross-platform credential-store adapter.
- Use macOS Keychain, Windows Credential Manager, and Secret Service over D-Bus on Linux through that adapter.
- Serialize first-run initialization with `github.com/gofrs/flock` v0.13.0 and a non-secret `master-key.lock` file below `~/.config/rappidAI/replay/`.
- Re-read and verify a newly persisted key before allowing the CAS to open.
- Treat malformed stored credentials and credential-backend errors as fatal. Never regenerate over an existing unreadable value and never fall back to a plaintext key file.
- Clear temporary key byte slices after the storage codec has consumed them where practical. Go does not provide a general guarantee that compiler/runtime copies are erased.

The versioned credential account is part of the local storage compatibility contract. Key rotation or migration must use an explicit future protocol; it must not silently replace `local-cas-master-key-v1`.

## Security properties

The credential-store backend protects the master key according to the logged-in operating-system user's security boundary. Replay does not claim protection from a process already capable of reading that user's unlocked credential store or memory.

The filesystem lock contains no secret material. Its sole purpose is to prevent competing Replay processes from racing during first-run key creation. OS-backed file locks are released when the owning process exits, avoiding stale-lock recovery logic.

On Linux, the absence or unavailability of a Secret Service implementation is an explicit startup error for encrypted storage. An insecure filesystem fallback is prohibited.

## Consequences

Normal production callers can open the local CAS without accepting raw key material from configuration or command-line flags. Tests retain an injected credential-store boundary and may construct codecs with explicit ephemeral keys without touching a developer's real keychain.

All Replay data roots used by the same OS user currently share this versioned local master key. This keeps key discovery independent of the data-root path and allows a data root to move without losing its key locator. Per-store keys can be introduced later only through an explicit migration design.
