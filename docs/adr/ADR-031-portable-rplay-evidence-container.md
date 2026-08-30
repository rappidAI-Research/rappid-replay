# ADR-031: `.rplay` is a self-contained authenticated evidence container

- Status: Accepted
- Date: 2026-08-30

## Context

Replay needs a portable interchange format that can move immutable session evidence between machines without coupling exports to the local SQLite file, encrypted CAS representation, OS credential store, or any rappidAI cloud service.

The format must preserve the deterministic core's existing truth model: session/event/state identities remain immutable, canonical state trees remain content-addressed, adapters remain additive, and import/verification must never execute recorded code.

A portable container is also an untrusted-input boundary. Archive path traversal, duplicate entries, incomplete object graphs, unsupported required features, identifier conflicts, and lineage forgery must fail closed.

## Decision

Replay uses the versioned `.rplay` container described in `docs/format/rplay-v1.md`.

Version 1 is ZIP/ZIP64 with a canonical `manifest.json`, BLAKE3 entry checksums, zstd-compressed NDJSON timeline/state/artifact streams, optional privacy-filtered environment JSON, and canonical typed plaintext CAS object frames.

The exported object representation is the exact typed plaintext frame whose BLAKE3 digest defines the Replay object ID. Local encrypted CAS payloads and local master keys are never exported.

Export of a session includes its complete ancestor lineage. Import validates parent/fork relationships against the exact published parent state and requires the child's initial root to match that fork root. Original immutable IDs are preserved; an existing session ID is a conflict rather than a merge target.

Archive verification authenticates checksums, validates object IDs and typed frames, proves every state object graph is complete, and validates structured session evidence before import. Reading, verifying, and importing are non-executing operations.

Unknown required format features and unsupported format versions fail closed. Unknown optional JSON fields may be ignored. Version 1 does not claim archive encryption or signatures; archives that request unsupported encryption/signature semantics are rejected.

Export applies the configured secret-scan mode (`block`, `warn`, or `off`) before publishing the completed archive. This is an export-time policy record, not a guarantee that workspace evidence contains no sensitive data.

## Consequences

A `.rplay` archive is independent of the originating user's local CAS encryption key and can be authenticated before local persistence is changed.

Portable archives may be larger than the local CAS representation because the portable object layer intentionally favors stable canonical identity over reusing local ciphertext/compression bytes.

Import may write content-addressed CAS objects before the SQLite metadata transaction. If metadata validation later fails, those objects remain unreferenced and are eligible for normal GC; no partial session metadata becomes visible.

Because v1 is unencrypted, users must treat `.rplay` files as potentially sensitive. Encryption can be added as a separately specified portable layer without changing or exporting the local CAS master key.

## Rejected alternatives

**Copy `replay.db` and the local `objects/` directory.** Rejected because that leaks implementation details, couples portability to local encryption/key management, and makes compatibility/migrations unsafe.

**Export local CAS ciphertext directly.** Rejected because recipients do not possess the originating OS-backed master key and ciphertext identity is not the canonical Replay object identity.

**Best-effort import that rewrites conflicting IDs.** Rejected because immutable evidence and branch lineage would no longer refer to the same execution history.

**Extract the ZIP to disk and validate afterward.** Rejected because archive entry names are untrusted and Replay does not need filesystem extraction to verify or import evidence.

**Silently accept unknown required features.** Rejected because doing so would claim guarantees the reader does not understand.
