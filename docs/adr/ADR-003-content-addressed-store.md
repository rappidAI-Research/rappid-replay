# ADR-003: Dedicated encrypted content-addressed store

- Status: Accepted
- Date: 2026-08-27

## Context

Replay must restore historical workspace states independently of a user's Git repository and must support deduplication, chunking, encryption, and Replay-specific metadata.

## Decision

Use a Replay-owned content-addressed object store. Object identity is derived from BLAKE3 over the canonical plaintext object. Stored payloads are compressed with zstd and encrypted at rest with XChaCha20-Poly1305. The user's `.git` directory is never used as Replay storage.

## Invariant

A state may not become visible or be referenced until every object it references has been durably written and verified.

## Consequences

The exact Go libraries, key-management implementation, chunking algorithm, and on-disk envelope are implementation decisions that require focused review before being treated as stable public surfaces.
