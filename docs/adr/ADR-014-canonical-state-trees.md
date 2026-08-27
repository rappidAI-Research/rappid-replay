# ADR-014: Canonical recursive state trees

- Status: Accepted
- Date: 2026-08-27

## Context

Replay must restore included workspace state byte-exactly and deduplicate repeated content across sessions. A flat list of paths is convenient for presentation but is not the durable storage primitive described by Architecture v1.0. The architecture requires State → Tree → blob/chunk-list, directory tree, or symlink link objects in the dedicated CAS.

Tree identity must also be independent from host traversal order, JSON whitespace, Unicode normalization, compression, and encryption. Filenames on Unix are byte strings and may not be valid UTF-8, so using ordinary JSON strings as the canonical filename representation would be lossy.

## Decision

Use recursive Merkle tree objects as the canonical state representation.

- Each directory is a typed `tree` CAS object.
- File entries reference typed `blob` objects initially and may reference `chunk_list` objects once large-file chunking is implemented.
- Symlink entries reference typed `link` objects containing the raw link target bytes. Symlinks are not followed during capture.
- Tree entries contain exactly one raw filename component, entry kind, permission bits, logical size, and child object ID.
- Canonical tree JSON orders entries by raw filename bytes and base64-encodes those filename bytes on the wire.
- Parsing requires the exact canonical byte representation. Reordered or whitespace-equivalent JSON is rejected.
- CAS objects use stable type framing before BLAKE3 hashing so equal payload bytes in different object domains cannot alias to the same object ID.
- Snapshot publication fails when the workspace is observed mutating during capture; callers retry instead of publishing a state whose consistency was not established.

## Consequences

A root tree object ID commits to all reachable directory structure and file/link contents. Unchanged subtrees deduplicate naturally. The representation is suitable for later verification, diffing, export, and safe materialization without relying on the source workspace.

Platform-specific metadata beyond common permission bits, content-defined large-file chunking, and restore/materialization policy remain separate follow-up work and do not change this canonical tree identity contract casually.
