# ADR-016: Content-defined chunking for large files

- Status: Accepted
- Date: 2026-08-27

## Context

Replay's state model must remain byte-exact while avoiding pathological storage growth when large files change locally. Storing every file as one CAS blob means even a small insertion into a large file creates an entirely new object. Fixed-size chunks improve deduplication but shift every downstream chunk after an insertion or deletion.

Architecture v1.0 therefore calls for variable content-defined chunking for large files, with an 8 MiB threshold as the recommended starting point.

## Decision

Files up to and including 8 MiB remain single `blob` objects. Larger files are represented by an ordered `chunk_list` object whose entries reference `blob` chunk objects.

Chunk boundaries use Replay CDC v1:

- deterministic 64-bit Gear rolling hash
- fixed Gear table generated with SplitMix64 from seed `0x7261707069644149`
- minimum chunk size: 1 MiB
- target boundary probability: 1 / 4 MiB using mask `0x3fffff`
- maximum chunk size: 8 MiB
- the final chunk may be smaller than the minimum

The chunk-list payload is a versioned canonical binary structure containing the total plaintext file size plus the ordered sequence of `(chunk_size, object_id)` entries. Chunk-list identity therefore commits to chunk order, chunk lengths, and chunk content identities.

A tree file entry continues to carry the original total file size and references either a `blob` or `chunk_list`. Verification and state publication MUST traverse every chunk referenced by a chunk list. A state is not publishable if any chunk is missing, corrupt, has the wrong object type, or does not match its declared length.

## Consequences

Small files retain the simplest representation. Large edits can reuse unaffected chunks across snapshots, while exact reconstruction remains possible from the ordered chunk list.

CDC parameters and chunk-list encoding are part of Replay's storage compatibility contract. Changing them requires a new version rather than silently changing v1 behavior.

The initial implementation still reads a stable file into memory before chunking. Streaming capture is a performance refinement that may be introduced later without changing CDC v1 boundaries or the chunk-list format.
