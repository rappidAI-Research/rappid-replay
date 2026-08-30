# `.rplay` format v1

`.rplay` is rappidAI Replay's portable evidence container. It transfers immutable Replay sessions, timelines, published workspace states, artifact provenance, privacy-filtered environment metadata, and the canonical CAS objects required to authenticate and reconstruct those states.

Reading, verifying, importing, or inspecting an archive is non-executing. A `.rplay` reader must not start recorded commands, agents, tools, model inference, or restored workspace code.

## Container

Version 1 uses a ZIP/ZIP64 container. Archive paths always use `/` separators and are relative to the archive root. Absolute paths, `..`, `.` components, empty path components, backslashes, directory entries, symlink entries, duplicate names, and unexpected non-object payloads are rejected.

Writers emit deterministic entry ordering, file modes, and a fixed ZIP timestamp so the same logical bundle with the same manifest timestamps serializes reproducibly.

The v1 layout is:

```text
manifest.json
checksums.b3
sessions/<session-id>/session.json
sessions/<session-id>/events.ndjson.zst
sessions/<session-id>/states.ndjson.zst
sessions/<session-id>/artifacts.ndjson.zst
sessions/<session-id>/environment.json        # optional
objects/<64-hex-blake3>.rpobj
```

`events.ndjson.zst`, `states.ndjson.zst`, and `artifacts.ndjson.zst` are newline-delimited JSON compressed with zstd. Empty event/state/artifact sets are represented by valid empty zstd streams rather than by omitting the required entry.

## Manifest

`manifest.json` contains:

- `format`: `rappid-replay`
- `version`: currently `1.0.0`
- `created_at`: UTC timestamp
- `created_by`: writer identity
- `required_features`: semantic features a reader must understand before accepting the archive
- `sessions`: session IDs and durable parent/fork lineage
- `privacy`: export secret-scan mode and archive-encryption flag
- `integrity`: checksum algorithm and checksum-file location

The current reader accepts version `1.0.0` exactly. Unknown JSON fields are ignored so additive metadata can be carried without changing canonical evidence. Unknown `required_features` are rejected fail-closed. A future format reader may add explicit migrations or wider SemVer compatibility; v1 readers do not silently reinterpret another format version.

Version 1 does not yet define encrypted or signed `.rplay` containers. `privacy.encrypted=true` and a non-empty signature path are therefore rejected instead of being ignored. Local Replay CAS master keys are never exported.

## Integrity

`checksums.b3` contains one lowercase BLAKE3-256 digest for every archive entry except `checksums.b3` itself:

```text
<64 lowercase hex>  <archive/path>
```

Coverage must be exact: missing checksums, duplicate checksum records, checksum references to absent entries, and archive entries without checksums are errors.

Object files use the canonical Replay typed plaintext object frame. The object archive path is derived from the canonical object ID:

```text
objects/<digest>.rpobj  <=>  b3:<digest>
```

A reader recomputes the BLAKE3 object ID from the complete typed plaintext frame and rejects any mismatch. Local CAS ciphertext, XChaCha20 nonces, zstd-at-rest representation, and OS credential-store material are not portable format data.

## State graph completeness

Every published state in the archive must be independently reconstructible from archive objects. Verification recursively authenticates tree objects and validates canonical tree encoding, file blobs, chunk lists, chunk sizes, symlink objects, and declared file/link sizes. A missing reachable child object or malformed typed object invalidates the archive.

The archive preserves evidence-bearing filename bytes in canonical tree objects. Import does not normalize those names for the current host. Host-specific pathname compatibility is checked later when a state is materialized.

## Session and lineage semantics

Each exported session is sealed; a live `recording` session is not portable evidence. Session metadata retains the privacy-filtered persisted command, CWD, timestamps, reproducibility level, adapter identity, initial/final state IDs, and optional parent lineage.

When a requested session is exported, Replay includes its complete ancestor chain. This keeps `parent_session_id` and `fork_event_seq` self-contained. During import, parents are inserted before children even if archive session order differs. A child is accepted only when its initial state's root object exactly equals the parent's published state at the recorded fork event sequence.

Import preserves original session, state, event, artifact, and object IDs. Existing immutable session IDs are never merged or rewritten. Metadata import is one SQLite transaction; a lineage or validation failure rolls the metadata transaction back. CAS objects written before that transaction are content-addressed and may remain as unreferenced objects for normal GC.

## Privacy

The manifest records the export-time secret-scan mode: `block`, `warn`, or `off`.

`block` refuses archive creation if the configured scanner finds likely secret material. `warn` writes the archive and returns findings. `off` performs no export secret scan. The mode records what the exporter did; it is not a claim that an archive is secret-free.

Exported command/environment/event data is already subject to Replay's recording-time privacy rules. Workspace state objects can still contain arbitrary user or agent files, so recipients must treat `.rplay` archives as potentially sensitive evidence.

## Resource limits and hostile input

The v1 reader bounds archive entry count, aggregate declared uncompressed size, manifest/session/environment/checksum sizes, compressed NDJSON entries, decompressed NDJSON lines, and typed object frames. It validates paths and duplicate entries before parsing archive payloads and authenticates checksums before accepting structured evidence.

These limits are defensive parser bounds, not a promise that every archive below the limits is cheap to process. Importers should continue to apply ordinary disk quotas and untrusted-input controls.

## CLI

```sh
rappid replay export <session-id> --out run.rplay
rappid replay verify --archive run.rplay
rappid replay import run.rplay
```

`export` supports `--force` for explicit replacement and `--secret-scan block|warn|off`. `verify --archive` requires no local Replay data store and performs no import. `import` authenticates the complete archive before mutating local metadata.
