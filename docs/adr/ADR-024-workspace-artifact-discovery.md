# ADR-024: Workspace artifact discovery is derived from published state transitions

- Status: Accepted
- Date: 2026-08-28

## Context

Architecture v1.0 requires the Generic Recorder to capture artifacts without making agent-specific adapters mandatory. The deterministic core already publishes authenticated workspace states into the encrypted CAS and records watcher-triggered reconciliation checkpoints. We need artifact provenance that remains useful for unknown commands while avoiding a false semantic claim that Replay can infer which changed file the user or agent intended as an output.

Artifact metadata must preserve Replay's existing invariants: append-only evidence, raw path fidelity, authenticated CAS identity, session-local total event ordering, and no duplicate plaintext storage merely to label a state object as an artifact.

The baseline event namespaces are already defined. Artifact observation is filesystem-derived evidence, so introducing a new top-level `artifact.*` namespace would unnecessarily expand that stable surface.

## Decision

The Generic Recorder derives artifact observations from consecutive **published** workspace states.

A regular file is published as an artifact observation when, across one state transition, it is:

- created;
- content-modified, meaning its authenticated file object ID changes; or
- created at a path that previously contained a non-file entry (`replaced`).

Mode-only changes are not new artifact observations because the authenticated file payload is unchanged. Deletions and symlink changes remain state evidence but are not generic file artifacts.

This classification is deliberately technical. A `workspace-delta` artifact means only that Replay observed a regular file payload appear or change during the run. It does **not** assert that the file is a successful build product, requested deliverable, or semantically important output. Agent adapters may add richer artifact semantics later without changing this baseline.

Each observation receives an immutable UUIDv7-backed `ar_` identifier. SQLite stores:

- the session;
- the source and destination state IDs;
- raw workspace-relative path bytes plus a best-effort UTF-8 display path;
- `created`, `modified`, or `replaced` change kind;
- the current CAS object ID and, where applicable, the prior object ID;
- file mode and size;
- the event sequence that published the observation.

The corresponding timeline event is `fs.artifact.discovered`, keeping the event inside the Architecture v1.0 `fs.*` namespace. Its payload carries `path_b64` so invalid UTF-8 path bytes survive losslessly, while `path_display` is presentation-only.

Artifact publication is a single SQLite transaction: Replay reserves the next event sequence, inserts the `fs.artifact.discovered` event, and inserts its artifact provenance row. The current object must already be reachable from the destination state. A previous object, when present, must be reachable from the source state. Modified observations additionally require both objects to be file objects (`blob` or `chunk_list`).

The artifact row references the existing authenticated CAS object. Replay does not copy workspace artifact bytes into a second artifact store. This preserves CAS deduplication and makes large chunked files work without a parallel storage format. A separate `artifacts/` storage area remains available for future non-workspace artifact classes that are not already represented by workspace state.

Artifact derivation runs after every successfully published reconciliation checkpoint and after the final state publication. The watcher is still only a trigger; the compared Merkle states are the source of truth.

## Consequences

Generic recording now provides deterministic, provider-independent artifact provenance for files observed at state boundaries. The same path may legitimately produce multiple immutable artifact observations across different states as its content evolves.

Short-lived files that are created and removed entirely between reconciliation boundaries may not be observed by the generic filesystem path. Replay must not claim otherwise. Future adapters can supply explicit tool or agent artifact events where stronger semantics or lifecycle coverage is available.

A failure while publishing artifact metadata does not roll back an already-published state snapshot. The recorder aborts the session and retains all previously committed evidence; it never deletes or silently rewrites the state to hide the partial failure.

## Rejected alternatives

**Treat every changed path as an intended artifact.** Rejected because source edits, caches, and incidental files would be mislabeled semantically.

**Copy every artifact into a separate artifact directory.** Rejected for workspace-backed artifacts because the encrypted CAS already provides authenticated storage and deduplication.

**Use `artifact.discovered` as a new top-level event namespace.** Rejected because Architecture v1.0 already defines `fs.*` for filesystem-derived evidence.

**Infer artifacts from watcher notifications directly.** Rejected because watchers are lossy triggers; published reconciled states remain authoritative.
