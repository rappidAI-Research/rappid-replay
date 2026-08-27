# ADR-015: Verified state publication is atomic

Status: **Accepted**

Date: 2026-08-27

## Context

Replay's architecture requires a state to become visible only after all of its reachable CAS objects have been written and verified. Publishing object metadata, a state row, its event, and session pointers in separate database operations would allow crashes or partial failures to expose a state that cannot be restored.

## Decision

A state publication MUST first authenticate and traverse the complete reachable snapshot graph. The verified object metadata, state row, state-to-object reachability edges, `state.snapshot` event, and any initial/final session pointer are then committed in one SQLite transaction.

The object catalog is append-only by content ID. Reusing an existing object ID is allowed only when its verified kind and size metadata exactly match the existing database row. Metadata mismatches are treated as integrity failures.

Published state IDs use a UUIDv7-backed `st_` identifier. The root BLAKE3 CAS object ID remains the content identity of the workspace; the state ID identifies the immutable publication record, allowing identical contents to occur in multiple sessions.

## Consequences

- SQLite cannot reference a state whose CAS graph failed verification.
- `state_objects` provides explicit reachability for later GC, export, and integrity operations.
- Initial snapshot publication raises a session from R0 to at least R1 because the included workspace bytes are exact and verified.
- Failed publication can leave unreferenced CAS objects, but cannot leave partially published metadata; later mark-and-sweep GC may reclaim those objects.
- Chunk-list states are not publishable until chunk traversal is implemented, preventing incomplete reachability metadata.
