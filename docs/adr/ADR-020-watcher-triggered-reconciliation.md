# ADR-020 — Watcher-triggered reconciliation

- Status: **Accepted**
- Date: 2026-08-28

## Context

Replay needs useful intermediate workspace states while an agent or other command is running, but operating-system filesystem notifications are not reliable enough to define state identity. Events may be coalesced, reordered, duplicated, lost on queue overflow, or delivered differently across supported platforms. The Architecture v1.0 baseline therefore states that the watcher is only a trigger and that canonical reconciliation/hash scanning is the source of truth.

The Generic Recorder already captures verified initial and final canonical state trees. Track B now needs intermediate checkpoints without weakening that invariant or coupling correctness to one platform-specific filesystem journal.

## Decision

Replay uses `github.com/fsnotify/fsnotify` v1.10.1 as the cross-platform notification trigger for Tier-1 recorder platforms. It is BSD-3-Clause licensed and remains behind an internal `workspaceChangeSource` interface.

The recorder recursively registers non-excluded directories. The configured deterministic ignore policy and the reserved `.git` exclusion are applied while building and extending the watch set. Newly created directories are added recursively when observed.

Filesystem notifications are never persisted as authoritative file-change evidence. Instead, notifications are coalesced and debounced, then cause a full canonical snapshot reconciliation using the same `state.Snapshotter` path as initial/final capture. A checkpoint is published only when the reconciled root object ID differs from the last published root.

Watcher backend errors, including overflow signals, are recorded as technical `fs.watcher.error` events and also trigger reconciliation. Transient inability to obtain a stable intermediate snapshot is recorded as `fs.reconcile.failed`; it does not abort an otherwise recordable command. The unconditional final snapshot after process exit remains authoritative for the final workspace state.

Replay performs one reconciliation immediately after watcher registration to close the race between the initial snapshot and watch setup. If watcher initialization is unavailable, the session records `fs.watcher.unavailable` and continues with exact initial/final state capture rather than pretending intermediate coverage exists.

All event and snapshot publication in one recording session is serialized through the recorder event sink. The monotonic timestamp is sampled while that serialization lock is held, preventing concurrent terminal output and checkpoint publication from committing in an order that violates monotonic event ordering.

## Consequences

- Intermediate states are content-derived and independently verifiable rather than dependent on notification fidelity.
- Queue overflow can reduce temporal granularity but cannot silently redefine the final recorded state.
- The watcher remains replaceable without changing state identity, event schemas, or persistence contracts.
- Recursive watches consume platform resources; ignored trees are pruned before registration to reduce that cost.
- A watcher-unavailable session can still reach exact final workspace reconstruction, but its intermediate filesystem timeline is explicitly incomplete.
- Full reconciliation work is proportional to the watched workspace after a debounced trigger; later performance work may add safe scan acceleration, but it must not make notification events authoritative.
