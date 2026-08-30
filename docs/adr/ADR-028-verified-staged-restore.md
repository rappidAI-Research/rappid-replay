# ADR-028: Restore verifies first and commits a staged tree

- Status: Accepted
- Date: 2026-08-30

## Context

Replay promises R1 state-exact reconstruction for included workspace files. A restore therefore cannot be implemented as a best-effort copy into a live destination: an unreadable CAS object, an incompatible pathname, a chunk mismatch, or a partial filesystem write must not leave a directory that looks like a successful historical state.

Restore is also a read-only Replay operation in the execution sense. It may materialize files, but it must never start the recorded command, an agent, a tool, or any restored code.

Architecture v1.0 and ADR-019 already require complete path-portability validation before materialization and prohibit renaming, escaping, truncating, or case-folding evidence-bearing paths to fit the current host.

## Decision

`rappid replay verify <state-id>` resolves the immutable published state and authenticates the complete reachable CAS graph. It performs no workspace writes and executes no code.

`rappid replay restore <state-id>` performs the following ordered steps:

1. Resolve the published state from SQLite.
2. Authenticate and structurally verify the complete CAS graph.
3. Validate the complete tree for the destination operating system.
4. Create a private staging directory beside the final destination.
5. Materialize the tree into staging, re-authenticating objects as they are read.
6. Reconstruct chunk-list files in order, enforcing declared chunk and file sizes.
7. Recreate symlinks with their recorded target bytes; never follow them during traversal.
8. Apply recorded file and directory permission bits where the host supports them.
9. Commit the completed staging directory with a rename.

A destination that already exists is rejected by default. `--force` is explicit replacement permission, not permission for piecemeal mutation. Replay first renames the old directory to a sibling backup, renames the completed staging tree into place, and rolls the old directory back if the commit rename fails. After a successful commit the backup is removed.

Replay refuses restore destinations that overlap the Replay data root in either direction, so a restore cannot replace or recursively contain the SQLite database or encrypted CAS it is reading.

The filesystem root itself is never a valid restore destination.

A CAS corruption discovered during verify or materialization is never substituted or repaired silently. The operation fails. Post-run integrity degradation is allowed to transition previously completed or aborted sessions to `degraded`, because evidence can become unreadable after recording has ended.

## Consequences

A successful restore has a clear commit boundary: before the final rename the requested destination is untouched, and after the rename it contains a completely materialized state.

`--force` may temporarily require enough disk space for both the previous destination and the restored staging tree. This is preferred over destroying the old directory before the replacement is known to be complete.

POSIX permission bits are restored from the canonical state metadata. Filesystem-specific metadata that Replay did not capture is outside the R1 guarantee. Symlink creation can fail on hosts whose security policy or privilege model disallows it; Replay reports that failure instead of degrading the link into a regular file.

Cross-filesystem rename is avoided by placing staging and backup paths in the destination's parent directory.

## Rejected alternatives

**Write directly into the destination.** Rejected because a mid-restore failure would expose a mixed or partial historical state.

**Delete an existing destination before restoring with `--force`.** Rejected because failure would destroy the user's previous directory without producing a valid replacement.

**Rewrite incompatible filenames for Windows or another target.** Rejected because the materialized tree would no longer represent the recorded state.

**Follow symlinks while restoring descendants.** Rejected because it would permit materialization outside the staging root and would not match the canonical tree model.
