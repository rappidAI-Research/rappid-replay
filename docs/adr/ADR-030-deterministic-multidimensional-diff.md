# ADR-030: Replay diff is deterministic, multi-dimensional, and read-only

- Status: Accepted
- Date: 2026-08-30

## Context

Replay needs to answer a stronger question than whether two final directories are byte-identical. Two runs can finish with the same files after taking different process, agent, or tool paths; conversely, two timelines can look similar while producing different workspace state or exit outcomes.

Architecture v1.0 therefore defines comparison across State, Timeline, Process, Agent, and Outcome dimensions and requires Replay to identify a common technical prefix and first divergence. Semantic AI comparison is optional and must never redefine canonical technical truth.

Run identity is also deliberately different from technical identity. A rerun receives a new session ID, new state IDs, new artifact IDs, process IDs, timestamps, and usually a different materialization path. Treating those execution-local identifiers as technical differences would make every legitimate rerun diverge immediately.

## Decision

`rappid replay diff <left-session-id> <right-session-id>` is a read-only operation. It opens existing local Replay evidence, authenticates state data, computes comparison results, and never executes agents, tools, shell commands, or restored code.

The canonical deterministic comparison contains these dimensions:

1. **Lineage.** Parent-session and fork-event relationships are read from persisted branch lineage. Replay never infers ancestry from event similarity. The nearest durable common session and the fork sequences on each side define the shared lineage prefix when one exists.
2. **State.** Final states are authenticated through the existing verify path before comparison. Equal root tree object IDs mean equal included workspace state. Unequal trees are recursively merge-scanned by canonical raw filename ordering. Equal content-addressed subtrees are pruned without traversal. File/symlink content and mode changes, directory mode changes, entry type changes, additions, and removals are reported. An added or removed directory is represented as one subtree change instead of enumerating every descendant.
3. **Timeline.** Events remain ordered by persisted session-local `seq`. For technical comparison, Replay builds a normalized event key that excludes envelope-local sequence/timestamps/session identity while retaining event type, source, privacy classification, normalized state roots, and normalized payload content. The common prefix is the longest equal prefix of those normalized event keys.
4. **Process.** The same normalized comparison is restricted to the `process.*` namespace and additionally reports event-type counts and first divergence.
5. **Agent.** The same normalized comparison is restricted to the `agent.*` namespace. Adapter/provider payload content remains evidence and is not globally stripped merely because a field happens to be named `pid`, `cwd`, or `session_id`.
6. **Outcome.** Session terminal status, the final authenticated root tree when available, and the last recorded `process.exited` exit code/success flag form the compact outcome comparison.

### Runtime-identity normalization

Normalization is deliberately narrow and event-specific.

For `process.*`, Replay ignores top-level process IDs (`pid`, `ppid`, `parent_pid`, `root_pid`) and the execution-local absolute `cwd` when forming the technical payload key. Executable path, command, PTY mode, process names, discovery metadata, and all other fields remain comparable.

For `session.started`, Replay ignores the execution-local `cwd` and branch bookkeeping payload fields (`parent_session_id`, `fork_event_seq`, `fork_state_id`). Durable lineage is compared separately.

For `state.snapshot`, the random session-local `state_id` payload field is ignored. The authenticated `root_tree_id`, role, object/file counts, and normalized envelope state roots remain comparable.

For `fs.artifact.discovered`, random `artifact_id` and session-local `from_state_id`/`state_id` payload fields are ignored. Path bytes, object IDs, previous object IDs, mode, size, change kind, and discovery method remain comparable. Envelope state references are normalized to authenticated root tree IDs.

No wildcard rule recursively removes similarly named fields from arbitrary payloads. In particular, `agent.*` provider identifiers remain visible technical evidence.

### State path representation

Canonical tree paths can contain filename bytes that are not valid UTF-8. Diff output therefore carries every path as an ordered `path_components_b64` array. A `display_path` is provided only for humans; it is not evidence identity and is never used for comparison.

### Bounded materialization

Diff counting is complete, but path-level change materialization is bounded by `--max-state-changes` (default 10,000). Truncation is explicit. Equal subtrees are content-address-pruned, so large unchanged trees are cheap to compare.

### Fingerprints

Normalized event payloads receive a SHA-256 fingerprint for compact display/navigation. This fingerprint is not a Replay CAS object ID, is not used as an integrity primitive, and does not replace BLAKE3-based object identity. The actual normalized payload participates in the internal technical comparison key.

## Consequences

Two sessions may be technically identical even though their durable lineage differs. Conversely, related parent/child sessions can diverge immediately after their fork point. Lineage and technical equality are intentionally separate results.

A missing final state makes the State dimension explicitly non-comparable rather than treating absence as an empty workspace. If one side has a final state, that state is still authenticated and surfaced for the Outcome dimension.

A corrupt final state aborts comparison instead of being converted into a normal difference. Existing integrity handling may mark the affected session degraded.

Timeline equality is deterministic with respect to persisted evidence and the versioned normalization rules above; it does not claim that external systems or nondeterministic model inference are reproducible.

Semantic/AI summaries may later annotate a technical diff, but they cannot change path changes, event prefix calculations, integrity results, lineage, or the `Identical` technical result.

## Rejected alternatives

**Compare only final workspace trees.** Rejected because it hides different execution paths and intermediate behavior.

**Compare raw event JSON byte-for-byte.** Rejected because new session/state/artifact IDs, timestamps, process IDs, and branch materialization paths would create false divergence on every rerun.

**Strip every payload field with a name such as `pid`, `cwd`, or `session_id`.** Rejected because adapter/provider payloads can use those names for meaningful evidence.

**Use semantic AI as the primary diff.** Rejected because model output is optional, non-canonical, and cannot decide integrity or technical equality.

**Normalize or rewrite evidence-bearing filenames for display.** Rejected because it would lose byte-exact path identity.
