# ADR-029: Branch from an exact state and require explicit live-rerun consent

- Status: Accepted
- Date: 2026-08-30

## Context

Replay branching must preserve the distinction between historical evidence and new execution. A selected state is immutable evidence. A branch is a new mutable workspace materialized from that state. A rerun is a new session whose lineage points back to the parent session and the exact event sequence at which the source state was published.

The architecture also requires that secrets are not reconstructed from history and that risky side effects are explicitly confirmed. Recorded command metadata may be privacy-redacted, so silently reusing historical argv could both fail to reproduce the original invocation and encourage unsafe secret reconstruction.

## Decision

`rappid replay branch <state-id>` is a non-executing operation. It authenticates the selected state using the existing verify/restore path, validates path portability, materializes into a staged sibling directory, and commits the completed workspace only after verification succeeds. It does not create a child execution session merely because a mutable branch directory was requested.

`rappid replay rerun --mode live <state-id> -- <command> ...` first materializes the selected state as a branch and then records a new Generic Recorder session from that workspace. The child session persists `parent_session_id` and `fork_event_seq`. Session creation validates that the declared fork tuple resolves to the exact selected published state.

Before any child process starts, the Generic Recorder re-captures the branch workspace and requires its canonical root object ID to equal the source state's root object ID. A mismatch aborts the new child session and no command is executed. This protects the branch boundary from platform materialization differences, ignore-policy drift, or mutation between restore and execution.

Live rerun never substitutes a historical command automatically. The user supplies the command explicitly after `--`; secrets and other sensitive arguments are therefore provided from the current execution context rather than reconstructed from persisted history.

Live rerun requires explicit `--confirm-execution`. The flag is interpreted as consent to execute the supplied command and accept its potential external side effects. Arguments are still passed directly to the process API; Replay does not concatenate them into an implicit shell string.

The rerun mode vocabulary is fixed as `recorded`, `live`, `controlled`, and `hybrid`. This implementation executes only `live`. The other modes fail closed before materialization until playback and cassette-controlled external-I/O support exist. Reserving the names now prevents later CLI/schema drift without pretending unavailable guarantees are implemented.

A branched live rerun intentionally does not apply the current project's ignore configuration to its initial snapshot. The restored branch contains only files that were part of the selected evidence state, and the exact-root assertion is authoritative. Applying today's ignore rules could otherwise silently remove historical evidence before execution. Subsequent states in that child session are captured under the same no-ignore policy for consistency.

## Consequences

A successful child session has explicit immutable lineage and an initial state whose root object exactly equals the selected historical state. Identical content still receives a new `StateID`; content identity remains the CAS root object ID.

Branch workspaces remain ordinary mutable directories after materialization. Their creation does not alter the parent session or append events to it.

Rerun failure after branch materialization leaves the branch directory available for inspection rather than deleting potentially useful forensic state.

Cross-platform materialization may succeed while exact re-capture fails because the host cannot preserve some recorded filesystem semantics. In that case live rerun refuses execution instead of silently weakening the branch guarantee.

## Rejected alternatives

**Reuse the recorded command automatically.** Rejected because persisted argv may be redacted and Replay must not reconstruct secrets from history.

**Treat restore success as sufficient for rerun.** Rejected because a mutable workspace can change between restore and process start; exact-root revalidation closes that gap.

**Create a child session for a non-executing branch.** Rejected because sessions represent recorded execution histories, while a branch directory alone is mutable working state rather than immutable execution evidence.

**Silently fall back from controlled/hybrid to live execution.** Rejected because it would misrepresent external-I/O guarantees and could cause unintended side effects.
