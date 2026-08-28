# ADR-021 — Privacy-filtered execution environment capture

- Status: **Accepted**
- Date: 2026-08-28

## Context

Replay's R2 reproducibility level requires an execution-environment record in addition to the byte-exact workspace state. Environment variables and Git context are useful for explaining why two otherwise identical workspaces behave differently, but environment variables are also a common secret-transport mechanism. Persisting them without filtering would violate Replay's local privacy guarantees and make later exports unsafe by construction.

Git metadata also needs a strict scope. Branch, commit identity, detached state, and dirty state are reproducibility evidence. Remote URLs and changed-path lists are not required for the initial R2 fingerprint and can leak credentials, usernames, repository topology, or sensitive filenames.

## Decision

The Generic Recorder captures one versioned environment fingerprint after the initial workspace state has been verified and published, and before the recorded child process starts. The fingerprint is stored once per session in SQLite and advances the session from R1 to R2.

The fingerprint records the host OS and architecture, Replay's Go runtime version, a deterministic view of the effective child environment, and a constrained Git context. Environment names are deduplicated using the target platform's name semantics and sorted deterministically before encoding.

Credential-shaped environment variable names are replaced with `[REDACTED]` before persistence. This includes password, token, secret, credential, cookie, authorization, DSN, API/access/private/session/signing/encryption key conventions, plus common connection-URL variables. Values under otherwise ordinary names still pass through Replay's known-secret content redactor. Redaction status is preserved as metadata; the original secret value is never written to the metadata database.

Git capture is local and read-only. Replay records only whether Git is available, whether the workspace is inside a repository, HEAD, branch or detached state, and whether the worktree is dirty when that can be determined. It does not persist remote URLs or changed filenames in this fingerprint. Git probes disable terminal prompting, optional locks, fsmonitor, and untracked-cache acceleration where applicable and are bounded by a short timeout.

A compact `session.environment` event records capture statistics, while `git.context` records the constrained Git metadata. The full privacy-filtered fingerprint remains in the `environments` table. If environment capture or persistence fails, the recorder aborts rather than claiming R2.

## Consequences

- R2 has a concrete, versioned persistence boundary instead of being inferred from ad-hoc events.
- Common secrets are not stored in plaintext environment metadata.
- The environment representation is deterministic enough for later comparison and export.
- Git context is useful without copying repository remotes or changed-path names into metadata.
- Secret detection is deliberately conservative but cannot prove that arbitrary user-defined variable names contain no sensitive material; export secret scanning remains a separate defense.
- Toolchain and dependency fingerprints can extend the environment schema later without changing the meaning of the initial R2 boundary.
