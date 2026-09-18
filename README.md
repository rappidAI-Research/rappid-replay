# rappidAI Replay

Open infrastructure for reproducible AI-agent execution.

`rappidAI Replay` is a local-first, open-source versioning and reproducibility system for AI-agent runs. It records executions, reconstructs technical workspace states, branches historical runs, supports explicitly authorized live re-execution, and compares alternative execution paths.

The deterministic core must remain fully functional without AI. Local AI is an optional, replaceable, read-only intelligence layer for summaries, semantic grouping, divergence explanations, and run labels.

## Status

Early implementation. Architecture baseline: Replay Architecture v1.0 (27 August 2026).

Track A (Foundations) is implemented and hardened: versioned event/state contracts, UUIDv7 identities, SQLite metadata with migration integrity, recursive Merkle states, deterministic large-file chunking, encrypted BLAKE3-addressed CAS storage using zstd + XChaCha20-Poly1305, OS-backed master-key management, configuration layering, portability validation, corruption quarantine, and hardened cross-platform CI.

The Generic Recorder now captures command/process/workspace state, reconciled filesystem events, environment and artifact metadata, sampled process trees, and optional PTY sessions. The adapter SDK and Codex local-rollout adapter enrich the mandatory recorder; they do not replace its evidence.

The current CLI also implements [authenticated state verification and staged restore](docs/adr/ADR-028-verified-staged-restore.md), [exact-state branching and explicit live rerun](docs/adr/ADR-029-exact-branch-live-rerun.md), and [deterministic multi-dimensional session diff](docs/adr/ADR-030-deterministic-multidimensional-diff.md). These are experimental implementation capabilities, not a guarantee of identical outcomes or captured external services.

Only **live** rerun is implemented. `recorded`, `controlled`, and `hybrid` modes fail closed; external-I/O cassettes and stronger reproducibility levels remain future work. No `.rplay` package, local UI, or optional intelligence layer is claimed as implemented on main.

## Recording a command

```sh
go run ./cmd/rappid replay record -- <command> [args...]
```

For example:

```sh
go run ./cmd/rappid replay record -- git status
```

Use `--data-dir DIR` to override Replay's per-user local data directory, `--cwd DIR` to record another workspace, and `--json` for a machine-readable result. In JSON mode child output is routed to stderr so stdout remains valid JSON; the original stdout/stderr identity is still retained in recorded events.

`--pty auto|on|off` selects the terminal mode. Auto requires interactive input and output; non-interactive recording retains the pipe fallback and records `pty: false`. Full terminal-input capture requires a PTY. See the [terminal recording contract](docs/adr/ADR-023-cross-platform-pty-recording.md) for platform and evidence boundaries.

## Inspecting and re-executing recorded state

```sh
go run ./cmd/rappid replay verify <state-id>
go run ./cmd/rappid replay restore --to <new-directory> <state-id>
go run ./cmd/rappid replay branch --to <new-branch-directory> <state-id>
go run ./cmd/rappid replay diff <left-session-id> <right-session-id>
```

Verification does not execute code; restore and branch materialize a verified state. State IDs and session IDs are different identifiers. A live rerun executes a new, explicitly supplied command and can have external side effects:

```sh
go run ./cmd/rappid replay rerun --mode live --confirm-execution <state-id> -- <command> [args...]
```

Replay checks that the new branch has the selected source state's exact root before execution. It does not reconstruct secrets or silently reuse redacted historical arguments. The new session retains parent/fork lineage; live services and other external inputs can still change.

## Core principles

- deterministic core
- local-first operation
- exact data with explicit reproducibility guarantees
- append-only history
- generic recorder as the mandatory baseline
- adapters enrich, never gate
- privacy by architecture
- open formats and stable schemas
- crash-safe recording
- no hidden cloud economics

## Remaining architecture work

Portable `.rplay` export/import, recorded playback and controlled external-I/O cassettes, a local UI, optional local intelligence, broader adapters, and further hardening remain distinct from the current CLI. The [ADRs](docs/adr/README.md) and [adapter SDK](docs/adapter-sdk.md) document the implemented contracts and limits.

## License

Apache License 2.0. Model weights and third-party artifacts retain their own upstream licenses.
