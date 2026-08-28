# rappidAI Replay

Open infrastructure for reproducible AI-agent execution.

`rappidAI Replay` is a local-first, open-source versioning and reproducibility system for AI-agent runs. It records executions, reconstructs technical workspace states, branches historical runs, re-executes them under controlled conditions, and compares alternative execution paths.

The deterministic core must remain fully functional without AI. Local AI is an optional, replaceable, read-only intelligence layer for summaries, semantic grouping, divergence explanations, and run labels.

## Status

Early implementation. Architecture baseline: Replay Architecture v1.0 (27 August 2026).

Track A (Foundations) is implemented and hardened: versioned event/state contracts, UUIDv7 identities, SQLite metadata with migration integrity, recursive Merkle states, deterministic large-file chunking, encrypted BLAKE3-addressed CAS storage using zstd + XChaCha20-Poly1305, OS-backed master-key management, configuration layering, portability validation, corruption quarantine, and hardened cross-platform CI.

Track B (Recorder) is now in progress. The first Generic Recorder execution path records a child command with exact initial/final workspace states, process lifecycle events, stdout/stderr byte events, durable session completion/abort semantics, and the CLI entry point below. PTY semantics, filesystem reconciliation checkpoints, process-tree discovery, and richer environment/artifact capture are follow-up Track B work.

## Recording a command

```sh
go run ./cmd/rappid replay record -- <command> [args...]
```

For example:

```sh
go run ./cmd/rappid replay record -- git status
```

Use `--data-dir DIR` to override Replay's per-user local data directory, `--cwd DIR` to record another workspace, and `--json` for a machine-readable result. In JSON mode child output is routed to stderr so stdout remains valid JSON; the original stdout/stderr identity is still retained in recorded events.

The current Generic Recorder uses pipes rather than a PTY and records that limitation explicitly as `pty: false`. A configuration requesting full terminal-input byte capture is rejected instead of silently weakening the requested guarantee.

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

## Planned implementation

The implementation order is Foundations, Recorder, Restore/Verify, Branch/Rerun, Diff, Adapter SDK + Codex/OTel, `.rplay`, local UI, optional local intelligence, and hardening. Each stage builds on the final interfaces rather than a throwaway prototype.

## License

Apache License 2.0. Model weights and third-party artifacts retain their own upstream licenses.
