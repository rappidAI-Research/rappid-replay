# rappidAI Replay

Open infrastructure for reproducible AI-agent execution.

`rappidAI Replay` is a local-first, open-source versioning and reproducibility system for AI-agent runs. It records executions, reconstructs technical workspace states, branches historical runs, re-executes them under controlled conditions, and compares alternative execution paths.

The deterministic core must remain fully functional without AI. Local AI is an optional, replaceable, read-only intelligence layer for summaries, semantic grouping, divergence explanations, and run labels.

## Status

Early implementation. Architecture baseline: Replay Architecture v1.0 (27 August 2026).

Track A (Foundations) is in progress. The repository now contains the initial event/state schemas, typed safety defaults, UUIDv7 session IDs, SQLite migrations, and an encrypted BLAKE3-addressed local object store using zstd + XChaCha20-Poly1305. OS credential-store integration and full state-tree materialization are not implemented yet.

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
