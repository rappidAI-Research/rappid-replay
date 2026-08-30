# rappidAI Replay

Open infrastructure for reproducible AI-agent execution.

`rappidAI Replay` is a local-first, open-source versioning and reproducibility system for AI-agent runs. It records executions, reconstructs technical workspace states, branches historical runs, re-executes them under controlled conditions, and compares alternative execution paths.

The deterministic core remains fully functional without AI. Local AI is an optional, replaceable, read-only intelligence layer for summaries, semantic grouping, divergence explanations, and run labels.

## Status

Architecture baseline: Replay Architecture v1.0 (27 August 2026).

The deterministic core now covers the main technical workflow through recording, reconstruction, branching, live rerun, comparison, and portable evidence exchange:

- hardened SQLite metadata/event persistence and encrypted BLAKE3-addressed CAS storage
- exact recursive workspace states with deterministic large-file chunking and cross-platform portability validation
- Generic Recorder execution with Unix PTY / Windows ConPTY support, reconciliation checkpoints, process/environment capture, and artifact provenance
- additive Adapter SDK, built-in Codex enrichment, and offline OpenTelemetry GenAI interoperability
- authenticated `verify` and staged atomic `restore`
- non-executing `branch` and explicitly confirmed exact-state `rerun --mode live`
- deterministic multi-dimensional `diff`
- versioned `.rplay` export, offline archive verification, and import work in Track G

Recorded playback and cassette-backed controlled/hybrid rerun modes are not yet implemented; unsupported modes fail closed instead of silently falling back to live execution.

## Core workflows

Record any command with the Generic Recorder:

```sh
go run ./cmd/rappid replay record -- <command> [args...]
```

Replay does not concatenate the recorded argv into an implicit shell command. Use `--pty auto|on|off` to control interactive terminal recording, `--data-dir DIR` to override the local Replay data root, `--cwd DIR` to select another workspace, and `--json` for machine-readable output.

Authenticate a published state without materializing it:

```sh
go run ./cmd/rappid replay verify <state-id>
```

Materialize an authenticated historical state into a new directory:

```sh
go run ./cmd/rappid replay restore <state-id> --to ./restored-run
```

Existing destinations require explicit `--force`. Restore verifies first, validates host pathname compatibility, builds a private sibling staging tree, and only then commits the completed tree.

Create a non-executing branch workspace:

```sh
go run ./cmd/rappid replay branch <state-id> --to ./branch-workspace
```

Start a new live child execution from an exact historical state:

```sh
go run ./cmd/rappid replay rerun --mode live --confirm-execution <state-id> -- <command> [args...]
```

Replay re-authenticates and materializes the selected state, re-captures it before execution, and refuses the live rerun unless the canonical initial root still matches the selected historical root.

Compare two immutable sessions:

```sh
go run ./cmd/rappid replay diff <left-session-id> <right-session-id>
```

The deterministic diff includes workspace state, timeline divergence, process/agent evidence, lineage, and outcome dimensions. Semantic AI is not part of canonical technical equality.

## Portable `.rplay` evidence

Export a sealed session together with the ancestor lineage and authenticated CAS objects required to reconstruct its states:

```sh
go run ./cmd/rappid replay export --out run.rplay <session-id>
```

The default export secret-scan policy comes from Replay configuration and can be overridden with `--secret-scan block|warn|off`. `--force` is required to replace an existing archive.

Verify a portable archive without opening the local Replay runtime or importing it:

```sh
go run ./cmd/rappid replay verify --archive run.rplay
```

Import a verified archive into the local Replay store:

```sh
go run ./cmd/rappid replay import run.rplay
```

`.rplay` contains canonical typed plaintext Replay objects, not local CAS ciphertext or OS-backed master keys. The format authenticates archive entries and object identities, rejects unsafe archive paths and unsupported required features, and preserves immutable session/state/event/artifact lineage. See [`docs/format/rplay-v1.md`](docs/format/rplay-v1.md) for the v1 interchange contract.

## Core principles

- deterministic core
- local-first operation
- exact data with explicit reproducibility guarantees
- append-only history
- Generic Recorder as the mandatory baseline
- adapters enrich, never gate
- privacy by architecture
- open formats and stable schemas
- crash-safe recording
- no hidden cloud economics

## Implementation tracks

Replay is developed against the Architecture v1.0 implementation tracks rather than as a disposable prototype: Foundations, Recorder, Restore/Verify, Branch/Rerun, Diff, Adapter SDK + Generic/Codex/OTel, `.rplay`, local UI, optional local intelligence, and hardening.

## License

Apache License 2.0. Model weights and third-party artifacts retain their own upstream licenses.
