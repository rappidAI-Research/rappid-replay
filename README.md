# rappidAI Replay

Open infrastructure for reproducible AI-agent execution.

`rappidAI Replay` is a local-first, open-source versioning and reproducibility system for AI-agent runs. It records executions, reconstructs technical workspace states, branches historical runs, re-executes them under controlled conditions, and compares alternative execution paths.

The deterministic core must remain fully functional without AI. Local AI is an optional, replaceable, read-only intelligence layer for summaries, semantic grouping, divergence explanations, and run labels.

## Status

Early implementation. Architecture baseline: Replay Architecture v1.0 (27 August 2026).

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

The initial implementation track is Foundations: schemas, configuration, SQLite persistence, content-addressed storage, encryption, identifiers, and migrations. Recording, restore/verify, branch/rerun, diff, adapters, `.rplay`, UI, local intelligence, and hardening follow on top of those final interfaces.

## License

Apache-2.0. Model weights, if supported or downloaded by Replay, retain their own upstream licenses.
