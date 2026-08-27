# ADR-006: Local-first core with no required rappidAI cloud

- Status: Accepted
- Date: 2026-08-27

## Decision

Recording, state restore, branching, rerun, diff, export, import, verification, and the local UI must not require a rappidAI cloud account or a rappidAI-paid inference service.

Local AI is optional, replaceable, and read-only with respect to the canonical historical record. Disabling or removing it must not break core tests or core functionality.
