# Architecture Decision Records

ADRs record durable technical decisions for rappidAI Replay. The Architecture v1.0 baseline is normative; these files make individual decisions reviewable alongside the code.

Status values: Proposed, Accepted, Superseded, Rejected.

Initial decisions are derived from the Architecture v1.0 baseline dated 27 August 2026.

## Current ADRs

- ADR-001 — Apache-2.0 project license — **Accepted**
- ADR-002 — Go as the core implementation language — **Accepted**
- ADR-003 — Dedicated encrypted content-addressed store — **Accepted**
- ADR-004 — SQLite for metadata and event indexing — **Accepted**
- ADR-005 — Generic Recorder as mandatory baseline — **Accepted**
- ADR-006 — Local-first; no rappidAI cloud required — **Accepted**
- ADR-012 — OpenTelemetry GenAI interoperability is offline, additive, and version-pinned — **Accepted**
- ADR-013 — Foundation library selections — **Accepted**
- ADR-014 — Canonical recursive state trees — **Accepted**
- ADR-015 — Verified state publication is atomic — **Accepted**
- ADR-016 — Content-defined chunking for large files — **Accepted**
- ADR-017 — Configuration layering and workspace ignore policy — **Accepted**
- ADR-018 — OS-backed local CAS master key — **Accepted**
- ADR-019 — Cross-platform path portability is validated before materialization — **Accepted**
- ADR-020 — Watcher-triggered reconciliation for intermediate checkpoints — **Accepted**
- ADR-021 — Privacy-filtered execution environment capture — **Accepted**
- ADR-022 — Cross-platform sampled process-tree discovery — **Accepted**
- ADR-023 — Cross-platform PTY recording — **Accepted**
- ADR-024 — Workspace artifact discovery is derived from published state transitions — **Accepted**
- ADR-025 — Adapter SDK v1 is additive and cannot gate generic recording — **Accepted**
- ADR-026 — Adapter hooks are additive, privacy-filtered, and failure-isolated — **Accepted**
- ADR-027 — Codex enrichment observes local rollout persistence without changing execution — **Accepted**
- ADR-028 — Restore verifies first and commits a staged tree — **Accepted**
- ADR-029 — Branch from an exact state and require explicit live-rerun consent — **Accepted**
- ADR-030 — Replay diff is deterministic, multi-dimensional, and read-only — **Accepted**
- ADR-031 — `.rplay` is a self-contained authenticated evidence container — **Accepted**

ADRs 007–011 remain defined by the Architecture v1.0 baseline and will be materialized when their implementation tracks are touched.
