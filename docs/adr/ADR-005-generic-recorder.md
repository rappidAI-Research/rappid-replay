# ADR-005: Generic recorder is the mandatory baseline

- Status: Accepted
- Date: 2026-08-27

## Context

Agent APIs and telemetry surfaces vary and can change independently of Replay.

## Decision

Any unknown agent or command must remain recordable through the generic execution path. Agent adapters add structured model, message, tool, token, cost, OpenTelemetry, or external-I/O semantics when available, but never gate recording, restore, branching, rerun, diff, export, or verification.

## Consequences

Recorder correctness is defined by technical observations and confirmed state, not by availability of an agent-specific integration.
