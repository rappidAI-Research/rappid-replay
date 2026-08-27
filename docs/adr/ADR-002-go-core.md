# ADR-002: Go as the core implementation language

- Status: Accepted
- Date: 2026-08-27

## Context

Replay needs a portable systems-oriented core for process control, filesystem recording, local persistence, a CLI, and an embedded local HTTP/UI server across macOS, Linux, and Windows.

## Decision

Use Go for the Replay core. The repository currently targets Go 1.27, the stable Go release at project bootstrap. Internal packages are not a stable public API.

## Consequences

Platform-specific behavior is isolated behind explicit abstractions. Public compatibility is provided through versioned formats, schemas, CLI JSON, the local HTTP API, and the adapter surface rather than internal Go packages.
