# ADR-025: Adapter SDK v1 is additive and cannot gate generic recording

- Status: Accepted
- Date: 2026-08-28

## Context

Architecture v1.0 requires the Generic Recorder to remain the mandatory provider-independent baseline while agent adapters add richer semantics. Unknown agents must always be recordable. Adapter failure must therefore never become a prerequisite failure for deterministic terminal, process, filesystem, state, artifact, environment, or Git capture.

Replay also needs a stable semantic surface before the first Codex-specific adapter is implemented. Built-in adapters can use a Go API directly, but a future external plugin transport must not depend on Go's unstable plugin ABI.

## Decision

Replay defines adapter contract `rappid.replay.adapter/1` in public package `pkg/adapter`.

The v1 interface follows the Architecture v1.0 adapter surface:

- immutable adapter `Descriptor` (`id`, `version`);
- `Detect`;
- `Capabilities`;
- `EnrichProcess`;
- `StreamEvents`;
- `Environment`;
- `RedactionHints`.

Capability flags are exactly the baseline dimensions defined by Architecture v1.0: model calls, tool calls, messages, token usage, cost metadata, OpenTelemetry, and external I/O.

Adapter detection receives privacy-filtered command metadata rather than raw command secrets. Runtime hook contexts likewise expose the privacy-filtered command. Adapter events are proposals only: adapters do not allocate canonical sequence numbers, timestamps, state references, or write directly to Replay persistence.

A registry contains one mandatory fallback adapter and zero or more specialized adapters. Specialized matches are ranked by confidence; ties are resolved deterministically by adapter ID. Detection failures are retained as diagnostics and skipped. They do not prevent fallback selection.

The built-in `generic` adapter is the mandatory fallback. It declares no provider-specific capabilities and all enrichment hooks are no-ops. Its purpose is to make the adapter layer explicit without changing the Generic Recorder's guarantees.

The Generic Recorder remains the execution and evidence-capture engine even when a specialized adapter is selected. Session metadata stores the selected adapter ID and version. The `session.started` event records the adapter descriptor, detection result, and any non-fatal detection diagnostics while still identifying the recorder itself as `generic`.

## Compatibility

The Go types in `pkg/adapter` are a public semantic API. Changes that alter the meaning or required shape of contract v1 require a new contract version rather than silent reinterpretation.

Future out-of-process plugins should map their protocol messages onto this contract. They must not require loading arbitrary Go plugins into the Replay core process.

## Consequences

Codex and future adapters can be implemented without forking the recorder. A broken or unknown specialized adapter cannot make an otherwise recordable command unrecordable merely because detection failed.

The current recorder only performs adapter selection and persists adapter identity/detection metadata. Hook execution bridges for process enrichment, event streaming, adapter environment metadata, and redaction hints can be added incrementally while preserving the same v1 contract.

## Rejected alternatives

**Let each adapter replace the Generic Recorder.** Rejected because provider-specific integrations would become correctness dependencies.

**Fail the run when specialized detection fails.** Rejected because adapters are additive and unknown/broken integrations must fall back safely.

**Use Go `plugin` as the public adapter ABI.** Rejected because it is platform-limited and does not provide the stable cross-version isolation required for a public plugin ecosystem.

**Pass raw command/environment secrets into detection.** Rejected because adapter selection does not justify widening secret exposure.
