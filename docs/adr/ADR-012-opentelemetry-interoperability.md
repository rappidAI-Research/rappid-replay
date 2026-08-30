# ADR-012: OpenTelemetry GenAI interoperability is offline, additive, and version-pinned

- Status: Accepted
- Date: 2026-08-29

## Context

Architecture v1.0 accepts OpenTelemetry interoperability so Replay can exchange structured GenAI telemetry without making any agent-specific telemetry format canonical.

The upstream OpenTelemetry GenAI semantic conventions are currently marked Development and have moved into the separate `open-telemetry/semantic-conventions-genai` repository. They therefore cannot be treated as a stable wire contract without an explicit compatibility point.

Replay is local-first. Deterministic recording must not silently acquire a network dependency or telemetry egress path merely because OpenTelemetry support is present. Replay's append-only event history, state graph, and verified objects also remain the authoritative evidence; OpenTelemetry is an interoperability representation, not a replacement evidence model.

## Decision

Replay implements OpenTelemetry GenAI interoperability as an offline OTLP/JSON mapping layer.

The first compatibility point is pinned to:

- OpenTelemetry core semantic conventions `1.44.0`;
- `open-telemetry/semantic-conventions-genai` commit `67dff024110be5bd9f318006e733f4078e0f4c97`.

The pin is recorded in exported telemetry as Replay metadata. Because the upstream GenAI conventions are Development, changing the compatibility point requires fixture-backed compatibility review and an explicit implementation update rather than silent reinterpretation.

OTLP JSON follows protobuf JSON conventions relevant to traces: lower-camel-case field names, trace and span identifiers encoded as hexadecimal strings, enum values encoded numerically, and 64-bit integer values encoded as decimal strings. Unknown OTLP JSON fields are ignored on import. Input is bounded by document, span, attribute, event, and nested-value limits before semantic mapping.

Replay does not start a collector, exporter, listener, sidecar, or network client by default. The interoperability layer accepts and returns local OTLP/JSON bytes. Any future live OTLP network transport must be an explicit, separately configured feature with normal Replay privacy and egress controls.

### Replay to OpenTelemetry

Replay exports only semantics supported by recorded evidence:

- one `invoke_agent` INTERNAL span represents an observed specialized-agent run;
- `agent.tool_call` plus matching `agent.tool_result` events become `execute_tool {gen_ai.tool.name}` INTERNAL child spans;
- supported token counters map to current `gen_ai.usage.*` attributes;
- conversation, model, provider, and reasoning-effort metadata are mapped when actually observed;
- point observations such as assistant messages remain Replay-namespaced span events when no upstream span boundary is known.

Replay does not fabricate inference spans merely because assistant output exists. A model-call span is only appropriate when a future adapter or imported telemetry provides an actual model-call boundary.

Synthetic OpenTelemetry trace and span identifiers created during export are deterministic functions of Replay session/event identity. They exist for stable interoperability output and are not evidence hashes or replacements for Replay IDs.

### OpenTelemetry to Replay

Imported GenAI spans become additive `agent.otel.span` event drafts. Recognized current GenAI semantics may additionally normalize into Replay `agent.message`, `agent.tool_call`, `agent.tool_result`, and `agent.usage` drafts. Import never allocates canonical sequence numbers, rewrites state, or writes persistence directly; the normal Replay event appender retains sequence, timestamp-order, privacy, and durability ownership.

Arbitrary imported OpenTelemetry attributes are treated conservatively as content because instrumentation attributes may contain user, source-code, customer, prompt, tool, or response data.

### Content and privacy

GenAI message bodies, system instructions, tool definitions, tool arguments, and tool results are content-bearing fields. Replay excludes those fields from interoperability mapping by default. Content transfer requires an explicit opt-in. Metadata-only import/export must remain useful without carrying prompt, response, tool-argument, or tool-result bodies.

The interoperability layer never exposes private chain-of-thought or creates a dependency on private reasoning. Reasoning token counts and requested reasoning level are metadata and may be mapped when explicitly observed; reasoning text is not.

## Consequences

Replay can exchange useful GenAI telemetry with OpenTelemetry tooling while retaining its deterministic local evidence model and local-first security posture. The mapping is deliberately conservative where Replay evidence is less granular than OpenTelemetry's span model.

The implementation does not require the OpenTelemetry Go SDK, protobuf runtime, or a collector in the deterministic core. A small bounded OTLP/JSON compatibility model keeps the dependency graph and implicit side effects minimal.

Because upstream GenAI conventions are still Development, interoperability tests and this pinned snapshot are part of the compatibility contract until a stable upstream release is adopted.

## Rejected alternatives

**Enable a live OTLP exporter automatically.** Rejected because it would introduce implicit network egress and make a local deterministic feature depend on collector availability.

**Treat OpenTelemetry as Replay's canonical event store.** Rejected because OTLP telemetry does not encode Replay's byte-exact state graph, CAS integrity, branch lineage, or restoration guarantees.

**Invent model spans from assistant messages.** Rejected because message observations do not prove model-call start/end boundaries and would overstate the evidence.

**Export all GenAI content by default.** Rejected because current OpenTelemetry content attributes are explicitly sensitive/opt-in and this would conflict with Replay's privacy-by-architecture principle.
