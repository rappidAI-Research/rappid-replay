# ADR-026: Adapter hooks are additive, privacy-filtered, and failure-isolated

- Status: Accepted
- Date: 2026-08-29

## Context

ADR-025 introduced the semantic Adapter SDK while deliberately leaving its enrichment hooks disconnected from the Generic Recorder. Replay now needs to execute process enrichment, adapter event streams, adapter environment metadata, and redaction hints without weakening the deterministic recorder guarantees.

Adapters are not trusted to allocate event sequence numbers, publish state, write SQLite directly, weaken privacy policy, or decide whether generic evidence should exist. A specialized adapter can also be stale, buggy, or unavailable while the underlying command remains perfectly recordable.

## Decision

The Generic Recorder owns an internal adapter-hook bridge for the one adapter selected at session start.

The bridge executes the SDK hooks as follows:

- `RedactionHints` runs after `session.started` and before environment capture or child execution. Valid environment-name and literal hints can only add redaction. Built-in secret rules always remain active.
- `Environment` contributes namespaced string attributes through an `agent.environment` event.
- `EnrichProcess` receives privacy-filtered root and sampled-descendant process observations. Non-empty attributes are emitted as `agent.process.enriched` events.
- `StreamEvents` runs under a recorder-owned cancellable context while the child command runs. Proposed events are accepted only in the `agent.*` or `external.*` namespaces and are persisted through the recorder event sink, which retains canonical sequence and timestamp ownership.

Adapter-provided event payloads are limited to 1 MiB, must remain valid JSON after privacy filtering, and are classified as content. Adapter metadata values and error messages pass through the same built-in plus adapter-contributed literal redaction policy. Attribute keys must be namespaced by the selected adapter ID, and attribute/hint counts are bounded.

The recorder applies accepted redaction hints to captured environment values, terminal output, full-capture PTY input, adapter event payloads, adapter metadata, and adapter diagnostics. Hints cannot disable or override a built-in redaction rule.

Adapter hook failures are evidence-quality diagnostics, not recording failures. They are recorded as `agent.adapter.error` whenever persistence is healthy and generic capture continues. A failure of Replay's own canonical event persistence remains fatal because continuing after durable evidence storage fails would violate recorder guarantees.

The adapter event stream is cancelled after child execution and receives a bounded shutdown grace period. An adapter that ignores cancellation is diagnosed and cannot indefinitely block session finalization.

## Consequences

A Codex adapter and future provider-specific adapters can now add structured semantics without becoming part of the deterministic capture path. Unknown commands and broken specialized adapters remain recordable through the Generic Recorder.

The bridge intentionally does not expose state publication, CAS mutation, sequence allocation, or raw SQLite handles. Future out-of-process plugin transport must preserve these boundaries.

Adapter-specific richer privacy semantics may be added later, but the v1 bridge deliberately treats streamed adapter payloads conservatively as content.
