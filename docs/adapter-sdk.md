# Adapter SDK

Replay adapters enrich the provider-independent Generic Recorder. They never replace it.

The public semantic contract is `rappid.replay.adapter/1` in `pkg/adapter`. Every run always has a selected adapter identity, with the built-in `generic` adapter acting as the mandatory fallback when no specialized adapter matches.

## Selection

A registry contains one fallback adapter plus optional specialized adapters. Replay passes privacy-filtered command metadata and the working directory to `Detect`. Matching specialized adapters are ranked by confidence. Equal scores are resolved deterministically by adapter ID. Detection errors become diagnostics and do not stop recording.

The selected descriptor is persisted on the session as `adapter_id` and `adapter_version`. `session.started` also records the descriptor, detection result, and any detection diagnostics.

## Contract

An adapter implements:

```go
type Adapter interface {
    Descriptor() Descriptor
    Detect(context.Context, DetectionInput) (Detection, error)
    Capabilities() Capabilities
    EnrichProcess(context.Context, RunContext, ProcessObservation) (ProcessEnrichment, error)
    StreamEvents(context.Context, RunContext, EventEmitter) error
    Environment(context.Context, RunContext) (EnvironmentMetadata, error)
    RedactionHints(context.Context, RunContext) ([]RedactionHint, error)
}
```

Capability flags are `model_calls`, `tool_calls`, `messages`, `token_usage`, `cost_metadata`, `otel`, and `external_io`.

Adapter events are proposals. The Replay core remains responsible for validation, timestamps, sequence numbers, privacy metadata, state references, and durable persistence. An adapter must never write canonical history directly.

## Generic adapter

`adapters/generic` is the mandatory fallback. It matches every run as confidence 0, declares no provider-specific capabilities, and implements every enrichment hook as a no-op. The deterministic recorder therefore continues to work without any proprietary agent integration.

## External plugins

The Go package defines semantics, not a long-term binary ABI. Future out-of-process plugins should map a versioned protocol over local IPC onto this contract. Replay should not rely on Go's `plugin` mechanism for public third-party integrations.
