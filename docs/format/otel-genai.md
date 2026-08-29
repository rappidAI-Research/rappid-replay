# OpenTelemetry GenAI interoperability

Replay's OpenTelemetry interoperability is an offline, additive representation of recorded evidence. It does not replace Replay's canonical event stream or state graph and it does not create network traffic by itself.

## Compatibility point

The current mapping targets OpenTelemetry core semantic conventions 1.44.0 and the Development GenAI conventions at `open-telemetry/semantic-conventions-genai@67dff024110be5bd9f318006e733f4078e0f4c97`.

Because the GenAI conventions are not yet stable, this snapshot is part of Replay's compatibility contract. An upstream update must be reviewed against fixtures and mapping tests before the pin changes.

## OTLP JSON envelope

`internal/otelgenai` reads and writes the OTLP `ExportTraceServiceRequest` JSON representation. The compatibility codec follows protobuf JSON rules used by OTLP: lower-camel-case field names, hexadecimal trace/span IDs, numeric enum values, decimal strings for 64-bit integers, and tolerance for unknown fields.

Parsing is bounded to 64 MiB per document, 100,000 spans, 512 attributes per attribute collection, 4,096 events per span, and 16 nested `AnyValue` levels. These limits protect local import from unbounded telemetry structures while remaining far above normal per-session telemetry sizes.

## Replay to OpenTelemetry

| Replay evidence | OpenTelemetry representation |
| --- | --- |
| specialized agent activity | one INTERNAL `invoke_agent {agent}` root span |
| `agent.codex.session` | conversation/model/provider/reasoning-level attributes when observed |
| latest `agent.usage` observation | supported `gen_ai.usage.*` attributes on the root span |
| `agent.tool_call` + matching `agent.tool_result` | INTERNAL `execute_tool {gen_ai.tool.name}` child span |
| `agent.message` | `rappid.replay.agent.message` event on the root span |
| Replay session identity | `rappid.replay.session.id` resource/span metadata |

Replay does not infer an inference/model-call span from an assistant message. A message proves that output was observed, not where a model call began or ended. A future adapter can export genuine inference spans when it records those boundaries directly.

Synthetic trace/span IDs are deterministic for stable repeated export. They are interoperability identifiers only; Replay object IDs, state IDs, event sequence, and integrity verification remain authoritative.

For Codex token observations, Replay exports the latest usage observation because Codex rollout `token_count` records are cumulative snapshots rather than independent per-call deltas.

## OpenTelemetry to Replay

Every recognized GenAI span can become an additive `agent.otel.span` draft containing its trace/span identity, operation, provider, timing, kind, and permitted attributes. Current recognized semantics are additionally normalized:

| OpenTelemetry semantic data | Replay draft |
| --- | --- |
| `execute_tool` span | `agent.tool_call` and, when a result attribute exists, `agent.tool_result` |
| supported `gen_ai.usage.*` attributes | `agent.usage` |
| `gen_ai.output.messages` | `agent.message` when content import is enabled |
| `gen_ai.client.inference.operation.details` output messages | `agent.message` when content import is enabled |
| Replay-exported `rappid.replay.agent.message` span event | `agent.message` |

The importer returns unstamped `event.Draft` values. It cannot allocate canonical sequence numbers or write the database. Normal Replay persistence remains responsible for event ordering and durability.

## Privacy

Content transfer is off by default in both directions. The following OpenTelemetry GenAI fields are treated as content-bearing and are excluded unless explicitly requested:

- `gen_ai.input.messages`
- `gen_ai.output.messages`
- `gen_ai.system_instructions`
- `gen_ai.tool.definitions`
- `gen_ai.tool.call.arguments`
- `gen_ai.tool.call.result`

Imported arbitrary telemetry attributes are conservatively classified as content because third-party instrumentation can place user data, source code, prompts, responses, or customer values in attributes. Reasoning text is never reconstructed or introduced by this mapping.

## Network behavior

This layer has no collector client and no automatic OTLP exporter. It transforms local bytes only. A future live collector transport must be separately configured and must preserve Replay's local-first privacy and egress guarantees.
