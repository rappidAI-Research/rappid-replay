# ADR-027: Codex enrichment observes local rollout persistence without changing execution

- Status: Accepted
- Date: 2026-08-29

## Context

Replay's first specialized adapter must add useful Codex semantics while preserving ADR-005, ADR-025, and ADR-026: the Generic Recorder remains the authoritative deterministic capture engine and adapters are additive, privacy-filtered, and failure-isolated.

Codex exposes richer first-class integration through App Server, but replacing a user's ordinary `codex` invocation with another execution mode would change the program being recorded. Codex also persists local thread metadata and JSONL rollout history beneath `CODEX_HOME`. That local evidence can be observed without modifying the child command or introducing network dependencies.

Codex rollout schemas are an upstream implementation surface and can evolve independently of Replay. Correlation is also not universally provable when multiple Codex threads in the same working directory are active because the local state database does not provide a process identifier that Replay can bind to the recorded child.

## Decision

Replay ships a built-in `codex` adapter selected only for conservative, explicit Codex CLI invocations, including the direct executable and known package-runner forms. Detection never uses broad substring matching.

The adapter does not launch Codex, rewrite arguments, inject environment variables, consume App Server, or write Codex state. It observes the existing local Codex state database and rollout JSONL files on a best-effort basis.

Correlation follows fail-closed enrichment rules:

- the state database is opened read-only;
- candidate threads must match the recorded working directory and a narrow run-start time window;
- an explicit resume thread ID is preferred when present;
- if more than one plausible thread remains, structured enrichment stops with an adapter diagnostic rather than guessing;
- a rollout path must be an absolute regular file under `CODEX_HOME/sessions`, including after symlink resolution, before Replay reads it.

The adapter emits a small normalized semantic surface:

- `agent.codex.session` for non-secret thread identity and model/provider metadata;
- `agent.message` for assistant output text;
- `agent.tool_call` and `agent.tool_result` for supported plaintext tool records;
- `agent.usage` for allowlisted numeric token-usage fields.

Unknown rollout record types are ignored so upstream additive schema changes do not gate recording. Normalized text is bounded before entering the recorder bridge.

Private reasoning is never adapter evidence. Rollout `reasoning`, compaction, and context-compaction records are ignored completely. Encrypted reasoning or encrypted function arguments are never copied into normalized events. User, developer, and system messages are not emitted by the Codex adapter. Generic terminal/process/filesystem evidence remains independent of these semantic events.

The adapter declares messages, tool calls, and token usage only. It does not yet claim model-call, cost, OpenTelemetry, or external-I/O capabilities.

## Consequences

Ordinary Codex runs gain structured local semantic events while remaining fully recordable when Codex local persistence is missing, ambiguous, incompatible, or unavailable. Those failures are handled by the ADR-026 bridge as non-gating adapter diagnostics.

The local rollout observer is intentionally narrower than a future direct App Server integration. If Replay later offers an explicit App Server recording mode, it must be a separate user-selected execution path rather than a silent adapter substitution.

OpenTelemetry GenAI mapping remains a separate follow-up and does not alter this decision.
