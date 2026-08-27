# ADR-017: Configuration layering and workspace ignore semantics

- Status: Accepted
- Date: 2026-08-27

## Context

Replay needs deterministic configuration resolution before the recorder becomes a public execution surface. Architecture v1.0 defines the precedence CLI flags > project `.rappid/replay.yaml` > user `~/.config/rappidAI/replay/config.yaml` > defaults, requires secrets to stay out of project configuration, and relies on default exclusions for large generated directories while never copying `.git` into Replay's own state store.

Configuration layering must distinguish an omitted value from an explicit zero value. In particular, `intelligence.enabled: false` and `record.ignore: []` must be able to override lower-precedence layers. Ignore matching must also produce the same result on macOS, Linux, and Windows instead of inheriting shell-specific glob behavior.

## Decision

Use `go.yaml.in/yaml/v3` v3.0.5 for YAML parsing. Configuration files are parsed strictly with unknown-field rejection, are limited to one YAML document and 1 MiB, and are merged only through sparse pointer-backed override structures.

The effective configuration is resolved in this order:

1. architecture defaults;
2. user config at `~/.config/rappidAI/replay/config.yaml`;
3. project config at `.rappid/replay.yaml` relative to the working directory;
4. CLI overrides.

A field that is absent in a higher layer leaves the lower value unchanged. A field that is explicitly present replaces the lower value. `record.ignore` is replaced as one complete list rather than concatenated, including when the higher layer explicitly supplies an empty list.

Replay's ignore language is deliberately smaller than Git ignore syntax:

- logical workspace paths always use `/` separators;
- `*`, `?`, and bracket classes match within one path component;
- `**` is valid only as a complete path component and matches zero or more components;
- a leading `/` anchors a pattern at the workspace root;
- otherwise a pattern can match at any path depth;
- a trailing `/` restricts a pattern to directories;
- negation with `!` is rejected in v1 rather than being partially implemented.

`.git` is a reserved exclusion at every depth and cannot be re-included by user configuration. This is independent of the configurable ignore list because Replay's CAS must never become a second copy of Git's internal object database.

No secret-bearing fields are part of the project configuration schema. Secret/key management remains behind the dedicated credential-store boundary.

## Consequences

Configuration behavior is inspectable, deterministic, and testable before recorder work starts. Explicit `false` and empty-list overrides behave correctly. Invalid or misspelled YAML keys fail closed instead of silently changing capture behavior. The ignore contract is portable and can later be extended only deliberately; adding negation or changing pattern semantics requires compatibility review because it changes which bytes enter a recorded state.
