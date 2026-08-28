# ADR-023 — Cross-platform PTY recording

Status: Accepted

Date: 2026-08-28

## Context

The Architecture v1.0 baseline requires the Generic Recorder to preserve real terminal semantics for interactive agents instead of reducing every command to independent stdin/stdout/stderr pipes. Interactive programs inspect TTY state, terminal size, line discipline, and resize events; a pipe-only recorder cannot faithfully provide those execution conditions.

Replay also has to keep terminal evidence honest. A PTY combines the child stdout and stderr byte streams into one terminal output stream, and host-side input capture is subject to the configured privacy policy. The implementation must not label combined PTY bytes as exact independent stdout/stderr channels.

## Decision

Replay uses `github.com/aymanbagabas/go-pty` v0.2.3 behind `internal/terminal` for the platform-native pseudo-terminal implementation. The dependency is MIT-licensed and provides Unix PTYs plus Windows ConPTY. Host terminal detection, raw mode, and viewport discovery use `golang.org/x/term` v0.45.0.

`rappid replay record` supports `--pty auto|on|off`. `auto` enables PTY mode only when both the effective input and terminal-output destination are interactive terminals. `on` permits callers and tests to force PTY semantics even when the host endpoints are not TTY files; `off` preserves the non-interactive pipe fallback.

A PTY recording emits one combined `terminal.output` stream. It does not manufacture separate stdout/stderr identities. `terminal.opened` records the PTY mode, initial viewport when known, and input policy. Viewport changes are applied to the child PTY and recorded as `terminal.resized` events.

Terminal input follows the existing privacy policy:

- `off`: bytes are delivered to the child but no input event is persisted.
- `metadata-only`: only the delivered byte count is recorded.
- `full`: delivered bytes are stored as content after known-secret redaction; the event privacy metadata marks any redaction.

Full input capture is rejected for pipe mode because it would imply interactive terminal fidelity the pipe recorder does not provide.

The CLI places an interactive host input terminal into raw mode for the duration of a PTY recording and restores the previous state before reporting the final result. Terminal size changes are sampled locally and forwarded to the recorder. No network service is involved.

After the child exits, Replay drains PTY output before sealing the process boundary. If the platform does not present EOF within a bounded post-exit grace period, Replay records `terminal.drain` with `complete=false` before closing the PTY; it never silently presents a timed-out drain as complete evidence.

## Consequences

Interactive command behavior is materially closer to direct terminal execution on macOS, Linux, and Windows. Terminal playback has a single causally ordered PTY output channel, which is more faithful than attempting to reconstruct interleaving from separate pipes.

PTY output cannot be used to recover an exact stdout/stderr split because the terminal itself has already merged those channels. Consumers must treat `terminal.output` and the older pipe-mode `terminal.stdout`/`terminal.stderr` events as different evidence types.

Input capture remains privacy-sensitive. `metadata-only` stays the default policy; `full` is explicit and still subject to redaction. Secrets are never reconstructed from historical metadata.

The PTY library is isolated behind `internal/terminal` so the public Replay event, CLI JSON, adapter, and `.rplay` contracts do not depend on third-party Go APIs.
