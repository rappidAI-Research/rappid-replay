# ADR-022 — Cross-platform sampled process-tree discovery

- Status: **Accepted**
- Date: 2026-08-28

## Context

The Generic Recorder must preserve the process relationships created beneath the command it launches. The root process has an exact lifecycle boundary because Replay owns the `os/exec` handle, but portable child-process start notifications are not available from one stable cross-platform primitive. Treating a polling observation as an exact start event would overstate the evidence Replay possesses.

Replay also must not persist descendant command lines by default: command lines routinely contain tokens, prompts, file paths, and other sensitive arguments. Parent-child identity and executable names are enough for the first process-tree layer and can be enriched later by explicit adapters or higher-fidelity platform collectors.

## Decision

Replay uses native read-only process-table snapshots on Tier-1 platforms and samples them while the recorded root process is running:

- Linux reads `/proc/<pid>/stat` and reconstructs PID, PPID, and the kernel command name.
- macOS reads `kern.proc.all` through `golang.org/x/sys/unix.SysctlKinfoProcSlice`.
- Windows uses the Tool Help process snapshot API through `golang.org/x/sys/windows`.

The platform snapshot is reduced to descendants of the recorded root process. Newly observed descendants are emitted as `process.discovered` events with `discovery: "sampled"`; Replay does not label them `process.started` because their exact creation time was not observed. Descendant command lines are not captured in this layer.

Sampling runs every 100 ms. A final sample is attempted when recording stops, then Replay emits a `process.tree` summary containing scan count, discovery count, scan errors, interval, and explicit `complete: false` / `short_lived_may_be_lost: true` markers. Platform enumeration errors are recorded as `process.discovery.error` and do not by themselves abort an otherwise reproducible R2 recording. Failure to persist process evidence remains fatal because continuing after losing the durable event stream would create an internally inconsistent timeline.

The root process continues to use the exact `process.started` and `process.exited` boundaries owned by the recorder.

## Consequences

- Replay gets a provider-independent process tree on macOS, Linux, and Windows without a new runtime dependency.
- The evidence model distinguishes exact root lifecycle boundaries from sampled descendant observations.
- Very short-lived descendants can be missed; the persisted summary states that limitation instead of claiming completeness.
- No descendant command-line secrets are copied into the metadata database by this layer.
- A future platform event collector may provide higher-fidelity child start/exit events, but it must remain additive and must not reinterpret historical sampled observations as exact events.
