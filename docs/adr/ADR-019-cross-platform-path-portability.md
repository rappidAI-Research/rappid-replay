# ADR-019: Cross-platform path portability is validated before materialization

- Status: Accepted
- Date: 2026-08-27

## Context

Replay state identity must preserve the exact source workspace representation. Canonical tree objects therefore retain raw filename bytes and do not normalize names to the rules of the machine that later reads an export.

That representation is intentionally more expressive than every Tier-1 target filesystem. A filename that is a single legal component on Linux can be reinterpreted or rejected on Windows. Examples include backslashes, colons, reserved Win32 device names, trailing periods/spaces, invalid UTF-8, and directory entries that differ only by case.

Silently normalizing any of those names during restore would violate byte-exact state semantics and could also turn one recorded path into another path on the destination.

## Decision

Canonical tree identity remains platform-neutral and lossless. Replay does not rewrite raw names to make a snapshot portable.

Before cross-platform materialization, Replay MUST validate the complete tree for the destination operating system. Validation is read-only and is separate from snapshot verification.

For Linux and Darwin, Replay applies the canonical component rules: a component must be non-empty, must not be `.` or `..`, and must not contain NUL or `/`.

For Windows, Replay additionally rejects components that:

- are not valid UTF-8;
- contain Win32-reserved characters `< > : " / \\ | ? *` or ASCII control characters;
- end in a space or period;
- use reserved device basenames such as `CON`, `PRN`, `AUX`, `NUL`, `CLOCK$`, `COM1`–`COM9`, or `LPT1`–`LPT9`, including extensions such as `NUL.txt`;
- exceed 255 UTF-16 code units;
- collide under conservative case-insensitive comparison with another entry in the same directory.

A portability failure aborts that materialization. Replay MUST NOT rename, escape, truncate, case-fold, or otherwise alter an evidence-bearing path to force the restore to succeed.

Filesystem-specific limitations beyond the operating-system contract may still make a restore impossible. Such failures are explicit; they do not change the recorded state.

Symlink targets remain recorded data and are not followed during validation or restore. Target-specific symlink creation capability is checked by the future materializer.

## Consequences

- Linux snapshots can retain names such as `a\\b` without changing state identity.
- The same snapshot can be rejected for Windows restore rather than being silently reinterpreted as multiple path components.
- `.rplay` import can preserve an otherwise valid snapshot even when the current host cannot materialize it; portability is a restore/materialization property, not an import-time mutation.
- Restore implementations must call the portability validator before writing destination paths.
