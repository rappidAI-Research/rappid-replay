# ADR-001: Project license

- Status: Accepted
- Date: 2026-08-27

## Context

Replay is intended as broadly reusable open infrastructure. The architecture baseline recommends Apache-2.0 because it is permissive and includes an explicit patent grant. Model weights must remain separately licensed.

## Decision

License Replay source code under Apache License 2.0. Do not repackage model weights under the Replay project license.

## Consequences

- The repository includes the canonical Apache License 2.0 text in `LICENSE`.
- Contributions to Replay source code are distributed under Apache-2.0 unless explicitly documented otherwise.
- Model weights and other third-party artifacts retain their own upstream licenses and are not relicensed by this project.
