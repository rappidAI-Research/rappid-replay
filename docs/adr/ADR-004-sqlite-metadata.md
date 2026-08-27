# ADR-004: SQLite for metadata and the event index

- Status: Accepted
- Date: 2026-08-27

## Decision

Use SQLite as the local transactional store for session metadata, ordered event metadata, state references, indexes, migrations, and related control-plane data. Use WAL mode for the daemon/UI reader-writer path.

Object contents remain outside SQLite in the dedicated content-addressed store.

## Consequences

Schema migrations are versioned. Transactions may reference only objects that have already been durably persisted and verified.
