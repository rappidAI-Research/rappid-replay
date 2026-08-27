CREATE TABLE sessions (
    id TEXT PRIMARY KEY,
    parent_session_id TEXT REFERENCES sessions(id),
    fork_event_seq INTEGER,
    status TEXT NOT NULL CHECK (status IN ('recording','completed','aborted','recovered','degraded')),
    command_json BLOB NOT NULL,
    cwd TEXT NOT NULL,
    started_at TEXT NOT NULL,
    ended_at TEXT,
    initial_state_id TEXT,
    final_state_id TEXT,
    reproducibility_level TEXT NOT NULL DEFAULT 'R0' CHECK (reproducibility_level IN ('R0','R1','R2','R3','R4')),
    adapter_id TEXT,
    adapter_version TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    CHECK (
        (parent_session_id IS NULL AND fork_event_seq IS NULL) OR
        (parent_session_id IS NOT NULL AND fork_event_seq IS NOT NULL AND fork_event_seq > 0)
    )
);

CREATE INDEX sessions_parent_idx ON sessions(parent_session_id);

CREATE TABLE objects (
    id TEXT PRIMARY KEY,
    kind TEXT NOT NULL CHECK (kind IN ('blob','tree','chunk_list','link')),
    plaintext_size INTEGER NOT NULL CHECK (plaintext_size >= 0),
    stored_size INTEGER NOT NULL CHECK (stored_size >= 0),
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

CREATE TABLE states (
    id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    event_seq INTEGER,
    root_object_id TEXT NOT NULL REFERENCES objects(id),
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    UNIQUE(session_id, event_seq),
    CHECK (event_seq IS NULL OR event_seq > 0)
);

CREATE INDEX states_session_idx ON states(session_id, event_seq);

CREATE TABLE events (
    session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    seq INTEGER NOT NULL CHECK (seq > 0),
    wall_time_utc TEXT NOT NULL,
    monotonic_ns INTEGER NOT NULL CHECK (monotonic_ns >= 0),
    type TEXT NOT NULL,
    source TEXT NOT NULL,
    state_before TEXT,
    state_after TEXT,
    payload_json BLOB NOT NULL,
    privacy_json BLOB NOT NULL DEFAULT '{}',
    PRIMARY KEY(session_id, seq)
);

CREATE INDEX events_type_idx ON events(type);

CREATE TABLE environments (
    session_id TEXT PRIMARY KEY REFERENCES sessions(id) ON DELETE CASCADE,
    fingerprint_json BLOB NOT NULL,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);
