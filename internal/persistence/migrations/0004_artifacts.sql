CREATE TABLE artifacts (
    id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    event_seq INTEGER NOT NULL CHECK (event_seq > 0),
    from_state_id TEXT NOT NULL REFERENCES states(id) ON DELETE CASCADE,
    state_id TEXT NOT NULL REFERENCES states(id) ON DELETE CASCADE,
    path_bytes BLOB NOT NULL CHECK (length(path_bytes) > 0),
    path_display TEXT NOT NULL,
    change_kind TEXT NOT NULL CHECK (change_kind IN ('created','modified','replaced')),
    discovery TEXT NOT NULL CHECK (discovery IN ('workspace-delta')),
    object_id TEXT NOT NULL REFERENCES objects(id) ON DELETE RESTRICT,
    previous_object_id TEXT REFERENCES objects(id) ON DELETE RESTRICT,
    mode INTEGER NOT NULL CHECK (mode >= 0),
    size INTEGER NOT NULL CHECK (size >= 0),
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    UNIQUE(session_id, state_id, path_bytes, object_id),
    FOREIGN KEY(session_id, event_seq) REFERENCES events(session_id, seq) ON DELETE CASCADE
);

CREATE INDEX artifacts_session_state_idx ON artifacts(session_id, state_id, path_display);
CREATE INDEX artifacts_object_idx ON artifacts(object_id);
