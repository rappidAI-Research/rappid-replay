ALTER TABLE schema_migrations ADD COLUMN checksum TEXT;

ALTER TABLE sessions
ADD COLUMN next_event_seq INTEGER NOT NULL DEFAULT 1 CHECK (next_event_seq > 0);

ALTER TABLE sessions
ADD COLUMN degraded_reason TEXT;

ALTER TABLE events
ADD COLUMN parent_event_id TEXT;

ALTER TABLE events
ADD COLUMN span_id TEXT;

UPDATE sessions
SET next_event_seq = COALESCE(
    (SELECT MAX(events.seq) + 1 FROM events WHERE events.session_id = sessions.id),
    1
);
