CREATE TABLE state_objects (
    state_id TEXT NOT NULL REFERENCES states(id) ON DELETE CASCADE,
    object_id TEXT NOT NULL REFERENCES objects(id) ON DELETE RESTRICT,
    PRIMARY KEY(state_id, object_id)
);

CREATE INDEX state_objects_object_idx ON state_objects(object_id);
