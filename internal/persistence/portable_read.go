package persistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/rappidAI-Research/rappid-replay/internal/id"
	"github.com/rappidAI-Research/rappid-replay/internal/store"
)

// ArtifactRecord is immutable workspace-artifact provenance used by portable
// export. Path retains the exact raw workspace-relative bytes.
type ArtifactRecord struct {
	ID               id.ArtifactID
	SessionID        id.SessionID
	EventSeq         uint64
	FromStateID      id.StateID
	StateID          id.StateID
	Path             []byte
	PathDisplay      string
	ChangeKind       ArtifactChangeKind
	Discovery        string
	ObjectID         store.ObjectID
	PreviousObjectID store.ObjectID
	Mode             uint32
	Size             int64
}

// ListStates returns published states in canonical event-sequence order.
func (db *DB) ListStates(ctx context.Context, sessionID id.SessionID) ([]StateRecord, error) {
	if _, err := db.GetSession(ctx, sessionID); err != nil {
		return nil, err
	}
	rows, err := db.sql.QueryContext(ctx, `
SELECT id, event_seq, root_object_id, created_at
FROM states
WHERE session_id = ?
ORDER BY event_seq ASC`, sessionID.String())
	if err != nil {
		return nil, fmt.Errorf("list states for session %s: %w", sessionID, err)
	}
	defer rows.Close()
	result := make([]StateRecord, 0)
	for rows.Next() {
		var rawID, rawRoot, created string
		var seq int64
		if err := rows.Scan(&rawID, &seq, &rawRoot, &created); err != nil {
			return nil, fmt.Errorf("scan state for session %s: %w", sessionID, err)
		}
		stateID, err := id.ParseState(rawID)
		if err != nil {
			return nil, fmt.Errorf("invalid state id %q: %w", rawID, err)
		}
		if seq <= 0 {
			return nil, fmt.Errorf("state %s has invalid event sequence %d", stateID, seq)
		}
		rootID, err := store.ParseObjectID(rawRoot)
		if err != nil {
			return nil, fmt.Errorf("state %s has invalid root object id: %w", stateID, err)
		}
		parsed, err := parseSQLiteTime(created)
		if err != nil {
			return nil, fmt.Errorf("state %s has invalid created_at: %w", stateID, err)
		}
		result = append(result, StateRecord{ID: stateID, SessionID: sessionID, EventSeq: uint64(seq), RootTreeID: rootID, CreatedAt: parsed})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate states for session %s: %w", sessionID, err)
	}
	return result, nil
}

// GetEnvironment returns the persisted privacy-filtered environment fingerprint.
// ok is false when the session legitimately has no environment record.
func (db *DB) GetEnvironment(ctx context.Context, sessionID id.SessionID) (fingerprint json.RawMessage, ok bool, err error) {
	if _, err := db.GetSession(ctx, sessionID); err != nil {
		return nil, false, err
	}
	var raw []byte
	err = db.sql.QueryRowContext(ctx, "SELECT fingerprint_json FROM environments WHERE session_id = ?", sessionID.String()).Scan(&raw)
	if err == sql.ErrNoRows {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("read environment for session %s: %w", sessionID, err)
	}
	if len(raw) == 0 || !json.Valid(raw) {
		return nil, false, fmt.Errorf("session %s contains invalid environment JSON", sessionID)
	}
	return append(json.RawMessage(nil), raw...), true, nil
}

// ListArtifacts returns immutable artifact provenance in timeline order.
func (db *DB) ListArtifacts(ctx context.Context, sessionID id.SessionID) ([]ArtifactRecord, error) {
	if _, err := db.GetSession(ctx, sessionID); err != nil {
		return nil, err
	}
	rows, err := db.sql.QueryContext(ctx, `
SELECT id, event_seq, from_state_id, state_id, path_bytes, path_display,
       change_kind, discovery, object_id, previous_object_id, mode, size
FROM artifacts
WHERE session_id = ?
ORDER BY event_seq ASC, id ASC`, sessionID.String())
	if err != nil {
		return nil, fmt.Errorf("list artifacts for session %s: %w", sessionID, err)
	}
	defer rows.Close()
	result := make([]ArtifactRecord, 0)
	for rows.Next() {
		var rawID, fromState, toState, display, change, discovery, objectID string
		var previous sql.NullString
		var seq, mode, size int64
		var rawPath []byte
		if err := rows.Scan(&rawID, &seq, &fromState, &toState, &rawPath, &display, &change, &discovery, &objectID, &previous, &mode, &size); err != nil {
			return nil, fmt.Errorf("scan artifact for session %s: %w", sessionID, err)
		}
		artifactID, err := id.ParseArtifact(rawID)
		if err != nil {
			return nil, fmt.Errorf("invalid artifact id %q: %w", rawID, err)
		}
		fromID, err := id.ParseState(fromState)
		if err != nil {
			return nil, fmt.Errorf("artifact %s has invalid from state: %w", artifactID, err)
		}
		toID, err := id.ParseState(toState)
		if err != nil {
			return nil, fmt.Errorf("artifact %s has invalid state: %w", artifactID, err)
		}
		objID, err := store.ParseObjectID(objectID)
		if err != nil {
			return nil, fmt.Errorf("artifact %s has invalid object id: %w", artifactID, err)
		}
		var previousID store.ObjectID
		if previous.Valid {
			previousID, err = store.ParseObjectID(previous.String)
			if err != nil {
				return nil, fmt.Errorf("artifact %s has invalid previous object id: %w", artifactID, err)
			}
		}
		if seq <= 0 || mode < 0 || mode > int64(^uint32(0)) || size < 0 {
			return nil, fmt.Errorf("artifact %s contains invalid numeric metadata", artifactID)
		}
		if err := validateArtifactPath(rawPath); err != nil {
			return nil, fmt.Errorf("artifact %s path: %w", artifactID, err)
		}
		result = append(result, ArtifactRecord{
			ID: artifactID, SessionID: sessionID, EventSeq: uint64(seq), FromStateID: fromID, StateID: toID,
			Path: append([]byte(nil), rawPath...), PathDisplay: display, ChangeKind: ArtifactChangeKind(change), Discovery: discovery,
			ObjectID: objID, PreviousObjectID: previousID, Mode: uint32(mode), Size: size,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate artifacts for session %s: %w", sessionID, err)
	}
	return result, nil
}
