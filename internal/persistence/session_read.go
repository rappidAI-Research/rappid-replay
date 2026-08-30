package persistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/rappidAI-Research/rappid-replay/internal/id"
)

var ErrSessionNotFound = errors.New("Replay session not found")

// SessionRecord is the immutable session metadata and current terminal status
// needed by branch, rerun, and future lineage views.
type SessionRecord struct {
	ID                   id.SessionID
	ParentSessionID      id.SessionID
	ForkEventSeq         uint64
	Status               string
	Command              []string
	CWD                  string
	StartedAt            time.Time
	EndedAt              time.Time
	InitialStateID       id.StateID
	FinalStateID         id.StateID
	ReproducibilityLevel string
	AdapterID            string
	AdapterVersion       string
}

// GetSession reads one session without changing evidence or execution state.
func (db *DB) GetSession(ctx context.Context, sessionID id.SessionID) (SessionRecord, error) {
	if db == nil || db.sql == nil {
		return SessionRecord{}, fmt.Errorf("Replay database is required")
	}
	if _, err := id.ParseSession(sessionID.String()); err != nil {
		return SessionRecord{}, fmt.Errorf("invalid session id: %w", err)
	}

	var rawID, status, cwd, startedAt, level string
	var commandJSON []byte
	var parentID, endedAt, initialState, finalState, adapterID, adapterVersion sql.NullString
	var forkEventSeq sql.NullInt64
	err := db.sql.QueryRowContext(ctx, `
SELECT id, parent_session_id, fork_event_seq, status, command_json, cwd, started_at,
       ended_at, initial_state_id, final_state_id, reproducibility_level,
       adapter_id, adapter_version
FROM sessions
WHERE id = ?`, sessionID.String()).Scan(
		&rawID, &parentID, &forkEventSeq, &status, &commandJSON, &cwd, &startedAt,
		&endedAt, &initialState, &finalState, &level, &adapterID, &adapterVersion,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return SessionRecord{}, fmt.Errorf("%w: %s", ErrSessionNotFound, sessionID)
	}
	if err != nil {
		return SessionRecord{}, fmt.Errorf("read session %s: %w", sessionID, err)
	}

	parsedID, err := id.ParseSession(rawID)
	if err != nil {
		return SessionRecord{}, fmt.Errorf("database contains invalid session id %q: %w", rawID, err)
	}
	var command []string
	if err := json.Unmarshal(commandJSON, &command); err != nil {
		return SessionRecord{}, fmt.Errorf("session %s contains invalid command JSON: %w", sessionID, err)
	}
	if len(command) == 0 || command[0] == "" {
		return SessionRecord{}, fmt.Errorf("session %s contains empty command", sessionID)
	}
	started, err := time.Parse(time.RFC3339Nano, startedAt)
	if err != nil {
		return SessionRecord{}, fmt.Errorf("session %s contains invalid started_at %q: %w", sessionID, startedAt, err)
	}

	record := SessionRecord{
		ID:                   parsedID,
		Status:               status,
		Command:              append([]string(nil), command...),
		CWD:                  cwd,
		StartedAt:            started,
		ReproducibilityLevel: level,
		AdapterID:            adapterID.String,
		AdapterVersion:       adapterVersion.String,
	}

	if parentID.Valid {
		parsedParent, err := id.ParseSession(parentID.String)
		if err != nil {
			return SessionRecord{}, fmt.Errorf("session %s contains invalid parent session id %q: %w", sessionID, parentID.String, err)
		}
		if !forkEventSeq.Valid || forkEventSeq.Int64 <= 0 {
			return SessionRecord{}, fmt.Errorf("session %s has parent lineage without a valid fork sequence", sessionID)
		}
		record.ParentSessionID = parsedParent
		record.ForkEventSeq = uint64(forkEventSeq.Int64)
	} else if forkEventSeq.Valid {
		return SessionRecord{}, fmt.Errorf("session %s has fork sequence without parent lineage", sessionID)
	}
	if endedAt.Valid {
		ended, err := time.Parse(time.RFC3339Nano, endedAt.String)
		if err != nil {
			return SessionRecord{}, fmt.Errorf("session %s contains invalid ended_at %q: %w", sessionID, endedAt.String, err)
		}
		record.EndedAt = ended
	}
	if initialState.Valid {
		parsed, err := id.ParseState(initialState.String)
		if err != nil {
			return SessionRecord{}, fmt.Errorf("session %s contains invalid initial state id %q: %w", sessionID, initialState.String, err)
		}
		record.InitialStateID = parsed
	}
	if finalState.Valid {
		parsed, err := id.ParseState(finalState.String)
		if err != nil {
			return SessionRecord{}, fmt.Errorf("session %s contains invalid final state id %q: %w", sessionID, finalState.String, err)
		}
		record.FinalStateID = parsed
	}
	return record, nil
}
