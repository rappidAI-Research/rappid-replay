package persistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/rappidAI-Research/rappid-replay/internal/id"
)

// SessionStart contains the immutable metadata known before a recording starts.
// Branch lineage is all-or-nothing: ParentSessionID, ForkEventSeq, and
// ForkStateID must either all be absent or identify one already-published state
// in the parent session. ForkStateID is validation input; parent_session_id and
// fork_event_seq are the canonical durable lineage stored on the session row.
type SessionStart struct {
	ID                   id.SessionID
	Command              []string
	CWD                  string
	StartedAt            time.Time
	ReproducibilityLevel string
	AdapterID            string
	AdapterVersion       string
	ParentSessionID      id.SessionID
	ForkEventSeq         uint64
	ForkStateID          id.StateID
}

// CreateSession inserts a new recording session. Runtime events and states are
// appended by later transactional operations; this row is never silently reused.
// Branched sessions are accepted only when their fork tuple resolves to an
// immutable published state in the declared parent session.
func (db *DB) CreateSession(ctx context.Context, start SessionStart) error {
	if db == nil || db.sql == nil {
		return fmt.Errorf("Replay database is required")
	}
	if _, err := id.ParseSession(start.ID.String()); err != nil {
		return fmt.Errorf("invalid session id: %w", err)
	}
	if len(start.Command) == 0 || start.Command[0] == "" {
		return fmt.Errorf("session command is required")
	}
	if start.CWD == "" {
		return fmt.Errorf("session cwd is required")
	}
	if start.StartedAt.IsZero() {
		return fmt.Errorf("session start time is required")
	}
	level := start.ReproducibilityLevel
	if level == "" {
		level = "R0"
	}
	if !validReproducibilityLevel(level) {
		return fmt.Errorf("invalid reproducibility level %q", level)
	}

	hasParent := start.ParentSessionID != ""
	hasForkSeq := start.ForkEventSeq != 0
	hasForkState := start.ForkStateID != ""
	if hasParent != hasForkSeq || hasParent != hasForkState {
		return fmt.Errorf("branch lineage requires parent session, fork event sequence, and fork state together")
	}
	if start.ForkEventSeq > maxSQLiteInteger {
		return fmt.Errorf("fork event sequence exceeds SQLite INTEGER range")
	}
	if hasParent {
		if _, err := id.ParseSession(start.ParentSessionID.String()); err != nil {
			return fmt.Errorf("invalid parent session id: %w", err)
		}
		if start.ParentSessionID == start.ID {
			return fmt.Errorf("session cannot branch from itself")
		}
		if _, err := id.ParseState(start.ForkStateID.String()); err != nil {
			return fmt.Errorf("invalid fork state id: %w", err)
		}
	}

	commandJSON, err := json.Marshal(start.Command)
	if err != nil {
		return fmt.Errorf("encode session command: %w", err)
	}

	tx, err := db.sql.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin session creation: %w", err)
	}
	rollback := true
	defer func() {
		if rollback {
			_ = tx.Rollback()
		}
	}()

	if hasParent {
		var parentStatus string
		if err := tx.QueryRowContext(ctx,
			"SELECT status FROM sessions WHERE id = ?", start.ParentSessionID.String(),
		).Scan(&parentStatus); err != nil {
			if err == sql.ErrNoRows {
				return fmt.Errorf("parent session %s does not exist", start.ParentSessionID)
			}
			return fmt.Errorf("read parent session %s: %w", start.ParentSessionID, err)
		}

		var count int
		if err := tx.QueryRowContext(ctx, `
SELECT COUNT(1)
FROM states
WHERE id = ? AND session_id = ? AND event_seq = ?`,
			start.ForkStateID.String(), start.ParentSessionID.String(), int64(start.ForkEventSeq),
		).Scan(&count); err != nil {
			return fmt.Errorf("validate fork state %s: %w", start.ForkStateID, err)
		}
		if count != 1 {
			return fmt.Errorf("fork state %s is not parent session %s at event sequence %d",
				start.ForkStateID, start.ParentSessionID, start.ForkEventSeq)
		}
	}

	_, err = tx.ExecContext(ctx, `
INSERT INTO sessions(
    id, parent_session_id, fork_event_seq, status, command_json, cwd, started_at,
    reproducibility_level, adapter_id, adapter_version
) VALUES(?, NULLIF(?, ''), NULLIF(?, 0), 'recording', ?, ?, ?, ?, NULLIF(?, ''), NULLIF(?, ''))`,
		start.ID.String(), start.ParentSessionID.String(), int64(start.ForkEventSeq), commandJSON,
		start.CWD, start.StartedAt.UTC().Format(time.RFC3339Nano), level,
		start.AdapterID, start.AdapterVersion,
	)
	if err != nil {
		return fmt.Errorf("create session %s: %w", start.ID, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit session %s creation: %w", start.ID, err)
	}
	rollback = false
	return nil
}

func validReproducibilityLevel(level string) bool {
	switch level {
	case "R0", "R1", "R2", "R3", "R4":
		return true
	default:
		return false
	}
}
