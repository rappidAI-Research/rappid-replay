package persistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/rappidAI-Research/rappid-replay/internal/event"
	"github.com/rappidAI-Research/rappid-replay/internal/id"
)

// SessionTerminalStatus is a terminal recording outcome persisted on sessions.
type SessionTerminalStatus string

const (
	SessionCompleted SessionTerminalStatus = "completed"
	SessionAborted   SessionTerminalStatus = "aborted"
)

// SessionEnd describes the final append-only event and status transition for a
// recording session. Completed sessions require their already-published final
// state. Aborted sessions may point at the latest state that was safely captured.
type SessionEnd struct {
	SessionID   id.SessionID
	Status      SessionTerminalStatus
	EndedAt     time.Time
	MonotonicNS uint64
	StateID     id.StateID
	Source      string
	ExitCode    *int
	Reason      string
}

// EndSession atomically appends session.completed/session.aborted and changes
// the session out of recording state. A failed transaction leaves the session
// recording and does not consume an event sequence number.
func (db *DB) EndSession(ctx context.Context, end SessionEnd) (event.Event, error) {
	if _, err := id.ParseSession(end.SessionID.String()); err != nil {
		return event.Event{}, fmt.Errorf("invalid session id: %w", err)
	}
	if end.EndedAt.IsZero() {
		return event.Event{}, fmt.Errorf("session end time is required")
	}
	if end.MonotonicNS > maxSQLiteInteger {
		return event.Event{}, fmt.Errorf("monotonic timestamp exceeds SQLite INTEGER range")
	}
	if strings.TrimSpace(end.Source) == "" {
		return event.Event{}, fmt.Errorf("session end source is required")
	}
	if end.StateID != "" {
		if _, err := id.ParseState(end.StateID.String()); err != nil {
			return event.Event{}, fmt.Errorf("invalid session end state id: %w", err)
		}
	}
	if end.Status != SessionCompleted && end.Status != SessionAborted {
		return event.Event{}, fmt.Errorf("invalid terminal session status %q", end.Status)
	}

	tx, err := db.sql.BeginTx(ctx, nil)
	if err != nil {
		return event.Event{}, fmt.Errorf("begin session end: %w", err)
	}
	rollback := true
	defer func() {
		if rollback {
			_ = tx.Rollback()
		}
	}()

	var currentStatus string
	var finalState sql.NullString
	if err := tx.QueryRowContext(ctx,
		"SELECT status, final_state_id FROM sessions WHERE id = ?", end.SessionID.String(),
	).Scan(&currentStatus, &finalState); err != nil {
		if err == sql.ErrNoRows {
			return event.Event{}, fmt.Errorf("session %s does not exist", end.SessionID)
		}
		return event.Event{}, fmt.Errorf("read session before ending: %w", err)
	}
	if currentStatus != "recording" {
		return event.Event{}, fmt.Errorf("session %s is %q and cannot end", end.SessionID, currentStatus)
	}

	if end.Status == SessionCompleted {
		if end.StateID == "" {
			return event.Event{}, fmt.Errorf("completed session requires final state")
		}
		if !finalState.Valid || finalState.String != end.StateID.String() {
			return event.Event{}, fmt.Errorf("completed session final state %s does not match published final state %q", end.StateID, finalState.String)
		}
	} else if end.StateID != "" {
		var count int
		if err := tx.QueryRowContext(ctx,
			"SELECT COUNT(1) FROM states WHERE id = ? AND session_id = ?", end.StateID.String(), end.SessionID.String(),
		).Scan(&count); err != nil {
			return event.Event{}, fmt.Errorf("validate aborted session state: %w", err)
		}
		if count != 1 {
			return event.Event{}, fmt.Errorf("aborted session state %s does not belong to session %s", end.StateID, end.SessionID)
		}
	}

	seq, err := claimEventSequence(ctx, tx, end.SessionID.String())
	if err != nil {
		return event.Event{}, err
	}
	if err := validateMonotonicOrder(ctx, tx, end.SessionID.String(), end.MonotonicNS); err != nil {
		return event.Event{}, err
	}

	payload, err := json.Marshal(struct {
		ExitCode *int   `json:"exit_code,omitempty"`
		Reason   string `json:"reason,omitempty"`
	}{ExitCode: end.ExitCode, Reason: end.Reason})
	if err != nil {
		return event.Event{}, fmt.Errorf("encode session end payload: %w", err)
	}
	eventType := "session.completed"
	if end.Status == SessionAborted {
		eventType = "session.aborted"
	}
	draft := event.NewDraft(
		end.SessionID.String(), eventType, end.Source, end.EndedAt,
		event.Privacy{Classification: "technical"}, payload,
	)
	if end.StateID != "" {
		draft.StateBefore = end.StateID.String()
		draft.StateAfter = end.StateID.String()
	}
	persisted, err := insertEventTx(ctx, tx, draft, seq, end.MonotonicNS)
	if err != nil {
		return event.Event{}, err
	}

	result, err := tx.ExecContext(ctx, `
UPDATE sessions
SET status = ?, ended_at = ?
WHERE id = ? AND status = 'recording'`,
		string(end.Status), end.EndedAt.UTC().Format(time.RFC3339Nano), end.SessionID.String(),
	)
	if err != nil {
		return event.Event{}, fmt.Errorf("persist session terminal status: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return event.Event{}, fmt.Errorf("read session terminal update result: %w", err)
	}
	if rows != 1 {
		return event.Event{}, fmt.Errorf("session %s terminal update affected %d rows, want 1", end.SessionID, rows)
	}

	if err := tx.Commit(); err != nil {
		return event.Event{}, fmt.Errorf("commit session end: %w", err)
	}
	rollback = false
	return persisted, nil
}
