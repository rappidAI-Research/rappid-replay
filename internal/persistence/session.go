package persistence

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/rappidAI-Research/rappid-replay/internal/id"
)

// SessionStart contains the immutable metadata known before a recording starts.
type SessionStart struct {
	ID                   id.SessionID
	Command              []string
	CWD                  string
	StartedAt            time.Time
	ReproducibilityLevel string
	AdapterID            string
	AdapterVersion       string
}

// CreateSession inserts a new recording session. Runtime events and states are
// appended by later transactional operations; this row is never silently reused.
func (db *DB) CreateSession(ctx context.Context, start SessionStart) error {
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
	commandJSON, err := json.Marshal(start.Command)
	if err != nil {
		return fmt.Errorf("encode session command: %w", err)
	}
	_, err = db.sql.ExecContext(ctx, `
INSERT INTO sessions(
    id, status, command_json, cwd, started_at, reproducibility_level,
    adapter_id, adapter_version
) VALUES(?, 'recording', ?, ?, ?, ?, NULLIF(?, ''), NULLIF(?, ''))`,
		start.ID.String(), commandJSON, start.CWD, start.StartedAt.UTC().Format(time.RFC3339Nano),
		level, start.AdapterID, start.AdapterVersion,
	)
	if err != nil {
		return fmt.Errorf("create session %s: %w", start.ID, err)
	}
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
