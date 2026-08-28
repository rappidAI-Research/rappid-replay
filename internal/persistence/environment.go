package persistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/rappidAI-Research/rappid-replay/internal/id"
)

// StoreEnvironment stores the privacy-filtered execution environment captured
// for one recording session. Environment capture is published only after the
// initial exact workspace state exists, which raises the session from R1 to R2.
func (db *DB) StoreEnvironment(ctx context.Context, sessionID id.SessionID, fingerprint []byte) error {
	if _, err := id.ParseSession(sessionID.String()); err != nil {
		return fmt.Errorf("invalid session id: %w", err)
	}
	if len(fingerprint) == 0 || !json.Valid(fingerprint) {
		return fmt.Errorf("environment fingerprint must be valid JSON")
	}
	var decoded any
	if err := json.Unmarshal(fingerprint, &decoded); err != nil {
		return fmt.Errorf("decode environment fingerprint: %w", err)
	}
	if _, ok := decoded.(map[string]any); !ok {
		return fmt.Errorf("environment fingerprint must be a JSON object")
	}

	tx, err := db.sql.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin environment publication: %w", err)
	}
	rollback := true
	defer func() {
		if rollback {
			_ = tx.Rollback()
		}
	}()

	var status, level string
	var initialState sql.NullString
	if err := tx.QueryRowContext(ctx,
		"SELECT status, reproducibility_level, initial_state_id FROM sessions WHERE id = ?", sessionID.String(),
	).Scan(&status, &level, &initialState); err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("session %s does not exist", sessionID)
		}
		return fmt.Errorf("read session before environment publication: %w", err)
	}
	if status != "recording" {
		return fmt.Errorf("session %s is %q and cannot capture environment", sessionID, status)
	}
	if !initialState.Valid || initialState.String == "" {
		return fmt.Errorf("session %s must publish its initial state before environment capture", sessionID)
	}

	var existing int
	if err := tx.QueryRowContext(ctx,
		"SELECT COUNT(1) FROM environments WHERE session_id = ?", sessionID.String(),
	).Scan(&existing); err != nil {
		return fmt.Errorf("check existing environment fingerprint: %w", err)
	}
	if existing != 0 {
		return fmt.Errorf("session %s already has an environment fingerprint", sessionID)
	}
	if _, err := tx.ExecContext(ctx,
		"INSERT INTO environments(session_id, fingerprint_json) VALUES(?, ?)", sessionID.String(), fingerprint,
	); err != nil {
		return fmt.Errorf("insert environment fingerprint: %w", err)
	}

	if level == "R0" || level == "R1" {
		if _, err := tx.ExecContext(ctx,
			"UPDATE sessions SET reproducibility_level = 'R2' WHERE id = ?", sessionID.String(),
		); err != nil {
			return fmt.Errorf("advance session reproducibility to R2: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit environment publication: %w", err)
	}
	rollback = false
	return nil
}
