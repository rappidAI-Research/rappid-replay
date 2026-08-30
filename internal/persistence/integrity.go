package persistence

import (
	"context"
	"fmt"
	"strings"

	"github.com/rappidAI-Research/rappid-replay/internal/id"
)

// MarkSessionIntegrityDegraded records a durable integrity failure discovered by
// a later verify/restore operation. Unlike recorder-time degradation, this must
// also be able to transition completed or aborted sessions because evidence can
// become unreadable after the original run has ended.
func (db *DB) MarkSessionIntegrityDegraded(ctx context.Context, sessionID id.SessionID, reason string) error {
	if _, err := id.ParseSession(sessionID.String()); err != nil {
		return fmt.Errorf("invalid session id: %w", err)
	}
	if strings.TrimSpace(reason) == "" {
		return fmt.Errorf("degraded reason is required")
	}
	result, err := db.sql.ExecContext(ctx, `
UPDATE sessions
SET status = 'degraded', degraded_reason = ?
WHERE id = ? AND status IN ('recording', 'completed', 'aborted', 'recovered', 'degraded')`, reason, sessionID.String())
	if err != nil {
		return fmt.Errorf("mark session %s integrity-degraded: %w", sessionID, err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read integrity-degraded session update result: %w", err)
	}
	if rows != 1 {
		return fmt.Errorf("session %s cannot be marked integrity-degraded", sessionID)
	}
	return nil
}
