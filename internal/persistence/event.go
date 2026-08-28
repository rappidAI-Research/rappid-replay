package persistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/rappidAI-Research/rappid-replay/internal/event"
	"github.com/rappidAI-Research/rappid-replay/internal/id"
)

var eventTypePattern = regexp.MustCompile(`^[a-z][a-z0-9_-]*\.[a-z][a-z0-9_.-]*$`)

// AppendEvent validates an event draft, atomically reserves the next strictly
// increasing session sequence, and persists the fully stamped envelope. State
// snapshots are intentionally excluded because they must be published together
// with their state/object metadata through PublishSnapshot.
func (db *DB) AppendEvent(ctx context.Context, draft event.Draft, monotonicNS uint64) (event.Event, error) {
	if err := validateEventDraft(draft, monotonicNS); err != nil {
		return event.Event{}, err
	}
	if draft.Type == "state.snapshot" {
		return event.Event{}, fmt.Errorf("state.snapshot events must be created through PublishSnapshot")
	}

	tx, err := db.sql.BeginTx(ctx, nil)
	if err != nil {
		return event.Event{}, fmt.Errorf("begin event append: %w", err)
	}
	rollback := true
	defer func() {
		if rollback {
			_ = tx.Rollback()
		}
	}()

	if err := validateEventStateReferences(ctx, tx, draft); err != nil {
		return event.Event{}, err
	}
	seq, err := claimEventSequence(ctx, tx, draft.SessionID)
	if err != nil {
		return event.Event{}, err
	}
	if err := validateMonotonicOrder(ctx, tx, draft.SessionID, monotonicNS); err != nil {
		return event.Event{}, err
	}
	persisted, err := insertEventTx(ctx, tx, draft, seq, monotonicNS)
	if err != nil {
		return event.Event{}, err
	}
	if err := tx.Commit(); err != nil {
		return event.Event{}, fmt.Errorf("commit event append: %w", err)
	}
	rollback = false
	return persisted, nil
}

func validateEventDraft(draft event.Draft, monotonicNS uint64) error {
	if _, err := id.ParseSession(draft.SessionID); err != nil {
		return fmt.Errorf("invalid event session id: %w", err)
	}
	if draft.WallTimeUTC.IsZero() {
		return fmt.Errorf("event wall time is required")
	}
	if !eventTypePattern.MatchString(draft.Type) {
		return fmt.Errorf("invalid event type %q", draft.Type)
	}
	if strings.TrimSpace(draft.Source) == "" {
		return fmt.Errorf("event source is required")
	}
	if strings.TrimSpace(draft.Privacy.Classification) == "" {
		return fmt.Errorf("event privacy classification is required")
	}
	if len(draft.Payload) != 0 && !json.Valid(draft.Payload) {
		return fmt.Errorf("event payload is not valid JSON")
	}
	if monotonicNS > maxSQLiteInteger {
		return fmt.Errorf("monotonic timestamp exceeds SQLite INTEGER range")
	}
	if draft.StateBefore != "" {
		if _, err := id.ParseState(draft.StateBefore); err != nil {
			return fmt.Errorf("invalid event state_before: %w", err)
		}
	}
	if draft.StateAfter != "" {
		if _, err := id.ParseState(draft.StateAfter); err != nil {
			return fmt.Errorf("invalid event state_after: %w", err)
		}
	}
	return nil
}

func claimEventSequence(ctx context.Context, tx *sql.Tx, sessionID string) (uint64, error) {
	var seq int64
	err := tx.QueryRowContext(ctx, `
UPDATE sessions
SET next_event_seq = next_event_seq + 1
WHERE id = ?
  AND status = 'recording'
  AND next_event_seq < 9223372036854775807
RETURNING next_event_seq - 1`, sessionID).Scan(&seq)
	if err == nil {
		if seq <= 0 {
			return 0, fmt.Errorf("database returned invalid event sequence %d", seq)
		}
		return uint64(seq), nil
	}
	if err != sql.ErrNoRows {
		return 0, fmt.Errorf("reserve event sequence: %w", err)
	}

	var status string
	if statusErr := tx.QueryRowContext(ctx, "SELECT status FROM sessions WHERE id = ?", sessionID).Scan(&status); statusErr != nil {
		if statusErr == sql.ErrNoRows {
			return 0, fmt.Errorf("session %s does not exist", sessionID)
		}
		return 0, fmt.Errorf("read session status while reserving event sequence: %w", statusErr)
	}
	if status != "recording" {
		return 0, fmt.Errorf("session %s is %q and cannot accept events", sessionID, status)
	}
	return 0, fmt.Errorf("session %s event sequence is exhausted", sessionID)
}

func validateMonotonicOrder(ctx context.Context, tx *sql.Tx, sessionID string, monotonicNS uint64) error {
	var previous int64
	err := tx.QueryRowContext(ctx,
		"SELECT monotonic_ns FROM events WHERE session_id = ? ORDER BY seq DESC LIMIT 1", sessionID,
	).Scan(&previous)
	if err == sql.ErrNoRows {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read previous event monotonic timestamp: %w", err)
	}
	if monotonicNS < uint64(previous) {
		return fmt.Errorf("event monotonic timestamp %d precedes previous value %d", monotonicNS, previous)
	}
	return nil
}

func validateEventStateReferences(ctx context.Context, tx *sql.Tx, draft event.Draft) error {
	for label, stateID := range map[string]string{
		"state_before": draft.StateBefore,
		"state_after":  draft.StateAfter,
	} {
		if stateID == "" {
			continue
		}
		var count int
		if err := tx.QueryRowContext(ctx,
			"SELECT COUNT(1) FROM states WHERE id = ? AND session_id = ?", stateID, draft.SessionID,
		).Scan(&count); err != nil {
			return fmt.Errorf("validate event %s: %w", label, err)
		}
		if count != 1 {
			return fmt.Errorf("event %s %s does not belong to session %s", label, stateID, draft.SessionID)
		}
	}
	return nil
}

func insertEventTx(ctx context.Context, tx *sql.Tx, draft event.Draft, seq, monotonicNS uint64) (event.Event, error) {
	persisted := draft.Stamp(seq, monotonicNS)
	payload := draft.Payload
	if len(payload) == 0 {
		payload = json.RawMessage("null")
	}
	privacyJSON, err := json.Marshal(draft.Privacy)
	if err != nil {
		return event.Event{}, fmt.Errorf("encode event privacy metadata: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO events(
    session_id, seq, wall_time_utc, monotonic_ns, type, source,
    state_before, state_after, payload_json, privacy_json,
    parent_event_id, span_id
) VALUES(?, ?, ?, ?, ?, ?, NULLIF(?, ''), NULLIF(?, ''), ?, ?, NULLIF(?, ''), NULLIF(?, ''))`,
		draft.SessionID,
		int64(seq),
		draft.WallTimeUTC.UTC().Format("2006-01-02T15:04:05.999999999Z07:00"),
		int64(monotonicNS),
		draft.Type,
		draft.Source,
		draft.StateBefore,
		draft.StateAfter,
		[]byte(payload),
		privacyJSON,
		draft.ParentEvent,
		draft.SpanID,
	); err != nil {
		return event.Event{}, fmt.Errorf("insert event %s/%d: %w", draft.SessionID, seq, err)
	}
	return persisted, nil
}

// MarkSessionDegraded records a durable degraded state without deleting any
// evidence. It is used when verified storage corruption prevents a requested
// state operation from completing.
func (db *DB) MarkSessionDegraded(ctx context.Context, sessionID id.SessionID, reason string) error {
	if _, err := id.ParseSession(sessionID.String()); err != nil {
		return fmt.Errorf("invalid session id: %w", err)
	}
	if strings.TrimSpace(reason) == "" {
		return fmt.Errorf("degraded reason is required")
	}
	result, err := db.sql.ExecContext(ctx, `
UPDATE sessions
SET status = 'degraded', degraded_reason = ?
WHERE id = ? AND status IN ('recording', 'recovered', 'degraded')`, reason, sessionID.String())
	if err != nil {
		return fmt.Errorf("mark session %s degraded: %w", sessionID, err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read degraded session update result: %w", err)
	}
	if rows != 1 {
		return fmt.Errorf("session %s cannot be marked degraded", sessionID)
	}
	return nil
}
