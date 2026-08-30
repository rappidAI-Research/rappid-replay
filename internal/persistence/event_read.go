package persistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/rappidAI-Research/rappid-replay/internal/event"
	"github.com/rappidAI-Research/rappid-replay/internal/id"
)

// ListEvents returns the immutable event stream for one session in canonical
// sequence order. The read path re-validates the persisted envelope fields so
// diff/export/UI consumers never have to trust malformed database rows.
func (db *DB) ListEvents(ctx context.Context, sessionID id.SessionID) ([]event.Event, error) {
	if db == nil || db.sql == nil {
		return nil, fmt.Errorf("Replay database is required")
	}
	if _, err := id.ParseSession(sessionID.String()); err != nil {
		return nil, fmt.Errorf("invalid session id: %w", err)
	}

	var exists int
	if err := db.sql.QueryRowContext(ctx, "SELECT COUNT(1) FROM sessions WHERE id = ?", sessionID.String()).Scan(&exists); err != nil {
		return nil, fmt.Errorf("check session %s: %w", sessionID, err)
	}
	if exists != 1 {
		return nil, fmt.Errorf("%w: %s", ErrSessionNotFound, sessionID)
	}

	rows, err := db.sql.QueryContext(ctx, `
SELECT seq, wall_time_utc, monotonic_ns, type, source,
       state_before, state_after, payload_json, privacy_json,
       parent_event_id, span_id
FROM events
WHERE session_id = ?
ORDER BY seq ASC`, sessionID.String())
	if err != nil {
		return nil, fmt.Errorf("list events for session %s: %w", sessionID, err)
	}
	defer rows.Close()

	result := make([]event.Event, 0)
	var previousSeq uint64
	var previousMonotonic uint64
	for rows.Next() {
		var seq, monotonic int64
		var wallText, eventType, source string
		var stateBefore, stateAfter, parentEvent, spanID sql.NullString
		var payloadJSON, privacyJSON []byte
		if err := rows.Scan(
			&seq, &wallText, &monotonic, &eventType, &source,
			&stateBefore, &stateAfter, &payloadJSON, &privacyJSON,
			&parentEvent, &spanID,
		); err != nil {
			return nil, fmt.Errorf("scan event for session %s: %w", sessionID, err)
		}
		if seq <= 0 {
			return nil, fmt.Errorf("session %s contains invalid event sequence %d", sessionID, seq)
		}
		seqValue := uint64(seq)
		if previousSeq != 0 && seqValue != previousSeq+1 {
			return nil, fmt.Errorf("session %s event sequence jumps from %d to %d", sessionID, previousSeq, seqValue)
		}
		if monotonic < 0 {
			return nil, fmt.Errorf("session %s event %d contains negative monotonic timestamp", sessionID, seqValue)
		}
		monotonicValue := uint64(monotonic)
		if previousSeq != 0 && monotonicValue < previousMonotonic {
			return nil, fmt.Errorf("session %s event %d monotonic timestamp precedes event %d", sessionID, seqValue, previousSeq)
		}
		wall, err := time.Parse(time.RFC3339Nano, wallText)
		if err != nil {
			return nil, fmt.Errorf("session %s event %d contains invalid wall time %q: %w", sessionID, seqValue, wallText, err)
		}
		if !eventTypePattern.MatchString(eventType) {
			return nil, fmt.Errorf("session %s event %d contains invalid type %q", sessionID, seqValue, eventType)
		}
		if source == "" {
			return nil, fmt.Errorf("session %s event %d contains empty source", sessionID, seqValue)
		}
		if len(payloadJSON) == 0 || !json.Valid(payloadJSON) {
			return nil, fmt.Errorf("session %s event %d contains invalid payload JSON", sessionID, seqValue)
		}
		var privacy event.Privacy
		if len(privacyJSON) == 0 || !json.Valid(privacyJSON) {
			return nil, fmt.Errorf("session %s event %d contains invalid privacy JSON", sessionID, seqValue)
		}
		if err := json.Unmarshal(privacyJSON, &privacy); err != nil {
			return nil, fmt.Errorf("decode privacy metadata for session %s event %d: %w", sessionID, seqValue, err)
		}
		if privacy.Classification == "" {
			return nil, fmt.Errorf("session %s event %d contains empty privacy classification", sessionID, seqValue)
		}
		for label, raw := range map[string]string{
			"state_before": stateBefore.String,
			"state_after":  stateAfter.String,
		} {
			if raw == "" {
				continue
			}
			if _, err := id.ParseState(raw); err != nil {
				return nil, fmt.Errorf("session %s event %d contains invalid %s %q: %w", sessionID, seqValue, label, raw, err)
			}
		}

		result = append(result, event.Event{
			Schema:      event.SchemaV1,
			SessionID:   sessionID.String(),
			Seq:         seqValue,
			WallTimeUTC: wall.UTC(),
			MonotonicNS: monotonicValue,
			Type:        eventType,
			Source:      source,
			StateBefore: stateBefore.String,
			StateAfter:  stateAfter.String,
			ParentEvent: parentEvent.String,
			SpanID:      spanID.String,
			Payload:     append(json.RawMessage(nil), payloadJSON...),
			Privacy:     privacy,
		})
		previousSeq = seqValue
		previousMonotonic = monotonicValue
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate events for session %s: %w", sessionID, err)
	}
	return result, nil
}

// IsSessionNotFound reports whether err identifies a missing session. It is
// intentionally small so higher-level read-only commands can preserve a stable
// not-found distinction without importing database/sql.
func IsSessionNotFound(err error) bool {
	return errors.Is(err, ErrSessionNotFound)
}
