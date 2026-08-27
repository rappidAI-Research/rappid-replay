// Package event defines the canonical Replay event envelope.
package event

import (
	"encoding/json"
	"time"
)

const SchemaV1 = "rappid.replay.event/1"

// Event is an immutable, totally ordered observation within one Replay session.
// Seq, not wall-clock time, defines ordering and causality within a session.
type Event struct {
	Schema      string          `json:"schema"`
	SessionID   string          `json:"session_id"`
	Seq         uint64          `json:"seq"`
	WallTimeUTC time.Time       `json:"wall_time_utc"`
	MonotonicNS uint64          `json:"monotonic_ns"`
	Type        string          `json:"type"`
	Source      string          `json:"source"`
	StateBefore string          `json:"state_before,omitempty"`
	StateAfter  string          `json:"state_after,omitempty"`
	ParentEvent string          `json:"parent_event_id,omitempty"`
	SpanID      string          `json:"span_id,omitempty"`
	Payload     json.RawMessage `json:"payload,omitempty"`
	Privacy     Privacy         `json:"privacy"`
}

// Privacy carries persisted classification metadata without changing event truth.
type Privacy struct {
	Classification string `json:"classification"`
	Redacted       bool   `json:"redacted,omitempty"`
}

// New creates the minimum valid v1 envelope. Persistence is responsible for
// assigning the strictly increasing sequence and monotonic timestamp.
func New(sessionID, eventType, source string, wallTime time.Time, privacy Privacy, payload json.RawMessage) Event {
	return Event{
		Schema:      SchemaV1,
		SessionID:   sessionID,
		WallTimeUTC: wallTime.UTC(),
		Type:        eventType,
		Source:      source,
		Payload:     payload,
		Privacy:     privacy,
	}
}
