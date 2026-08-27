// Package event defines the canonical Replay event envelope.
package event

import (
	"encoding/json"
	"time"
)

const SchemaV1 = "rappid.replay.event/1"

// Draft is an event observation before persistence assigns the session-local
// sequence and monotonic timestamp. Draft deliberately has no JSON schema
// representation so it cannot accidentally be exported as a valid event.
type Draft struct {
	SessionID   string
	WallTimeUTC time.Time
	Type        string
	Source      string
	StateBefore string
	StateAfter  string
	ParentEvent string
	SpanID      string
	Payload     json.RawMessage
	Privacy     Privacy
}

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

// NewDraft creates an unstamped event observation. Persistence must validate and
// stamp it before the value can become a canonical Event.
func NewDraft(sessionID, eventType, source string, wallTime time.Time, privacy Privacy, payload json.RawMessage) Draft {
	return Draft{
		SessionID:   sessionID,
		WallTimeUTC: wallTime.UTC(),
		Type:        eventType,
		Source:      source,
		Payload:     append(json.RawMessage(nil), payload...),
		Privacy:     privacy,
	}
}

// Stamp creates the schema-valid immutable envelope after persistence has
// reserved a strictly increasing sequence number.
func (d Draft) Stamp(seq, monotonicNS uint64) Event {
	return Event{
		Schema:      SchemaV1,
		SessionID:   d.SessionID,
		Seq:         seq,
		WallTimeUTC: d.WallTimeUTC.UTC(),
		MonotonicNS: monotonicNS,
		Type:        d.Type,
		Source:      d.Source,
		StateBefore: d.StateBefore,
		StateAfter:  d.StateAfter,
		ParentEvent: d.ParentEvent,
		SpanID:      d.SpanID,
		Payload:     append(json.RawMessage(nil), d.Payload...),
		Privacy:     d.Privacy,
	}
}
