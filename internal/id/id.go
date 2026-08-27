// Package id defines Replay's stable identifier types and generation rules.
package id

import (
	"fmt"
	"strings"

	"github.com/google/uuid"
)

const sessionPrefix = "rp_"

// SessionID identifies one immutable Replay session. The UUIDv7 payload keeps
// identifiers time-sortable while the rp_ prefix makes their domain explicit
// in CLI output, logs, exports, and database rows.
type SessionID string

// NewSession returns a cryptographically-random UUIDv7-backed session ID.
func NewSession() (SessionID, error) {
	u, err := uuid.NewV7()
	if err != nil {
		return "", fmt.Errorf("generate UUIDv7 session id: %w", err)
	}
	return SessionID(sessionPrefix + u.String()), nil
}

// ParseSession validates the canonical rp_<uuidv7> representation.
func ParseSession(s string) (SessionID, error) {
	if !strings.HasPrefix(s, sessionPrefix) {
		return "", fmt.Errorf("session id must start with %q", sessionPrefix)
	}

	u, err := uuid.Parse(strings.TrimPrefix(s, sessionPrefix))
	if err != nil {
		return "", fmt.Errorf("parse session UUID: %w", err)
	}
	if u.Version() != 7 {
		return "", fmt.Errorf("session id must contain UUIDv7, got version %d", u.Version())
	}
	return SessionID(s), nil
}

func (s SessionID) String() string { return string(s) }
