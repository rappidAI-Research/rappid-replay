// Package id defines Replay's stable identifier types and generation rules.
package id

import (
	"fmt"
	"strings"

	"github.com/google/uuid"
)

const (
	sessionPrefix = "rp_"
	statePrefix   = "st_"
)

// SessionID identifies one immutable Replay session. The UUIDv7 payload keeps
// identifiers time-sortable while the rp_ prefix makes their domain explicit
// in CLI output, logs, exports, and database rows.
type SessionID string

// StateID identifies one immutable published workspace state. Content identity
// remains the root CAS object ID; StateID identifies the publication record and
// lets identical workspace contents appear in more than one session.
type StateID string

// NewSession returns a cryptographically-random UUIDv7-backed session ID.
func NewSession() (SessionID, error) {
	value, err := newUUIDv7(sessionPrefix, "session")
	if err != nil {
		return "", err
	}
	return SessionID(value), nil
}

// ParseSession validates the canonical rp_<uuidv7> representation.
func ParseSession(s string) (SessionID, error) {
	value, err := parseUUIDv7(s, sessionPrefix, "session")
	if err != nil {
		return "", err
	}
	return SessionID(value), nil
}

// NewState returns a cryptographically-random UUIDv7-backed state publication ID.
func NewState() (StateID, error) {
	value, err := newUUIDv7(statePrefix, "state")
	if err != nil {
		return "", err
	}
	return StateID(value), nil
}

// ParseState validates the canonical st_<uuidv7> representation.
func ParseState(s string) (StateID, error) {
	value, err := parseUUIDv7(s, statePrefix, "state")
	if err != nil {
		return "", err
	}
	return StateID(value), nil
}

func newUUIDv7(prefix, domain string) (string, error) {
	u, err := uuid.NewV7()
	if err != nil {
		return "", fmt.Errorf("generate UUIDv7 %s id: %w", domain, err)
	}
	return prefix + u.String(), nil
}

func parseUUIDv7(s, prefix, domain string) (string, error) {
	if !strings.HasPrefix(s, prefix) {
		return "", fmt.Errorf("%s id must start with %q", domain, prefix)
	}
	payload := strings.TrimPrefix(s, prefix)
	u, err := uuid.Parse(payload)
	if err != nil {
		return "", fmt.Errorf("parse %s UUID: %w", domain, err)
	}
	if u.Version() != 7 {
		return "", fmt.Errorf("%s id must contain UUIDv7, got version %d", domain, u.Version())
	}
	if payload != u.String() {
		return "", fmt.Errorf("%s id UUID must use canonical lowercase representation", domain)
	}
	return prefix + u.String(), nil
}

func (s SessionID) String() string { return string(s) }
func (s StateID) String() string   { return string(s) }
