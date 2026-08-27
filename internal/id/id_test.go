package id

import (
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestNewSessionProducesCanonicalUUIDv7(t *testing.T) {
	sid, err := NewSession()
	if err != nil {
		t.Fatalf("NewSession() error = %v", err)
	}

	if !strings.HasPrefix(sid.String(), sessionPrefix) {
		t.Fatalf("session id %q missing prefix %q", sid, sessionPrefix)
	}

	u, err := uuid.Parse(strings.TrimPrefix(sid.String(), sessionPrefix))
	if err != nil {
		t.Fatalf("parse UUID payload: %v", err)
	}
	if u.Version() != 7 {
		t.Fatalf("UUID version = %d, want 7", u.Version())
	}

	parsed, err := ParseSession(sid.String())
	if err != nil {
		t.Fatalf("ParseSession() error = %v", err)
	}
	if parsed != sid {
		t.Fatalf("ParseSession() = %q, want %q", parsed, sid)
	}
}

func TestParseSessionRejectsWrongDomainAndVersion(t *testing.T) {
	if _, err := ParseSession("not-a-session"); err == nil {
		t.Fatal("ParseSession() accepted missing prefix")
	}

	v4 := uuid.New()
	if _, err := ParseSession(sessionPrefix + v4.String()); err == nil {
		t.Fatal("ParseSession() accepted UUIDv4")
	}
}
