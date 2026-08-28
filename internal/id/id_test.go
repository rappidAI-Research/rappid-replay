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

func TestNewStateProducesCanonicalUUIDv7(t *testing.T) {
	stateID, err := NewState()
	if err != nil {
		t.Fatalf("NewState() error = %v", err)
	}
	if !strings.HasPrefix(stateID.String(), statePrefix) {
		t.Fatalf("state id %q missing prefix %q", stateID, statePrefix)
	}
	parsed, err := ParseState(stateID.String())
	if err != nil {
		t.Fatalf("ParseState() error = %v", err)
	}
	if parsed != stateID {
		t.Fatalf("ParseState() = %q, want %q", parsed, stateID)
	}
}

func TestNewArtifactProducesCanonicalUUIDv7(t *testing.T) {
	artifactID, err := NewArtifact()
	if err != nil {
		t.Fatalf("NewArtifact() error = %v", err)
	}
	if !strings.HasPrefix(artifactID.String(), artifactPrefix) {
		t.Fatalf("artifact id %q missing prefix %q", artifactID, artifactPrefix)
	}
	parsed, err := ParseArtifact(artifactID.String())
	if err != nil {
		t.Fatalf("ParseArtifact() error = %v", err)
	}
	if parsed != artifactID {
		t.Fatalf("ParseArtifact() = %q, want %q", parsed, artifactID)
	}
}

func TestParsersRejectWrongDomainVersionAndNonCanonicalUUID(t *testing.T) {
	if _, err := ParseSession("not-a-session"); err == nil {
		t.Fatal("ParseSession() accepted missing prefix")
	}
	if _, err := ParseState("not-a-state"); err == nil {
		t.Fatal("ParseState() accepted missing prefix")
	}
	if _, err := ParseArtifact("not-an-artifact"); err == nil {
		t.Fatal("ParseArtifact() accepted missing prefix")
	}

	v4 := uuid.New()
	if _, err := ParseSession(sessionPrefix + v4.String()); err == nil {
		t.Fatal("ParseSession() accepted UUIDv4")
	}
	if _, err := ParseState(statePrefix + v4.String()); err == nil {
		t.Fatal("ParseState() accepted UUIDv4")
	}
	if _, err := ParseArtifact(artifactPrefix + v4.String()); err == nil {
		t.Fatal("ParseArtifact() accepted UUIDv4")
	}

	v7, err := uuid.NewV7()
	if err != nil {
		t.Fatalf("uuid.NewV7() error = %v", err)
	}
	if _, err := ParseState(statePrefix + strings.ToUpper(v7.String())); err == nil {
		t.Fatal("ParseState() accepted non-canonical uppercase UUID")
	}
	if _, err := ParseArtifact(artifactPrefix + strings.ToUpper(v7.String())); err == nil {
		t.Fatal("ParseArtifact() accepted non-canonical uppercase UUID")
	}
}
