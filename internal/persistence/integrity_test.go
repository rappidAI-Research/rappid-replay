package persistence

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/rappidAI-Research/rappid-replay/internal/id"
)

func TestMarkSessionIntegrityDegradedTransitionsCompletedSession(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, filepath.Join(t.TempDir(), "replay.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	sessionID, err := id.NewSession()
	if err != nil {
		t.Fatal(err)
	}
	if err := db.CreateSession(ctx, SessionStart{
		ID: sessionID, Command: []string{"test"}, CWD: t.TempDir(), StartedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.sql.ExecContext(ctx, "UPDATE sessions SET status = 'completed' WHERE id = ?", sessionID.String()); err != nil {
		t.Fatal(err)
	}
	if err := db.MarkSessionIntegrityDegraded(ctx, sessionID, "CAS object corrupt"); err != nil {
		t.Fatal(err)
	}
	var status, reason string
	if err := db.sql.QueryRowContext(ctx, "SELECT status, degraded_reason FROM sessions WHERE id = ?", sessionID.String()).Scan(&status, &reason); err != nil {
		t.Fatal(err)
	}
	if status != "degraded" || reason != "CAS object corrupt" {
		t.Fatalf("session integrity state = %q/%q", status, reason)
	}
}
