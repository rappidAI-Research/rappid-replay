package record

import (
	"bytes"
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"

	"github.com/rappidAI-Research/rappid-replay/internal/persistence"
	"github.com/rappidAI-Research/rappid-replay/internal/store"
)

func TestGenericRecorderAbortsSessionWhenCommandCannotStart(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "replay.db")
	db, err := persistence.Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	cas, err := store.NewLocalStore(t.TempDir(), bytes.Repeat([]byte{0x73}, 32))
	if err != nil {
		t.Fatal(err)
	}
	defer cas.Close()

	result, err := Run(ctx, Dependencies{DB: db, CAS: cas}, Options{
		Command:       []string{"rappid-replay-command-that-does-not-exist-7f71f632"},
		WorkingDir:    t.TempDir(),
		TerminalInput: "off",
	})
	if err == nil || !strings.Contains(err.Error(), "start recorded command") {
		t.Fatalf("Run() error = %v, want command start failure", err)
	}
	if result.SessionID == "" || result.InitialStateID == "" || result.FinalStateID != "" {
		t.Fatalf("aborted result = %+v", result)
	}

	raw := openFailureRawDB(t, dbPath)
	defer raw.Close()
	var status, initialState string
	var finalState sql.NullString
	if err := raw.QueryRowContext(ctx,
		"SELECT status, initial_state_id, final_state_id FROM sessions WHERE id = ?", result.SessionID.String(),
	).Scan(&status, &initialState, &finalState); err != nil {
		t.Fatal(err)
	}
	if status != "aborted" || initialState != result.InitialStateID.String() || finalState.Valid {
		t.Fatalf("aborted session = status %q initial %q final %+v", status, initialState, finalState)
	}

	var lastType string
	if err := raw.QueryRowContext(ctx,
		"SELECT type FROM events WHERE session_id = ? ORDER BY seq DESC LIMIT 1", result.SessionID.String(),
	).Scan(&lastType); err != nil {
		t.Fatal(err)
	}
	if lastType != "session.aborted" {
		t.Fatalf("last event = %q, want session.aborted", lastType)
	}
}

func TestGenericRecorderRejectsFullInputCaptureBeforeCreatingSession(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "replay.db")
	db, err := persistence.Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	cas, err := store.NewLocalStore(t.TempDir(), bytes.Repeat([]byte{0x74}, 32))
	if err != nil {
		t.Fatal(err)
	}
	defer cas.Close()

	_, err = Run(ctx, Dependencies{DB: db, CAS: cas}, Options{
		Command:       []string{"unused"},
		WorkingDir:    t.TempDir(),
		TerminalInput: "full",
	})
	if err == nil || !strings.Contains(err.Error(), "PTY recorder") {
		t.Fatalf("Run() error = %v, want explicit PTY requirement", err)
	}

	raw := openFailureRawDB(t, dbPath)
	defer raw.Close()
	var count int
	if err := raw.QueryRowContext(ctx, "SELECT COUNT(1) FROM sessions").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("session count = %d, want 0 after preflight rejection", count)
	}
}

func openFailureRawDB(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Ping(); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	return db
}
