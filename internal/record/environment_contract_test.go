package record

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/rappidAI-Research/rappid-replay/internal/persistence"
	"github.com/rappidAI-Research/rappid-replay/internal/store"
)

func TestRecorderPersistsEnvironmentBeforeProcessExecution(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "replay.db")
	db, err := persistence.Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	cas, err := store.NewLocalStore(t.TempDir(), bytes.Repeat([]byte{0x6b}, 32))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cas.Close() })

	workspace := t.TempDir()
	result, err := Run(ctx, Dependencies{DB: db, CAS: cas}, Options{
		Command:       helperCommand(workspace),
		WorkingDir:    workspace,
		TerminalInput: "off",
		Env: append(helperEnv(0),
			"OPENAI_API_KEY=must-never-persist",
			"REPLAY_TEST_VALUE=visible-metadata",
		),
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	raw := openRawDB(t, dbPath)
	defer raw.Close()
	var fingerprint []byte
	var level string
	if err := raw.QueryRowContext(ctx, `
SELECT e.fingerprint_json, s.reproducibility_level
FROM environments e
JOIN sessions s ON s.id = e.session_id
WHERE e.session_id = ?`, result.SessionID.String()).Scan(&fingerprint, &level); err != nil {
		t.Fatal(err)
	}
	if level != "R2" {
		t.Fatalf("reproducibility level = %q, want R2", level)
	}
	if bytes.Contains(fingerprint, []byte("must-never-persist")) {
		t.Fatalf("environment fingerprint leaked secret: %s", fingerprint)
	}
	if !bytes.Contains(fingerprint, []byte("visible-metadata")) {
		t.Fatalf("environment fingerprint omitted ordinary metadata: %s", fingerprint)
	}

	var decoded struct {
		Variables []environmentVariable `json:"variables"`
	}
	if err := json.Unmarshal(fingerprint, &decoded); err != nil {
		t.Fatal(err)
	}
	foundRedacted := false
	for _, variable := range decoded.Variables {
		if variable.Name == "OPENAI_API_KEY" {
			foundRedacted = variable.Redacted && variable.Value == "[REDACTED]"
		}
	}
	if !foundRedacted {
		t.Fatalf("OPENAI_API_KEY was not represented as redacted: %+v", decoded.Variables)
	}

	rows, err := raw.QueryContext(ctx, `SELECT type FROM events WHERE session_id = ? ORDER BY seq`, result.SessionID.String())
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var eventTypes []string
	for rows.Next() {
		var eventType string
		if err := rows.Scan(&eventType); err != nil {
			t.Fatal(err)
		}
		eventTypes = append(eventTypes, eventType)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	index := func(name string) int {
		for i, eventType := range eventTypes {
			if eventType == name {
				return i
			}
		}
		return -1
	}
	initial := index("state.snapshot")
	environment := index("session.environment")
	started := index("process.started")
	if initial < 0 || environment < 0 || started < 0 || !(initial < environment && environment < started) {
		t.Fatalf("event order = %v, want initial snapshot < environment < process.started", eventTypes)
	}
}
