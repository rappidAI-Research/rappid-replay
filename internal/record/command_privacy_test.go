package record

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/rappidAI-Research/rappid-replay/internal/persistence"
	"github.com/rappidAI-Research/rappid-replay/internal/store"
)

func TestGenericRecorderRedactsCommandSecretsFromDurableMetadata(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "replay.db")
	db, err := persistence.Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	cas, err := store.NewLocalStore(t.TempDir(), bytes.Repeat([]byte{0x75}, 32))
	if err != nil {
		t.Fatal(err)
	}
	defer cas.Close()

	workspace := t.TempDir()
	secret := "super-secret-command-token-value"
	command := append(helperCommand(workspace), "--token", secret)
	result, err := Run(ctx, Dependencies{DB: db, CAS: cas}, Options{
		Command:       command,
		WorkingDir:    workspace,
		TerminalInput: "off",
		Env:           helperEnv(0),
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	raw, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	var commandJSON []byte
	if err := raw.QueryRowContext(ctx, "SELECT command_json FROM sessions WHERE id = ?", result.SessionID.String()).Scan(&commandJSON); err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(commandJSON, []byte(secret)) || !bytes.Contains(commandJSON, []byte("[REDACTED]")) {
		t.Fatalf("session command metadata = %s", commandJSON)
	}

	var payloadJSON, privacyJSON []byte
	if err := raw.QueryRowContext(ctx, `
SELECT payload_json, privacy_json
FROM events
WHERE session_id = ? AND type = 'process.started'`, result.SessionID.String()).Scan(&payloadJSON, &privacyJSON); err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(payloadJSON, []byte(secret)) || !bytes.Contains(payloadJSON, []byte("[REDACTED]")) {
		t.Fatalf("process.started payload = %s", payloadJSON)
	}
	var privacyMetadata struct {
		Redacted bool `json:"redacted"`
	}
	if err := json.Unmarshal(privacyJSON, &privacyMetadata); err != nil {
		t.Fatal(err)
	}
	if !privacyMetadata.Redacted {
		t.Fatalf("process.started privacy metadata = %s", privacyJSON)
	}

	// The helper only succeeds if it actually ran. The durable metadata is
	// redacted without mutating the argv supplied to exec.CommandContext.
	if _, err := os.Stat(filepath.Join(workspace, "after.txt")); err != nil {
		t.Fatalf("child did not execute with original argv: %v", err)
	}
	if strings.Contains(string(commandJSON), secret) {
		t.Fatal("secret leaked into command metadata")
	}
}
