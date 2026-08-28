package record

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rappidAI-Research/rappid-replay/internal/id"
	"github.com/rappidAI-Research/rappid-replay/internal/persistence"
)

func TestStreamEventWriterRedactsSecretAcrossWritesBeforeSQLite(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "replay.db")
	db, err := persistence.Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	sessionID, err := id.NewSession()
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	if err := db.CreateSession(ctx, persistence.SessionStart{
		ID:        sessionID,
		Command:   []string{"secret-writer"},
		CWD:       t.TempDir(),
		StartedAt: started,
	}); err != nil {
		t.Fatal(err)
	}

	sink := newEventSink(ctx, db, sessionID.String(), newRunClock(started))
	var passthrough bytes.Buffer
	writer := &streamEventWriter{sink: sink, stream: "stdout", output: &passthrough}
	secret := "github_pat_ABCDEFGHIJKLMNOPQRSTUVWXYZ123456"
	first := []byte("credential=" + secret[:18])
	second := []byte(secret[18:] + "\n")
	if _, err := writer.Write(first); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write(second); err != nil {
		t.Fatal(err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(passthrough.String(), secret) {
		t.Fatalf("live passthrough should remain unmodified, got %q", passthrough.String())
	}

	raw, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	var payloadJSON, privacyJSON []byte
	if err := raw.QueryRowContext(ctx, `
SELECT payload_json, privacy_json
FROM events
WHERE session_id = ? AND type = 'terminal.stdout'`, sessionID.String()).Scan(&payloadJSON, &privacyJSON); err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(payloadJSON, []byte(secret)) {
		t.Fatalf("SQLite payload leaked raw secret: %s", payloadJSON)
	}
	var payload struct {
		DataB64   string `json:"data_b64"`
		Redaction string `json:"redaction"`
	}
	if err := json.Unmarshal(payloadJSON, &payload); err != nil {
		t.Fatal(err)
	}
	stored, err := base64.StdEncoding.DecodeString(payload.DataB64)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(stored, []byte(secret)) || !bytes.Contains(stored, []byte("[REDACTED]")) {
		t.Fatalf("persisted terminal bytes = %q", stored)
	}
	if payload.Redaction != "known-secret-pattern" {
		t.Fatalf("redaction reason = %q", payload.Redaction)
	}
	var privacyMetadata struct {
		Classification string `json:"classification"`
		Redacted       bool   `json:"redacted"`
	}
	if err := json.Unmarshal(privacyJSON, &privacyMetadata); err != nil {
		t.Fatal(err)
	}
	if privacyMetadata.Classification != "content" || !privacyMetadata.Redacted {
		t.Fatalf("privacy metadata = %+v", privacyMetadata)
	}
}
