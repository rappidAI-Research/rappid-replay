package record

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rappidAI-Research/rappid-replay/internal/event"
	"github.com/rappidAI-Research/rappid-replay/internal/persistence"
	"github.com/rappidAI-Research/rappid-replay/internal/store"
	"github.com/rappidAI-Research/rappid-replay/internal/terminal"
)

func TestPTYRecorderCapturesCombinedOutputInputAndResize(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "replay.db")
	db, err := persistence.Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	cas, err := store.NewLocalStore(t.TempDir(), bytes.Repeat([]byte{0x75}, 32))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cas.Close() })

	workspace := t.TempDir()
	resize := make(chan terminal.Size, 1)
	resize <- terminal.Size{Columns: 100, Rows: 30}
	close(resize)

	var output bytes.Buffer
	result, err := Run(ctx, Dependencies{DB: db, CAS: cas}, Options{
		Command:             ptyHelperCommand(),
		WorkingDir:          workspace,
		TerminalInput:       "full",
		PTY:                 true,
		InitialTerminalSize: terminal.Size{Columns: 80, Rows: 24},
		TerminalResize:      resize,
		Stdin:               strings.NewReader("hello replay\n"),
		Stdout:              &output,
		Env:                 append(os.Environ(), "RAPPID_REPLAY_PTY_HELPER=1"),
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if result.ExitCode != 0 {
		t.Fatalf("exit code = %d, want 0", result.ExitCode)
	}
	if !strings.Contains(output.String(), "pty received:hello replay") {
		t.Fatalf("PTY output = %q, want helper response", output.String())
	}

	raw := openRawDB(t, dbPath)
	defer raw.Close()
	rows, err := raw.QueryContext(ctx, "SELECT type FROM events WHERE session_id = ? ORDER BY seq", result.SessionID.String())
	if err != nil {
		t.Fatal(err)
	}
	var types []string
	for rows.Next() {
		var eventType string
		if err := rows.Scan(&eventType); err != nil {
			_ = rows.Close()
			t.Fatal(err)
		}
		types = append(types, eventType)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	joined := "," + strings.Join(types, ",") + ","
	for _, required := range []string{"terminal.opened", "process.started", "terminal.stdin", "terminal.resized", "terminal.output", "process.exited"} {
		if !strings.Contains(joined, ","+required+",") {
			t.Fatalf("event %q missing from %v", required, types)
		}
	}
	if strings.Contains(joined, ",terminal.stdout,") || strings.Contains(joined, ",terminal.stderr,") {
		t.Fatalf("PTY recording must not invent stdout/stderr split: %v", types)
	}

	var payloadJSON, privacyJSON []byte
	if err := raw.QueryRowContext(ctx,
		"SELECT payload, privacy FROM events WHERE session_id = ? AND type = 'terminal.stdin' ORDER BY seq LIMIT 1",
		result.SessionID.String(),
	).Scan(&payloadJSON, &privacyJSON); err != nil {
		t.Fatal(err)
	}
	var payload struct {
		Encoding    string `json:"encoding"`
		DataB64     string `json:"data_b64"`
		Bytes       int    `json:"bytes"`
		StoredBytes int    `json:"stored_bytes"`
	}
	if err := json.Unmarshal(payloadJSON, &payload); err != nil {
		t.Fatal(err)
	}
	decoded, err := base64.StdEncoding.DecodeString(payload.DataB64)
	if err != nil {
		t.Fatal(err)
	}
	if string(decoded) != "hello replay\n" || payload.Bytes != len("hello replay\n") || payload.StoredBytes != len(decoded) {
		t.Fatalf("terminal.stdin payload = %+v decoded %q", payload, decoded)
	}
	var privacy event.Privacy
	if err := json.Unmarshal(privacyJSON, &privacy); err != nil {
		t.Fatal(err)
	}
	if privacy.Classification != "content" || privacy.Redacted {
		t.Fatalf("terminal.stdin privacy = %+v", privacy)
	}
}

func TestPTYRecorderMetadataOnlyInputDoesNotPersistBytes(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "replay.db")
	db, err := persistence.Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	cas, err := store.NewLocalStore(t.TempDir(), bytes.Repeat([]byte{0x76}, 32))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cas.Close() })

	result, err := Run(ctx, Dependencies{DB: db, CAS: cas}, Options{
		Command:             ptyHelperCommand(),
		WorkingDir:          t.TempDir(),
		TerminalInput:       "metadata-only",
		PTY:                 true,
		InitialTerminalSize: terminal.Size{Columns: 80, Rows: 24},
		Stdin:               strings.NewReader("private text\n"),
		Env:                 append(os.Environ(), "RAPPID_REPLAY_PTY_HELPER=1"),
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	raw := openRawDB(t, dbPath)
	defer raw.Close()
	var payload []byte
	if err := raw.QueryRowContext(ctx,
		"SELECT payload FROM events WHERE session_id = ? AND type = 'terminal.stdin' ORDER BY seq LIMIT 1",
		result.SessionID.String(),
	).Scan(&payload); err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(payload, []byte("private text")) || bytes.Contains(payload, []byte("cHJpdmF0ZSB0ZXh0")) {
		t.Fatalf("metadata-only input leaked content: %s", payload)
	}
	var metadata struct {
		Bytes   int    `json:"bytes"`
		Capture string `json:"capture"`
	}
	if err := json.Unmarshal(payload, &metadata); err != nil {
		t.Fatal(err)
	}
	if metadata.Bytes != len("private text\n") || metadata.Capture != "metadata-only" {
		t.Fatalf("metadata-only payload = %+v", metadata)
	}
}

func ptyHelperCommand() []string {
	return []string{os.Args[0], "-test.run=^TestReplayPTYHelperProcess$"}
}

func TestReplayPTYHelperProcess(t *testing.T) {
	if os.Getenv("RAPPID_REPLAY_PTY_HELPER") != "1" {
		return
	}
	line, err := bufio.NewReader(os.Stdin).ReadString('\n')
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(96)
	}
	fmt.Fprintf(os.Stdout, "pty received:%s", line)
	// Keep the process alive briefly so a queued resize observation can be
	// applied deterministically by the recorder's resize pump.
	time.Sleep(100 * time.Millisecond)
	os.Exit(0)
}
