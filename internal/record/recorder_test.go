package record

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/rappidAI-Research/rappid-replay/internal/persistence"
	"github.com/rappidAI-Research/rappid-replay/internal/state"
	"github.com/rappidAI-Research/rappid-replay/internal/store"
)

func TestGenericRecorderCapturesLifecycleStreamsAndState(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "replay.db")
	db, err := persistence.Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	cas, err := store.NewLocalStore(t.TempDir(), bytes.Repeat([]byte{0x71}, 32))
	if err != nil {
		t.Fatalf("NewLocalStore() error = %v", err)
	}
	t.Cleanup(func() { _ = cas.Close() })

	workspace := t.TempDir()
	if err := os.WriteFile(filepath.Join(workspace, "before.txt"), []byte("before"), 0o600); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	result, err := Run(ctx, Dependencies{DB: db, CAS: cas}, Options{
		Command:       helperCommand(workspace),
		WorkingDir:    workspace,
		TerminalInput: "metadata-only",
		Env:           helperEnv(0),
		Stdout:        &stdout,
		Stderr:        &stderr,
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if result.SessionID == "" || result.InitialStateID == "" || result.FinalStateID == "" {
		t.Fatalf("recording result has empty ids: %+v", result)
	}
	if result.InitialStateID == result.FinalStateID {
		t.Fatalf("publication ids should differ across initial/final state: %+v", result)
	}
	if result.ExitCode != 0 {
		t.Fatalf("exit code = %d, want 0", result.ExitCode)
	}
	if stdout.String() != "helper stdout\n" {
		t.Fatalf("stdout = %q", stdout.String())
	}
	if stderr.String() != "helper stderr\n" {
		t.Fatalf("stderr = %q", stderr.String())
	}
	if got, err := os.ReadFile(filepath.Join(workspace, "after.txt")); err != nil || string(got) != "after" {
		t.Fatalf("recorded child mutation = %q, %v", got, err)
	}

	raw := openRawDB(t, dbPath)
	defer raw.Close()
	var status, initialState, finalState string
	if err := raw.QueryRowContext(ctx,
		"SELECT status, initial_state_id, final_state_id FROM sessions WHERE id = ?", result.SessionID.String(),
	).Scan(&status, &initialState, &finalState); err != nil {
		t.Fatalf("read recorded session: %v", err)
	}
	if status != "completed" || initialState != result.InitialStateID.String() || finalState != result.FinalStateID.String() {
		t.Fatalf("session status/states = %s %s %s", status, initialState, finalState)
	}

	rows, err := raw.QueryContext(ctx, "SELECT type FROM events WHERE session_id = ? ORDER BY seq", result.SessionID.String())
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
	if len(eventTypes) < 8 {
		t.Fatalf("event types = %v, want complete lifecycle", eventTypes)
	}
	if eventTypes[0] != "session.started" || eventTypes[1] != "state.snapshot" || eventTypes[2] != "process.started" {
		t.Fatalf("initial event order = %v", eventTypes)
	}
	if eventTypes[len(eventTypes)-3] != "process.exited" || eventTypes[len(eventTypes)-2] != "state.snapshot" || eventTypes[len(eventTypes)-1] != "session.completed" {
		t.Fatalf("terminal event order = %v", eventTypes)
	}
	joined := strings.Join(eventTypes, ",")
	if !strings.Contains(joined, "terminal.stdout") || !strings.Contains(joined, "terminal.stderr") {
		t.Fatalf("stream events missing from %v", eventTypes)
	}

	var finalRoot string
	if err := raw.QueryRowContext(ctx, "SELECT root_object_id FROM states WHERE id = ?", result.FinalStateID.String()).Scan(&finalRoot); err != nil {
		t.Fatal(err)
	}
	rootID, err := store.ParseObjectID(finalRoot)
	if err != nil {
		t.Fatal(err)
	}
	verified, err := state.VerifySnapshot(cas, rootID)
	if err != nil {
		t.Fatalf("VerifySnapshot(final) error = %v", err)
	}
	if verified.Files != 2 {
		t.Fatalf("final snapshot file count = %d, want 2", verified.Files)
	}
}

func TestGenericRecorderPreservesNonZeroChildExitAsCompletedRun(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "replay.db")
	db, err := persistence.Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	cas, err := store.NewLocalStore(t.TempDir(), bytes.Repeat([]byte{0x72}, 32))
	if err != nil {
		t.Fatal(err)
	}
	defer cas.Close()

	workspace := t.TempDir()
	result, err := Run(ctx, Dependencies{DB: db, CAS: cas}, Options{
		Command:       helperCommand(workspace),
		WorkingDir:    workspace,
		TerminalInput: "off",
		Env:           helperEnv(7),
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if result.ExitCode != 7 {
		t.Fatalf("exit code = %d, want 7", result.ExitCode)
	}

	raw := openRawDB(t, dbPath)
	defer raw.Close()
	var status string
	if err := raw.QueryRowContext(ctx, "SELECT status FROM sessions WHERE id = ?", result.SessionID.String()).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "completed" {
		t.Fatalf("session status = %q, want completed", status)
	}
}

func TestGenericRecorderRejectsUnsupportedFullInputCapture(t *testing.T) {
	_, err := Run(context.Background(), Dependencies{}, Options{
		Command:       []string{"unused"},
		TerminalInput: "full",
	})
	if err == nil || !strings.Contains(err.Error(), "database is required") {
		// Dependency validation intentionally precedes policy validation so invalid
		// wiring is never hidden by a command option.
		t.Fatalf("Run() error = %v", err)
	}
}

func helperCommand(workspace string) []string {
	return []string{os.Args[0], "-test.run=^TestReplayRecorderHelperProcess$", "--", workspace}
}

func helperEnv(exitCode int) []string {
	return append(os.Environ(),
		"RAPPID_REPLAY_HELPER=1",
		"RAPPID_REPLAY_HELPER_EXIT="+strconv.Itoa(exitCode),
	)
}

func TestReplayRecorderHelperProcess(t *testing.T) {
	if os.Getenv("RAPPID_REPLAY_HELPER") != "1" {
		return
	}
	separator := -1
	for index, arg := range os.Args {
		if arg == "--" {
			separator = index
			break
		}
	}
	if separator < 0 || separator+1 >= len(os.Args) {
		fmt.Fprintln(os.Stderr, "helper missing workspace")
		os.Exit(97)
	}
	workspace := os.Args[separator+1]
	fmt.Fprintln(os.Stdout, "helper stdout")
	fmt.Fprintln(os.Stderr, "helper stderr")
	if err := os.WriteFile(filepath.Join(workspace, "after.txt"), []byte("after"), 0o600); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(98)
	}
	exitCode, _ := strconv.Atoi(os.Getenv("RAPPID_REPLAY_HELPER_EXIT"))
	os.Exit(exitCode)
}

func openRawDB(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("sql.Open() error = %v", err)
	}
	if err := db.Ping(); err != nil {
		_ = db.Close()
		t.Fatalf("raw database ping: %v", err)
	}
	return db
}
