package record

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rappidAI-Research/rappid-replay/internal/ignore"
	"github.com/rappidAI-Research/rappid-replay/internal/persistence"
	"github.com/rappidAI-Research/rappid-replay/internal/store"
)

func TestGenericRecorderPublishesWatcherTriggeredCheckpoint(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "replay.db")
	db, err := persistence.Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	cas, err := store.NewLocalStore(t.TempDir(), bytes.Repeat([]byte{0x79}, 32))
	if err != nil {
		t.Fatalf("NewLocalStore() error = %v", err)
	}
	t.Cleanup(func() { _ = cas.Close() })

	workspace := t.TempDir()
	if err := os.WriteFile(filepath.Join(workspace, "before.txt"), []byte("before"), 0o600); err != nil {
		t.Fatal(err)
	}
	result, err := Run(ctx, Dependencies{DB: db, CAS: cas}, Options{
		Command: []string{os.Args[0], "-test.run=^TestReplayReconciliationHelperProcess$", "--", workspace},
		WorkingDir: workspace,
		Env: append(os.Environ(),
			"RAPPID_REPLAY_RECONCILE_HELPER=1",
		),
		TerminalInput: "off",
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if result.FinalStateID == "" {
		t.Fatalf("recording has no final state: %+v", result)
	}

	raw := openRawDB(t, dbPath)
	defer raw.Close()
	rows, err := raw.QueryContext(ctx, `
SELECT seq, type, payload_json
FROM events
WHERE session_id = ?
ORDER BY seq`, result.SessionID.String())
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	var processStartedSeq, checkpointSeq, processExitedSeq uint64
	for rows.Next() {
		var seq uint64
		var eventType string
		var payload []byte
		if err := rows.Scan(&seq, &eventType, &payload); err != nil {
			t.Fatal(err)
		}
		switch eventType {
		case "process.started":
			processStartedSeq = seq
		case "process.exited":
			processExitedSeq = seq
		case "state.snapshot":
			var snapshot struct {
				Role string `json:"role"`
			}
			if err := json.Unmarshal(payload, &snapshot); err != nil {
				t.Fatalf("decode snapshot payload: %v", err)
			}
			if snapshot.Role == "checkpoint" && checkpointSeq == 0 {
				checkpointSeq = seq
			}
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if processStartedSeq == 0 || processExitedSeq == 0 || checkpointSeq == 0 {
		t.Fatalf("event boundaries process.started=%d checkpoint=%d process.exited=%d", processStartedSeq, checkpointSeq, processExitedSeq)
	}
	if !(processStartedSeq < checkpointSeq && checkpointSeq < processExitedSeq) {
		t.Fatalf("checkpoint sequence %d is not between process start %d and exit %d", checkpointSeq, processStartedSeq, processExitedSeq)
	}

	var stateCount int
	if err := raw.QueryRowContext(ctx,
		"SELECT COUNT(1) FROM states WHERE session_id = ?", result.SessionID.String(),
	).Scan(&stateCount); err != nil {
		t.Fatal(err)
	}
	if stateCount < 3 {
		t.Fatalf("published state count = %d, want at least initial/checkpoint/final", stateCount)
	}
}

func TestWorkspaceWatcherSkipsExcludedDirectories(t *testing.T) {
	root := t.TempDir()
	keep := filepath.Join(root, "src")
	ignored := filepath.Join(root, "build")
	gitDir := filepath.Join(root, ".git")
	for _, dir := range []string{keep, ignored, gitDir} {
		if err := os.MkdirAll(filepath.Join(dir, "nested"), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	policy, err := ignore.New([]string{"build/**"})
	if err != nil {
		t.Fatal(err)
	}
	watcher, err := newWorkspaceWatcher(root, policy)
	if err != nil {
		t.Fatalf("newWorkspaceWatcher() error = %v", err)
	}
	defer watcher.Close()

	watched := make(map[string]bool)
	for _, path := range watcher.watcher.WatchList() {
		watched[filepath.Clean(path)] = true
	}
	if !watched[filepath.Clean(root)] || !watched[filepath.Clean(keep)] {
		t.Fatalf("watch list missing included directories: %v", watcher.watcher.WatchList())
	}
	for path := range watched {
		if path == filepath.Clean(ignored) || path == filepath.Clean(gitDir) ||
			filepath.Dir(path) == filepath.Clean(ignored) || filepath.Dir(path) == filepath.Clean(gitDir) {
			t.Fatalf("excluded directory was watched: %s", path)
		}
	}
}

func TestReplayReconciliationHelperProcess(t *testing.T) {
	if os.Getenv("RAPPID_REPLAY_RECONCILE_HELPER") != "1" {
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
		fmt.Fprintln(os.Stderr, "reconciliation helper missing workspace")
		os.Exit(97)
	}
	workspace := os.Args[separator+1]
	if err := os.WriteFile(filepath.Join(workspace, "checkpoint-one.txt"), []byte("one"), 0o600); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(98)
	}
	time.Sleep(450 * time.Millisecond)
	if err := os.WriteFile(filepath.Join(workspace, "checkpoint-two.txt"), []byte("two"), 0o600); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(99)
	}
	time.Sleep(450 * time.Millisecond)
	os.Exit(0)
}
