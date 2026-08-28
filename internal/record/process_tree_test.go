package record

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/rappidAI-Research/rappid-replay/internal/persistence"
	"github.com/rappidAI-Research/rappid-replay/internal/store"
)

func TestGenericRecorderDiscoversDescendantProcesses(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "replay.db")
	db, err := persistence.Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	cas, err := store.NewLocalStore(t.TempDir(), bytes.Repeat([]byte{0x57}, 32))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cas.Close() })

	workspace := t.TempDir()
	result, err := Run(ctx, Dependencies{DB: db, CAS: cas}, Options{
		Command:       []string{os.Args[0], "-test.run=^TestReplayProcessTreeHelperProcess$"},
		WorkingDir:    workspace,
		TerminalInput: "off",
		Env: append(os.Environ(),
			"RAPPID_REPLAY_PROCESS_TREE_PARENT=1",
		),
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
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

	rootPID := 0
	discoveredChild := false
	treeSeq := uint64(0)
	exitSeq := uint64(0)
	for rows.Next() {
		var seq uint64
		var eventType string
		var payload []byte
		if err := rows.Scan(&seq, &eventType, &payload); err != nil {
			t.Fatal(err)
		}
		switch eventType {
		case "process.started":
			var started struct {
				PID int `json:"pid"`
			}
			if err := json.Unmarshal(payload, &started); err != nil {
				t.Fatal(err)
			}
			rootPID = started.PID
		case "process.discovered":
			var discovered struct {
				PID  int `json:"pid"`
				PPID int `json:"ppid"`
			}
			if err := json.Unmarshal(payload, &discovered); err != nil {
				t.Fatal(err)
			}
			if rootPID != 0 && discovered.PID != rootPID && discovered.PPID == rootPID {
				discoveredChild = true
			}
		case "process.tree":
			treeSeq = seq
		case "process.exited":
			exitSeq = seq
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if rootPID == 0 {
		t.Fatal("recording has no process.started root PID")
	}
	if !discoveredChild {
		t.Fatal("recorder did not discover the helper child process")
	}
	if treeSeq == 0 || exitSeq == 0 || treeSeq >= exitSeq {
		t.Fatalf("process.tree seq=%d process.exited seq=%d, want tree before exit", treeSeq, exitSeq)
	}
}

func TestReplayProcessTreeHelperProcess(t *testing.T) {
	if os.Getenv("RAPPID_REPLAY_PROCESS_TREE_CHILD") == "1" {
		time.Sleep(700 * time.Millisecond)
		os.Exit(0)
	}
	if os.Getenv("RAPPID_REPLAY_PROCESS_TREE_PARENT") != "1" {
		return
	}
	command := exec.Command(os.Args[0], "-test.run=^TestReplayProcessTreeHelperProcess$")
	command.Env = append(os.Environ(),
		"RAPPID_REPLAY_PROCESS_TREE_PARENT=",
		"RAPPID_REPLAY_PROCESS_TREE_CHILD=1",
	)
	command.Stdout = io.Discard
	command.Stderr = io.Discard
	if err := command.Start(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(97)
	}
	if err := command.Wait(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(98)
	}
	os.Exit(0)
}
