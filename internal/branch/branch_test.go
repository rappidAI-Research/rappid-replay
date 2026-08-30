package branch

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/rappidAI-Research/rappid-replay/internal/persistence"
	"github.com/rappidAI-Research/rappid-replay/internal/record"
	"github.com/rappidAI-Research/rappid-replay/internal/store"
)

const branchHelperEnv = "RAPPID_BRANCH_HELPER_MODE"

func TestBranchHelperProcess(t *testing.T) {
	mode := os.Getenv(branchHelperEnv)
	if mode == "" {
		return
	}
	if mode == "write" {
		if err := os.WriteFile("rerun.txt", []byte("child\n"), 0o640); err != nil {
			t.Fatalf("write rerun.txt: %v", err)
		}
	}
}

func TestCreateMaterializesExactHistoricalStateWithoutExecution(t *testing.T) {
	ctx := context.Background()
	deps := testDependencies(t, ctx)
	sourceDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(sourceDir, "base.txt"), []byte("base\n"), 0o640); err != nil {
		t.Fatal(err)
	}
	source := recordSourceSession(t, ctx, deps, sourceDir)

	destination := filepath.Join(t.TempDir(), "branch")
	created, err := Create(ctx, deps, CreateOptions{StateID: source.FinalStateID, Destination: destination})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if created.Source.ID != source.FinalStateID {
		t.Fatalf("source state = %s, want %s", created.Source.ID, source.FinalStateID)
	}
	content, err := os.ReadFile(filepath.Join(destination, "base.txt"))
	if err != nil {
		t.Fatalf("read branched file: %v", err)
	}
	if string(content) != "base\n" {
		t.Fatalf("branched file = %q", content)
	}
	if _, err := os.Stat(filepath.Join(destination, "rerun.txt")); !os.IsNotExist(err) {
		t.Fatalf("branch unexpectedly executed child command: stat error = %v", err)
	}
}

func TestLiveRerunPersistsParentLineageAndExactInitialRoot(t *testing.T) {
	ctx := context.Background()
	deps := testDependencies(t, ctx)
	sourceDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(sourceDir, "base.txt"), []byte("base\n"), 0o640); err != nil {
		t.Fatal(err)
	}
	source := recordSourceSession(t, ctx, deps, sourceDir)
	sourceState, err := deps.DB.GetState(ctx, source.FinalStateID)
	if err != nil {
		t.Fatalf("GetState(source) error = %v", err)
	}

	destination := filepath.Join(t.TempDir(), "rerun")
	result, err := Rerun(ctx, deps, RerunOptions{
		StateID:          source.FinalStateID,
		Destination:      destination,
		Mode:             ModeLive,
		ConfirmExecution: true,
		Command:          []string{os.Args[0], "-test.run=TestBranchHelperProcess"},
		TerminalInput:    "off",
		Stdout:           io.Discard,
		Stderr:           io.Discard,
		Env:              append(os.Environ(), branchHelperEnv+"=write"),
	})
	if err != nil {
		t.Fatalf("Rerun() error = %v", err)
	}
	if result.Run.SessionID == source.SessionID {
		t.Fatal("rerun reused parent session id")
	}

	child, err := deps.DB.GetSession(ctx, result.Run.SessionID)
	if err != nil {
		t.Fatalf("GetSession(child) error = %v", err)
	}
	if child.ParentSessionID != source.SessionID {
		t.Fatalf("parent session = %s, want %s", child.ParentSessionID, source.SessionID)
	}
	if child.ForkEventSeq != sourceState.EventSeq {
		t.Fatalf("fork sequence = %d, want %d", child.ForkEventSeq, sourceState.EventSeq)
	}
	initial, err := deps.DB.GetState(ctx, result.Run.InitialStateID)
	if err != nil {
		t.Fatalf("GetState(child initial) error = %v", err)
	}
	if initial.RootTreeID != sourceState.RootTreeID {
		t.Fatalf("child initial root = %s, want source root %s", initial.RootTreeID, sourceState.RootTreeID)
	}
	if content, err := os.ReadFile(filepath.Join(destination, "rerun.txt")); err != nil || string(content) != "child\n" {
		t.Fatalf("rerun output file = %q, err = %v", content, err)
	}
}

func TestLiveRerunRequiresExplicitConfirmationBeforeMaterialization(t *testing.T) {
	ctx := context.Background()
	deps := testDependencies(t, ctx)
	destination := filepath.Join(t.TempDir(), "must-not-exist")
	_, err := Rerun(ctx, deps, RerunOptions{
		Mode:        ModeLive,
		Destination: destination,
		Command:     []string{"ignored"},
	})
	if err == nil {
		t.Fatal("Rerun() accepted live execution without confirmation")
	}
	if _, statErr := os.Stat(destination); !os.IsNotExist(statErr) {
		t.Fatalf("unconfirmed rerun touched destination: %v", statErr)
	}
}

func TestNonLiveModesFailClosedBeforeMaterialization(t *testing.T) {
	ctx := context.Background()
	deps := testDependencies(t, ctx)
	for _, mode := range []Mode{ModeRecorded, ModeControlled, ModeHybrid} {
		t.Run(string(mode), func(t *testing.T) {
			destination := filepath.Join(t.TempDir(), "must-not-exist")
			_, err := Rerun(ctx, deps, RerunOptions{
				Mode:             mode,
				Destination:      destination,
				ConfirmExecution: true,
				Command:          []string{"ignored"},
			})
			if err == nil {
				t.Fatalf("Rerun() unexpectedly accepted %s mode", mode)
			}
			if _, statErr := os.Stat(destination); !os.IsNotExist(statErr) {
				t.Fatalf("unsupported mode touched destination: %v", statErr)
			}
		})
	}
}

func testDependencies(t *testing.T, ctx context.Context) Dependencies {
	t.Helper()
	db, err := persistence.Open(ctx, filepath.Join(t.TempDir(), "replay.db"))
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	cas, err := store.NewLocalStore(filepath.Join(t.TempDir(), "objects"), bytes.Repeat([]byte{0x6b}, 32))
	if err != nil {
		t.Fatalf("open CAS: %v", err)
	}
	t.Cleanup(func() { _ = cas.Close() })
	return Dependencies{DB: db, CAS: cas}
}

func recordSourceSession(t *testing.T, ctx context.Context, deps Dependencies, sourceDir string) record.Result {
	t.Helper()
	result, err := record.Run(ctx, record.Dependencies{DB: deps.DB, CAS: deps.CAS}, record.Options{
		Command:       []string{os.Args[0], "-test.run=TestBranchHelperProcess"},
		WorkingDir:    sourceDir,
		TerminalInput: "off",
		Stdout:        io.Discard,
		Stderr:        io.Discard,
		Env:           append(os.Environ(), branchHelperEnv+"=source"),
	})
	if err != nil {
		t.Fatalf("record source session: %v", err)
	}
	return result
}
