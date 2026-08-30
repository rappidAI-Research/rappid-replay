package diff

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rappidAI-Research/rappid-replay/internal/event"
	"github.com/rappidAI-Research/rappid-replay/internal/id"
	"github.com/rappidAI-Research/rappid-replay/internal/persistence"
	"github.com/rappidAI-Research/rappid-replay/internal/state"
	"github.com/rappidAI-Research/rappid-replay/internal/store"
)

func TestCompareSessionsSeparatesStateTimelineProcessAndOutcome(t *testing.T) {
	ctx := context.Background()
	db, err := persistence.Open(ctx, filepath.Join(t.TempDir(), "replay.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	cas, err := store.NewLocalStore(t.TempDir(), bytes.Repeat([]byte{0x74}, 32))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cas.Close() })

	leftID := createCompletedDiffSession(t, ctx, db, cas, "left")
	rightID := createCompletedDiffSession(t, ctx, db, cas, "right")

	result, err := CompareSessions(ctx, Dependencies{DB: db, CAS: cas}, leftID, rightID, Options{MaxStateChanges: 100})
	if err != nil {
		t.Fatalf("CompareSessions() error = %v", err)
	}
	if result.Identical {
		t.Fatal("different final evidence reported technically identical")
	}
	if result.Lineage.Related {
		t.Fatalf("independent sessions reported related: %+v", result.Lineage)
	}
	if !result.State.Comparable || result.State.Equal || result.State.Modified != 1 || result.State.TotalChanges != 1 {
		t.Fatalf("state diff = %+v", result.State)
	}
	if result.Timeline.Equal || result.Timeline.CommonPrefixEvents != 3 {
		t.Fatalf("timeline diff = %+v", result.Timeline)
	}
	if !result.Process.Equal || result.Process.LeftEvents != 2 || result.Process.RightEvents != 2 {
		t.Fatalf("process diff = %+v", result.Process)
	}
	if !result.Agent.Equal || result.Agent.LeftEvents != 0 || result.Agent.RightEvents != 0 {
		t.Fatalf("agent diff = %+v", result.Agent)
	}
	if result.Outcome.Equal || result.Outcome.Left.ExitCode == nil || result.Outcome.Right.ExitCode == nil {
		t.Fatalf("outcome diff = %+v", result.Outcome)
	}
}

func TestCompareLineageUsesDurableForkStateRelation(t *testing.T) {
	ctx := context.Background()
	db, err := persistence.Open(ctx, filepath.Join(t.TempDir(), "replay.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	cas, err := store.NewLocalStore(t.TempDir(), bytes.Repeat([]byte{0x73}, 32))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cas.Close() })

	parentID, _ := id.NewSession()
	now := time.Now().UTC()
	if err := db.CreateSession(ctx, persistence.SessionStart{
		ID: parentID, Command: []string{"parent"}, CWD: t.TempDir(), StartedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := (state.Snapshotter{CAS: cas}).Capture(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	forkStateID, _ := id.NewState()
	published, err := db.PublishSnapshot(ctx, cas, persistence.PublishSnapshotRequest{
		StateID: forkStateID, SessionID: parentID, RootTreeID: snapshot.RootTreeID,
		Role: persistence.SnapshotInitial, WallTimeUTC: now.Add(time.Millisecond), MonotonicNS: 1, Source: "test",
	})
	if err != nil {
		t.Fatal(err)
	}

	childID, _ := id.NewSession()
	if err := db.CreateSession(ctx, persistence.SessionStart{
		ID: childID, Command: []string{"child"}, CWD: t.TempDir(), StartedAt: now.Add(time.Second),
		ParentSessionID: parentID, ForkEventSeq: published.Event.Seq, ForkStateID: forkStateID,
	}); err != nil {
		t.Fatal(err)
	}
	parent, err := db.GetSession(ctx, parentID)
	if err != nil {
		t.Fatal(err)
	}
	child, err := db.GetSession(ctx, childID)
	if err != nil {
		t.Fatal(err)
	}

	lineage, err := compareLineage(ctx, db, parent, child)
	if err != nil {
		t.Fatal(err)
	}
	if !lineage.Related || lineage.CommonSessionID != parentID.String() {
		t.Fatalf("lineage = %+v", lineage)
	}
	if lineage.LeftDepth != 0 || lineage.RightDepth != 1 {
		t.Fatalf("lineage depths = %d/%d, want 0/1", lineage.LeftDepth, lineage.RightDepth)
	}
	if lineage.SharedThroughEventSeq != published.Event.Seq || lineage.RightForkEventSeq != published.Event.Seq {
		t.Fatalf("lineage fork = %+v", lineage)
	}
}

func TestCompareLineageDoesNotInferRelationshipFromSimilarity(t *testing.T) {
	leftID, _ := id.NewSession()
	rightID, _ := id.NewSession()
	left := persistence.SessionRecord{ID: leftID}
	right := persistence.SessionRecord{ID: rightID}

	lineage, err := compareLineage(context.Background(), nil, left, right)
	if err != nil {
		t.Fatal(err)
	}
	if lineage.Related {
		t.Fatalf("unrelated roots reported related: %+v", lineage)
	}
}

func createCompletedDiffSession(
	t *testing.T,
	ctx context.Context,
	db *persistence.DB,
	cas *store.LocalStore,
	finalContent string,
) id.SessionID {
	t.Helper()
	sessionID, _ := id.NewSession()
	now := time.Now().UTC()
	cwd := t.TempDir()
	if err := db.CreateSession(ctx, persistence.SessionStart{
		ID: sessionID, Command: []string{"tool", "run"}, CWD: cwd, StartedAt: now,
	}); err != nil {
		t.Fatal(err)
	}

	initialWorkspace := t.TempDir()
	initialSnapshot, err := (state.Snapshotter{CAS: cas}).Capture(initialWorkspace)
	if err != nil {
		t.Fatal(err)
	}
	initialStateID, _ := id.NewState()
	if _, err := db.PublishSnapshot(ctx, cas, persistence.PublishSnapshotRequest{
		StateID: initialStateID, SessionID: sessionID, RootTreeID: initialSnapshot.RootTreeID,
		Role: persistence.SnapshotInitial, WallTimeUTC: now.Add(time.Millisecond), MonotonicNS: 1, Source: "test",
	}); err != nil {
		t.Fatal(err)
	}

	startedPayload, _ := json.Marshal(struct {
		PID     int      `json:"pid"`
		Path    string   `json:"path"`
		Command []string `json:"command"`
		CWD     string   `json:"cwd"`
		PTY     bool     `json:"pty"`
	}{PID: 100 + len(finalContent), Path: "/usr/bin/tool", Command: []string{"tool", "run"}, CWD: cwd, PTY: false})
	started := event.NewDraft(sessionID.String(), "process.started", "test", now.Add(2*time.Millisecond), event.Privacy{Classification: "technical"}, startedPayload)
	started.StateBefore = initialStateID.String()
	started.StateAfter = initialStateID.String()
	if _, err := db.AppendEvent(ctx, started, 2); err != nil {
		t.Fatal(err)
	}

	exitedPayload, _ := json.Marshal(struct {
		PID      int  `json:"pid"`
		ExitCode int  `json:"exit_code"`
		Success  bool `json:"success"`
	}{PID: 100 + len(finalContent), ExitCode: 0, Success: true})
	exited := event.NewDraft(sessionID.String(), "process.exited", "test", now.Add(3*time.Millisecond), event.Privacy{Classification: "technical"}, exitedPayload)
	exited.StateBefore = initialStateID.String()
	exited.StateAfter = initialStateID.String()
	if _, err := db.AppendEvent(ctx, exited, 3); err != nil {
		t.Fatal(err)
	}

	finalWorkspace := t.TempDir()
	if err := os.WriteFile(filepath.Join(finalWorkspace, "result.txt"), []byte(finalContent), 0o600); err != nil {
		t.Fatal(err)
	}
	finalSnapshot, err := (state.Snapshotter{CAS: cas}).Capture(finalWorkspace)
	if err != nil {
		t.Fatal(err)
	}
	finalStateID, _ := id.NewState()
	if _, err := db.PublishSnapshot(ctx, cas, persistence.PublishSnapshotRequest{
		StateID: finalStateID, SessionID: sessionID, RootTreeID: finalSnapshot.RootTreeID,
		Role: persistence.SnapshotFinal, StateBefore: initialStateID,
		WallTimeUTC: now.Add(4 * time.Millisecond), MonotonicNS: 4, Source: "test",
	}); err != nil {
		t.Fatal(err)
	}

	exitCode := 0
	if _, err := db.EndSession(ctx, persistence.SessionEnd{
		SessionID: sessionID, Status: persistence.SessionCompleted, EndedAt: now.Add(5 * time.Millisecond),
		MonotonicNS: 5, StateID: finalStateID, Source: "test", ExitCode: &exitCode,
	}); err != nil {
		t.Fatal(err)
	}
	return sessionID
}
