package persistence

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rappidAI-Research/rappid-replay/internal/id"
	"github.com/rappidAI-Research/rappid-replay/internal/state"
	"github.com/rappidAI-Research/rappid-replay/internal/store"
)

func TestCreateSessionAndPublishInitialSnapshotAtomically(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, filepath.Join(t.TempDir(), "replay.db"))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	objectsRoot := t.TempDir()
	cas, err := store.NewLocalStore(objectsRoot, bytes.Repeat([]byte{0x33}, 32))
	if err != nil {
		t.Fatalf("NewLocalStore() error = %v", err)
	}
	t.Cleanup(func() { _ = cas.Close() })

	workspace := t.TempDir()
	if err := os.WriteFile(filepath.Join(workspace, "hello.txt"), []byte("hello"), 0o640); err != nil {
		t.Fatalf("write workspace file: %v", err)
	}
	snapshot, err := (state.Snapshotter{CAS: cas}).Capture(workspace)
	if err != nil {
		t.Fatalf("Capture() error = %v", err)
	}

	sessionID, err := id.NewSession()
	if err != nil {
		t.Fatalf("NewSession() error = %v", err)
	}
	started := time.Date(2026, 8, 27, 17, 0, 0, 0, time.UTC)
	if err := db.CreateSession(ctx, SessionStart{
		ID:        sessionID,
		Command:   []string{"codex"},
		CWD:       workspace,
		StartedAt: started,
	}); err != nil {
		t.Fatalf("CreateSession() error = %v", err)
	}

	stateID, err := id.NewState()
	if err != nil {
		t.Fatalf("NewState() error = %v", err)
	}
	inspection, err := db.PublishSnapshot(ctx, cas, PublishSnapshotRequest{
		StateID:     stateID,
		SessionID:   sessionID,
		RootTreeID:  snapshot.RootTreeID,
		Role:        SnapshotInitial,
		EventSeq:    1,
		WallTimeUTC: started.Add(time.Second),
		MonotonicNS: 1_000_000,
		Source:      "replay.core",
	})
	if err != nil {
		t.Fatalf("PublishSnapshot() error = %v", err)
	}
	if len(inspection.Objects) != 2 {
		t.Fatalf("reachable object count = %d, want 2", len(inspection.Objects))
	}

	var initialState, level string
	if err := db.sql.QueryRowContext(ctx,
		"SELECT initial_state_id, reproducibility_level FROM sessions WHERE id = ?", sessionID.String(),
	).Scan(&initialState, &level); err != nil {
		t.Fatalf("read session: %v", err)
	}
	if initialState != stateID.String() {
		t.Fatalf("initial_state_id = %q, want %q", initialState, stateID)
	}
	if level != "R1" {
		t.Fatalf("reproducibility_level = %q, want R1", level)
	}

	var stateCount, objectCount, edgeCount, eventCount int
	queries := []struct {
		query string
		args  []any
		out   *int
	}{
		{"SELECT COUNT(1) FROM states WHERE session_id = ?", []any{sessionID.String()}, &stateCount},
		{"SELECT COUNT(1) FROM objects", nil, &objectCount},
		{"SELECT COUNT(1) FROM state_objects WHERE state_id = ?", []any{stateID.String()}, &edgeCount},
		{"SELECT COUNT(1) FROM events WHERE session_id = ? AND type = 'state.snapshot'", []any{sessionID.String()}, &eventCount},
	}
	for _, item := range queries {
		if err := db.sql.QueryRowContext(ctx, item.query, item.args...).Scan(item.out); err != nil {
			t.Fatalf("query %q: %v", item.query, err)
		}
	}
	if stateCount != 1 || eventCount != 1 {
		t.Fatalf("state/event counts = %d/%d, want 1/1", stateCount, eventCount)
	}
	if objectCount != len(inspection.Objects) || edgeCount != len(inspection.Objects) {
		t.Fatalf("object/edge counts = %d/%d, want %d", objectCount, edgeCount, len(inspection.Objects))
	}

	var eventStateAfter, payload string
	if err := db.sql.QueryRowContext(ctx,
		"SELECT state_after, payload_json FROM events WHERE session_id = ? AND seq = 1", sessionID.String(),
	).Scan(&eventStateAfter, &payload); err != nil {
		t.Fatalf("read snapshot event: %v", err)
	}
	if eventStateAfter != stateID.String() {
		t.Fatalf("event state_after = %q, want %q", eventStateAfter, stateID)
	}
	if !strings.Contains(payload, snapshot.RootTreeID.String()) {
		t.Fatalf("snapshot event payload does not contain root tree id: %s", payload)
	}
}

func TestPublishSnapshotVerificationFailureLeavesNoStateRows(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, filepath.Join(t.TempDir(), "replay.db"))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	objectsRoot := t.TempDir()
	cas, err := store.NewLocalStore(objectsRoot, bytes.Repeat([]byte{0x44}, 32))
	if err != nil {
		t.Fatalf("NewLocalStore() error = %v", err)
	}
	t.Cleanup(func() { _ = cas.Close() })

	workspace := t.TempDir()
	if err := os.WriteFile(filepath.Join(workspace, "unstable.txt"), []byte("data"), 0o600); err != nil {
		t.Fatalf("write workspace file: %v", err)
	}
	snapshot, err := (state.Snapshotter{CAS: cas}).Capture(workspace)
	if err != nil {
		t.Fatalf("Capture() error = %v", err)
	}

	sessionID, _ := id.NewSession()
	stateID, _ := id.NewState()
	now := time.Now().UTC()
	if err := db.CreateSession(ctx, SessionStart{ID: sessionID, Command: []string{"agent"}, CWD: workspace, StartedAt: now}); err != nil {
		t.Fatalf("CreateSession() error = %v", err)
	}

	digest := strings.TrimPrefix(snapshot.RootTreeID.String(), "b3:")
	rootObjectPath := filepath.Join(objectsRoot, "b3", digest[:2], digest[2:4], digest[4:])
	if err := os.Remove(rootObjectPath); err != nil {
		t.Fatalf("remove root object: %v", err)
	}

	if _, err := db.PublishSnapshot(ctx, cas, PublishSnapshotRequest{
		StateID: stateID, SessionID: sessionID, RootTreeID: snapshot.RootTreeID,
		Role: SnapshotInitial, EventSeq: 1, WallTimeUTC: now, MonotonicNS: 1, Source: "replay.core",
	}); err == nil {
		t.Fatal("PublishSnapshot() succeeded with missing root CAS object")
	}

	for _, table := range []string{"states", "objects", "state_objects", "events"} {
		var count int
		if err := db.sql.QueryRowContext(ctx, "SELECT COUNT(1) FROM "+table).Scan(&count); err != nil {
			t.Fatalf("count %s: %v", table, err)
		}
		if count != 0 {
			t.Fatalf("%s count = %d after failed publication, want 0", table, count)
		}
	}
}

func TestPublishSecondInitialSnapshotRollsBackMetadata(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, filepath.Join(t.TempDir(), "replay.db"))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	cas, err := store.NewLocalStore(t.TempDir(), bytes.Repeat([]byte{0x55}, 32))
	if err != nil {
		t.Fatalf("NewLocalStore() error = %v", err)
	}
	t.Cleanup(func() { _ = cas.Close() })
	workspace := t.TempDir()
	file := filepath.Join(workspace, "x.txt")
	if err := os.WriteFile(file, []byte("one"), 0o600); err != nil {
		t.Fatalf("write first content: %v", err)
	}
	firstSnapshot, err := (state.Snapshotter{CAS: cas}).Capture(workspace)
	if err != nil {
		t.Fatalf("capture first snapshot: %v", err)
	}

	sessionID, _ := id.NewSession()
	firstStateID, _ := id.NewState()
	now := time.Now().UTC()
	if err := db.CreateSession(ctx, SessionStart{ID: sessionID, Command: []string{"agent"}, CWD: workspace, StartedAt: now}); err != nil {
		t.Fatalf("CreateSession() error = %v", err)
	}
	if _, err := db.PublishSnapshot(ctx, cas, PublishSnapshotRequest{
		StateID: firstStateID, SessionID: sessionID, RootTreeID: firstSnapshot.RootTreeID,
		Role: SnapshotInitial, EventSeq: 1, WallTimeUTC: now, MonotonicNS: 1, Source: "replay.core",
	}); err != nil {
		t.Fatalf("publish first snapshot: %v", err)
	}

	if err := os.WriteFile(file, []byte("two"), 0o600); err != nil {
		t.Fatalf("write second content: %v", err)
	}
	secondSnapshot, err := (state.Snapshotter{CAS: cas}).Capture(workspace)
	if err != nil {
		t.Fatalf("capture second snapshot: %v", err)
	}
	secondStateID, _ := id.NewState()
	if _, err := db.PublishSnapshot(ctx, cas, PublishSnapshotRequest{
		StateID: secondStateID, SessionID: sessionID, RootTreeID: secondSnapshot.RootTreeID,
		Role: SnapshotInitial, EventSeq: 2, WallTimeUTC: now.Add(time.Second), MonotonicNS: 2, Source: "replay.core",
	}); err == nil {
		t.Fatal("second initial snapshot unexpectedly succeeded")
	}

	var states, events int
	if err := db.sql.QueryRowContext(ctx, "SELECT COUNT(1) FROM states WHERE session_id = ?", sessionID.String()).Scan(&states); err != nil {
		t.Fatalf("count states: %v", err)
	}
	if err := db.sql.QueryRowContext(ctx, "SELECT COUNT(1) FROM events WHERE session_id = ?", sessionID.String()).Scan(&events); err != nil {
		t.Fatalf("count events: %v", err)
	}
	if states != 1 || events != 1 {
		t.Fatalf("states/events after rollback = %d/%d, want 1/1", states, events)
	}
}
