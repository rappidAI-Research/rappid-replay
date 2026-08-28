package persistence

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rappidAI-Research/rappid-replay/internal/id"
	"github.com/rappidAI-Research/rappid-replay/internal/state"
	"github.com/rappidAI-Research/rappid-replay/internal/store"
)

func TestStoreEnvironmentRequiresInitialStateAndAdvancesToR2(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, filepath.Join(t.TempDir(), "replay.db"))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	cas, err := store.NewLocalStore(t.TempDir(), bytes.Repeat([]byte{0x61}, 32))
	if err != nil {
		t.Fatalf("NewLocalStore() error = %v", err)
	}
	t.Cleanup(func() { _ = cas.Close() })

	workspace := t.TempDir()
	if err := os.WriteFile(filepath.Join(workspace, "input.txt"), []byte("input"), 0o600); err != nil {
		t.Fatal(err)
	}
	snapshot, err := (state.Snapshotter{CAS: cas}).Capture(workspace)
	if err != nil {
		t.Fatal(err)
	}

	sessionID, err := id.NewSession()
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if err := db.CreateSession(ctx, SessionStart{
		ID: sessionID, Command: []string{"agent"}, CWD: workspace, StartedAt: now,
	}); err != nil {
		t.Fatal(err)
	}

	fingerprint := []byte(`{"schema":"rappid.replay.environment/1","os":"test"}`)
	if err := db.StoreEnvironment(ctx, sessionID, fingerprint); err == nil {
		t.Fatal("StoreEnvironment() succeeded before initial state publication")
	}

	stateID, err := id.NewState()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.PublishSnapshot(ctx, cas, PublishSnapshotRequest{
		StateID: stateID, SessionID: sessionID, RootTreeID: snapshot.RootTreeID,
		Role: SnapshotInitial, WallTimeUTC: now.Add(time.Second), MonotonicNS: 1, Source: "replay.core",
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.StoreEnvironment(ctx, sessionID, fingerprint); err != nil {
		t.Fatalf("StoreEnvironment() error = %v", err)
	}

	var stored []byte
	var level string
	if err := db.sql.QueryRowContext(ctx, `
SELECT e.fingerprint_json, s.reproducibility_level
FROM environments e
JOIN sessions s ON s.id = e.session_id
WHERE e.session_id = ?`, sessionID.String()).Scan(&stored, &level); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(stored, fingerprint) {
		t.Fatalf("stored fingerprint = %s, want %s", stored, fingerprint)
	}
	if level != "R2" {
		t.Fatalf("reproducibility_level = %q, want R2", level)
	}
	if err := db.StoreEnvironment(ctx, sessionID, fingerprint); err == nil {
		t.Fatal("StoreEnvironment() unexpectedly replaced existing fingerprint")
	}
}

func TestStoreEnvironmentRejectsNonObjectJSON(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, filepath.Join(t.TempDir(), "replay.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	sessionID, err := id.NewSession()
	if err != nil {
		t.Fatal(err)
	}
	for _, fingerprint := range [][]byte{nil, []byte("not-json"), []byte(`[]`), []byte(`null`)} {
		if err := db.StoreEnvironment(ctx, sessionID, fingerprint); err == nil {
			t.Fatalf("StoreEnvironment(%q) unexpectedly succeeded", fingerprint)
		}
	}
}
