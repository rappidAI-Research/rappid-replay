package persistence

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rappidAI-Research/rappid-replay/internal/id"
	"github.com/rappidAI-Research/rappid-replay/internal/state"
	"github.com/rappidAI-Research/rappid-replay/internal/store"
)

func TestGetStateReturnsPublishedSnapshotMetadata(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, filepath.Join(t.TempDir(), "replay.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	cas, err := store.NewLocalStore(t.TempDir(), bytes.Repeat([]byte{0x51}, 32))
	if err != nil {
		t.Fatal(err)
	}
	defer cas.Close()

	workspace := t.TempDir()
	if err := os.WriteFile(filepath.Join(workspace, "hello.txt"), []byte("hello"), 0o640); err != nil {
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
	stateID, err := id.NewState()
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now().UTC()
	if err := db.CreateSession(ctx, SessionStart{
		ID: sessionID, Command: []string{"test"}, CWD: workspace, StartedAt: started,
	}); err != nil {
		t.Fatal(err)
	}
	published, err := db.PublishSnapshot(ctx, cas, PublishSnapshotRequest{
		StateID: stateID, SessionID: sessionID, RootTreeID: snapshot.RootTreeID,
		Role: SnapshotInitial, WallTimeUTC: started, MonotonicNS: 1, Source: "test",
	})
	if err != nil {
		t.Fatal(err)
	}

	got, err := db.GetState(ctx, stateID)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != stateID || got.SessionID != sessionID || got.RootTreeID != snapshot.RootTreeID {
		t.Fatalf("GetState() = %+v", got)
	}
	if got.EventSeq != published.Event.Seq {
		t.Fatalf("event seq = %d, want %d", got.EventSeq, published.Event.Seq)
	}
	if got.CreatedAt.IsZero() {
		t.Fatal("created_at is zero")
	}
}

func TestGetStateNotFound(t *testing.T) {
	db, err := Open(context.Background(), filepath.Join(t.TempDir(), "replay.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	stateID, err := id.NewState()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.GetState(context.Background(), stateID); !errors.Is(err, ErrStateNotFound) {
		t.Fatalf("GetState() error = %v, want ErrStateNotFound", err)
	}
}
