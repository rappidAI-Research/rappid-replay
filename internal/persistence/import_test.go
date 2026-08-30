package persistence

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rappidAI-Research/rappid-replay/internal/event"
	"github.com/rappidAI-Research/rappid-replay/internal/id"
	"github.com/rappidAI-Research/rappid-replay/internal/store"
)

func TestImportEvidenceOrdersAndPreservesPortableLineage(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, filepath.Join(t.TempDir(), "replay.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	parentID, _ := id.NewSession()
	parentStateID, _ := id.NewState()
	childID, _ := id.NewSession()
	childStateID, _ := id.NewState()
	rootID := store.SumObject([]byte("shared-root"))
	now := time.Date(2026, 8, 30, 10, 0, 0, 0, time.UTC)

	parent := importedSessionFixture(parentID, parentStateID, rootID, "", 0, now)
	child := importedSessionFixture(childID, childStateID, rootID, parentID, 1, now.Add(time.Second))
	objects := []store.ObjectMetadata{{
		ID:            rootID,
		Kind:          store.ObjectTree,
		PlaintextSize: 32,
		StoredSize:    64,
	}}

	if err := db.ImportEvidence(ctx, objects, []ImportedSession{child, parent}); err != nil {
		t.Fatalf("ImportEvidence() error = %v", err)
	}
	got, err := db.GetSession(ctx, childID)
	if err != nil {
		t.Fatal(err)
	}
	if got.ParentSessionID != parentID || got.ForkEventSeq != 1 {
		t.Fatalf("child lineage = parent %s fork %d, want %s/1", got.ParentSessionID, got.ForkEventSeq, parentID)
	}
	stateRecord, err := db.GetState(ctx, childStateID)
	if err != nil {
		t.Fatal(err)
	}
	if stateRecord.RootTreeID != rootID {
		t.Fatalf("child initial root = %s, want %s", stateRecord.RootTreeID, rootID)
	}
}

func TestImportEvidenceRejectsLineageRootMismatchAtomically(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, filepath.Join(t.TempDir(), "replay.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	parentID, _ := id.NewSession()
	parentStateID, _ := id.NewState()
	childID, _ := id.NewSession()
	childStateID, _ := id.NewState()
	parentRoot := store.SumObject([]byte("parent-root"))
	childRoot := store.SumObject([]byte("child-root"))
	now := time.Date(2026, 8, 30, 10, 0, 0, 0, time.UTC)

	parent := importedSessionFixture(parentID, parentStateID, parentRoot, "", 0, now)
	child := importedSessionFixture(childID, childStateID, childRoot, parentID, 1, now.Add(time.Second))
	objects := []store.ObjectMetadata{
		{ID: parentRoot, Kind: store.ObjectTree, PlaintextSize: 32, StoredSize: 64},
		{ID: childRoot, Kind: store.ObjectTree, PlaintextSize: 32, StoredSize: 64},
	}

	err = db.ImportEvidence(ctx, objects, []ImportedSession{parent, child})
	if err == nil || !strings.Contains(err.Error(), "differs from parent fork root") {
		t.Fatalf("ImportEvidence() error = %v, want lineage root mismatch", err)
	}
	var sessions, objectsCount int
	if err := db.sql.QueryRowContext(ctx, "SELECT COUNT(1) FROM sessions").Scan(&sessions); err != nil {
		t.Fatal(err)
	}
	if err := db.sql.QueryRowContext(ctx, "SELECT COUNT(1) FROM objects").Scan(&objectsCount); err != nil {
		t.Fatal(err)
	}
	if sessions != 0 || objectsCount != 0 {
		t.Fatalf("failed import leaked metadata: sessions=%d objects=%d", sessions, objectsCount)
	}
}

func importedSessionFixture(
	sessionID id.SessionID,
	stateID id.StateID,
	rootID store.ObjectID,
	parentID id.SessionID,
	forkSeq uint64,
	now time.Time,
) ImportedSession {
	draft := event.NewDraft(
		sessionID.String(),
		"state.snapshot",
		"test",
		now,
		event.Privacy{Classification: "technical"},
		json.RawMessage(`{}`),
	)
	draft.StateAfter = stateID.String()
	persisted := draft.Stamp(1, 1)
	return ImportedSession{
		Record: SessionRecord{
			ID:                   sessionID,
			ParentSessionID:      parentID,
			ForkEventSeq:         forkSeq,
			Status:               "completed",
			Command:              []string{"agent"},
			CWD:                  "/workspace",
			StartedAt:            now,
			EndedAt:              now.Add(time.Second),
			InitialStateID:       stateID,
			FinalStateID:         stateID,
			ReproducibilityLevel: "R1",
		},
		Events: []event.Event{persisted},
		States: []ImportedState{{
			Record: StateRecord{
				ID:         stateID,
				SessionID:  sessionID,
				EventSeq:   1,
				RootTreeID: rootID,
				CreatedAt:  now,
			},
			Objects: []store.ObjectID{rootID},
		}},
	}
}
