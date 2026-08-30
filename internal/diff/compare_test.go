package diff

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/rappidAI-Research/rappid-replay/internal/id"
	"github.com/rappidAI-Research/rappid-replay/internal/persistence"
	"github.com/rappidAI-Research/rappid-replay/internal/state"
	"github.com/rappidAI-Research/rappid-replay/internal/store"
)

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
