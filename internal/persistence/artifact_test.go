package persistence

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rappidAI-Research/rappid-replay/internal/id"
	"github.com/rappidAI-Research/rappid-replay/internal/state"
	"github.com/rappidAI-Research/rappid-replay/internal/store"
)

func TestPublishArtifactPersistsEventAndProvenanceAtomically(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, filepath.Join(t.TempDir(), "replay.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	cas, err := store.NewLocalStore(t.TempDir(), bytes.Repeat([]byte{0x79}, 32))
	if err != nil {
		t.Fatal(err)
	}
	defer cas.Close()

	workspace := t.TempDir()
	path := filepath.Join(workspace, "report.txt")
	if err := os.WriteFile(path, []byte("old"), 0o640); err != nil {
		t.Fatal(err)
	}
	initialSnapshot, err := (state.Snapshotter{CAS: cas}).Capture(workspace)
	if err != nil {
		t.Fatal(err)
	}
	previousObject := snapshotFileObject(t, cas, initialSnapshot.RootTreeID, "report.txt")

	sessionID, _ := id.NewSession()
	initialStateID, _ := id.NewState()
	nextStateID, _ := id.NewState()
	now := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)
	if err := db.CreateSession(ctx, SessionStart{ID: sessionID, Command: []string{"agent"}, CWD: workspace, StartedAt: now}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.PublishSnapshot(ctx, cas, PublishSnapshotRequest{
		StateID: initialStateID, SessionID: sessionID, RootTreeID: initialSnapshot.RootTreeID,
		Role: SnapshotInitial, WallTimeUTC: now.Add(time.Second), MonotonicNS: 1, Source: "recorder.generic",
	}); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(path, []byte("new report"), 0o640); err != nil {
		t.Fatal(err)
	}
	nextSnapshot, err := (state.Snapshotter{CAS: cas}).Capture(workspace)
	if err != nil {
		t.Fatal(err)
	}
	currentObject := snapshotFileObject(t, cas, nextSnapshot.RootTreeID, "report.txt")
	if _, err := db.PublishSnapshot(ctx, cas, PublishSnapshotRequest{
		StateID: nextStateID, SessionID: sessionID, RootTreeID: nextSnapshot.RootTreeID,
		Role: SnapshotCheckpoint, StateBefore: initialStateID,
		WallTimeUTC: now.Add(2 * time.Second), MonotonicNS: 2, Source: "recorder.generic",
	}); err != nil {
		t.Fatal(err)
	}

	published, err := db.PublishArtifact(ctx, PublishArtifactRequest{
		SessionID: sessionID, FromStateID: initialStateID, StateID: nextStateID,
		Path: []byte("report.txt"), ChangeKind: ArtifactModified,
		ObjectID: currentObject, PreviousObjectID: previousObject,
		Mode: 0o640, Size: int64(len("new report")),
		Discovery: ArtifactDiscoveryWorkspaceDelta, Source: "recorder.generic",
		WallTimeUTC: now.Add(3 * time.Second), MonotonicNS: 3,
	})
	if err != nil {
		t.Fatalf("PublishArtifact() error = %v", err)
	}
	if published.Event.Seq != 3 || published.Event.Type != ArtifactEventType {
		t.Fatalf("artifact event = %+v", published.Event)
	}
	if published.Event.StateBefore != initialStateID.String() || published.Event.StateAfter != nextStateID.String() {
		t.Fatalf("artifact event state boundary = %s -> %s", published.Event.StateBefore, published.Event.StateAfter)
	}
	if _, err := id.ParseArtifact(published.ID.String()); err != nil {
		t.Fatalf("artifact id = %q: %v", published.ID, err)
	}

	var eventSeq int64
	var fromState, toState, pathDisplay, changeKind, discovery, objectID, previousObjectID string
	var pathBytes []byte
	var size int64
	if err := db.sql.QueryRowContext(ctx, `
SELECT event_seq, from_state_id, state_id, path_bytes, path_display,
       change_kind, discovery, object_id, previous_object_id, size
FROM artifacts WHERE id = ?`, published.ID.String()).Scan(
		&eventSeq, &fromState, &toState, &pathBytes, &pathDisplay,
		&changeKind, &discovery, &objectID, &previousObjectID, &size,
	); err != nil {
		t.Fatal(err)
	}
	if eventSeq != 3 || fromState != initialStateID.String() || toState != nextStateID.String() {
		t.Fatalf("artifact provenance event/state = %d %s -> %s", eventSeq, fromState, toState)
	}
	if string(pathBytes) != "report.txt" || pathDisplay != "report.txt" {
		t.Fatalf("artifact path = %q / %q", pathBytes, pathDisplay)
	}
	if changeKind != string(ArtifactModified) || discovery != ArtifactDiscoveryWorkspaceDelta {
		t.Fatalf("artifact kind/discovery = %s/%s", changeKind, discovery)
	}
	if objectID != currentObject.String() || previousObjectID != previousObject.String() || size != int64(len("new report")) {
		t.Fatalf("artifact object provenance mismatch")
	}

	var payload artifactEventPayload
	if err := json.Unmarshal(published.Event.Payload, &payload); err != nil {
		t.Fatal(err)
	}
	decodedPath, err := base64.StdEncoding.DecodeString(payload.PathB64)
	if err != nil {
		t.Fatal(err)
	}
	if string(decodedPath) != "report.txt" || payload.ArtifactID != published.ID.String() {
		t.Fatalf("artifact event payload = %+v", payload)
	}

	unreachable, err := cas.PutObject(store.ObjectBlob, []byte("not in published state"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.PublishArtifact(ctx, PublishArtifactRequest{
		SessionID: sessionID, FromStateID: initialStateID, StateID: nextStateID,
		Path: []byte("unreachable.bin"), ChangeKind: ArtifactCreated,
		ObjectID: unreachable, Mode: 0o600, Size: 22,
		Discovery: ArtifactDiscoveryWorkspaceDelta, Source: "recorder.generic",
		WallTimeUTC: now.Add(4 * time.Second), MonotonicNS: 4,
	}); err == nil {
		t.Fatal("PublishArtifact() accepted object outside state reachability")
	}
	var artifacts, events, nextSeq int
	if err := db.sql.QueryRowContext(ctx, "SELECT COUNT(1) FROM artifacts WHERE session_id = ?", sessionID.String()).Scan(&artifacts); err != nil {
		t.Fatal(err)
	}
	if err := db.sql.QueryRowContext(ctx, "SELECT COUNT(1) FROM events WHERE session_id = ?", sessionID.String()).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if err := db.sql.QueryRowContext(ctx, "SELECT next_event_seq FROM sessions WHERE id = ?", sessionID.String()).Scan(&nextSeq); err != nil {
		t.Fatal(err)
	}
	if artifacts != 1 || events != 3 || nextSeq != 4 {
		t.Fatalf("failed artifact changed durable counts: artifacts/events/next = %d/%d/%d", artifacts, events, nextSeq)
	}
}

func snapshotFileObject(t *testing.T, cas *store.LocalStore, root store.ObjectID, name string) store.ObjectID {
	t.Helper()
	object, err := cas.GetObject(root)
	if err != nil {
		t.Fatal(err)
	}
	tree, err := state.ParseCanonicalTree(object.Payload)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range tree.Entries {
		if string(entry.Name) == name && entry.Kind == state.EntryFile {
			return entry.ObjectID
		}
	}
	t.Fatalf("file %q not found in snapshot", name)
	return ""
}
