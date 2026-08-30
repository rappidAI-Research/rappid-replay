package replay

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/rappidAI-Research/rappid-replay/internal/id"
	"github.com/rappidAI-Research/rappid-replay/internal/persistence"
	"github.com/rappidAI-Research/rappid-replay/internal/state"
	"github.com/rappidAI-Research/rappid-replay/internal/store"
)

func TestRestoreStateMaterializesAuthenticatedTree(t *testing.T) {
	deps, stateID, wantChunked := testPublishedState(t)
	defer deps.DB.Close()
	defer deps.CAS.Close()

	destination := filepath.Join(t.TempDir(), "restored")
	result, err := RestoreState(context.Background(), deps, RestoreOptions{StateID: stateID, Destination: destination})
	if err != nil {
		t.Fatalf("RestoreState() error = %v", err)
	}
	abs, _ := filepath.Abs(destination)
	if result.Destination != abs {
		t.Fatalf("destination = %q, want %q", result.Destination, abs)
	}
	if result.State.ID != stateID || result.Verification.Files != 3 || result.Verification.Directories != 1 {
		t.Fatalf("restore result = %+v", result)
	}
	got, err := os.ReadFile(filepath.Join(destination, "chunked.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, wantChunked) {
		t.Fatalf("chunked file = %q, want %q", got, wantChunked)
	}
	got, err = os.ReadFile(filepath.Join(destination, "nested", "note.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "nested" {
		t.Fatalf("nested file = %q", got)
	}
	if runtime.GOOS != "windows" {
		info, err := os.Stat(filepath.Join(destination, "plain.txt"))
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != 0o640 {
			t.Fatalf("plain file mode = %o, want 640", info.Mode().Perm())
		}
		target, err := os.Readlink(filepath.Join(destination, "note-link"))
		if err != nil {
			t.Fatal(err)
		}
		if target != "nested/note.txt" {
			t.Fatalf("symlink target = %q", target)
		}
	}
}

func TestRestoreStateRequiresForceAndReplacesDestination(t *testing.T) {
	deps, stateID, _ := testPublishedState(t)
	defer deps.DB.Close()
	defer deps.CAS.Close()

	destination := filepath.Join(t.TempDir(), "existing")
	if err := os.Mkdir(destination, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(destination, "stale.txt"), []byte("stale"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := RestoreState(context.Background(), deps, RestoreOptions{StateID: stateID, Destination: destination}); err == nil || !strings.Contains(err.Error(), "--force") {
		t.Fatalf("RestoreState() error = %v, want --force requirement", err)
	}
	if _, err := RestoreState(context.Background(), deps, RestoreOptions{StateID: stateID, Destination: destination, Force: true}); err != nil {
		t.Fatalf("forced RestoreState() error = %v", err)
	}
	if _, err := os.Stat(filepath.Join(destination, "stale.txt")); !os.IsNotExist(err) {
		t.Fatalf("stale file still exists, stat error = %v", err)
	}
	if got, err := os.ReadFile(filepath.Join(destination, "plain.txt")); err != nil || string(got) != "plain" {
		t.Fatalf("restored plain file = %q, err = %v", got, err)
	}
}

func TestVerifyStateDoesNotMaterialize(t *testing.T) {
	deps, stateID, _ := testPublishedState(t)
	defer deps.DB.Close()
	defer deps.CAS.Close()

	result, err := VerifyState(context.Background(), deps, stateID)
	if err != nil {
		t.Fatal(err)
	}
	if result.State.ID != stateID || result.Verification.Files != 3 {
		t.Fatalf("VerifyState() = %+v", result)
	}
}

func TestDefaultRestoreDestinationIncludesStateID(t *testing.T) {
	stateID, err := id.NewState()
	if err != nil {
		t.Fatal(err)
	}
	if got := DefaultRestoreDestination(stateID); got != "rappid-restore-"+stateID.String() {
		t.Fatalf("DefaultRestoreDestination() = %q", got)
	}
}

func testPublishedState(t *testing.T) (Dependencies, id.StateID, []byte) {
	t.Helper()
	ctx := context.Background()
	db, err := persistence.Open(ctx, filepath.Join(t.TempDir(), "replay.db"))
	if err != nil {
		t.Fatal(err)
	}
	cas, err := store.NewLocalStore(t.TempDir(), bytes.Repeat([]byte{0x63}, 32))
	if err != nil {
		db.Close()
		t.Fatal(err)
	}

	plainID, err := cas.PutObject(store.ObjectBlob, []byte("plain"))
	if err != nil {
		t.Fatal(err)
	}
	nestedID, err := cas.PutObject(store.ObjectBlob, []byte("nested"))
	if err != nil {
		t.Fatal(err)
	}
	nestedTreeBytes, err := state.CanonicalBytes(state.NewTree([]state.Entry{{
		Name: []byte("note.txt"), Kind: state.EntryFile, Mode: 0o600, Size: 6, ObjectID: nestedID,
	}}))
	if err != nil {
		t.Fatal(err)
	}
	nestedTreeID, err := cas.PutObject(store.ObjectTree, nestedTreeBytes)
	if err != nil {
		t.Fatal(err)
	}

	chunkOne := []byte("chunk-one-")
	chunkTwo := []byte("chunk-two")
	chunkOneID, err := cas.PutObject(store.ObjectBlob, chunkOne)
	if err != nil {
		t.Fatal(err)
	}
	chunkTwoID, err := cas.PutObject(store.ObjectBlob, chunkTwo)
	if err != nil {
		t.Fatal(err)
	}
	chunked := append(append([]byte(nil), chunkOne...), chunkTwo...)
	chunkListBytes, err := state.EncodeChunkList(state.ChunkList{
		Size: int64(len(chunked)),
		Chunks: []state.ChunkRef{
			{ObjectID: chunkOneID, Size: uint32(len(chunkOne))},
			{ObjectID: chunkTwoID, Size: uint32(len(chunkTwo))},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	chunkListID, err := cas.PutObject(store.ObjectChunkList, chunkListBytes)
	if err != nil {
		t.Fatal(err)
	}

	entries := []state.Entry{
		{Name: []byte("chunked.bin"), Kind: state.EntryFile, Mode: 0o644, Size: int64(len(chunked)), ObjectID: chunkListID},
		{Name: []byte("nested"), Kind: state.EntryDir, Mode: 0o750, ObjectID: nestedTreeID},
		{Name: []byte("plain.txt"), Kind: state.EntryFile, Mode: 0o640, Size: 5, ObjectID: plainID},
	}
	if runtime.GOOS != "windows" {
		linkTarget := []byte("nested/note.txt")
		linkID, err := cas.PutObject(store.ObjectLink, linkTarget)
		if err != nil {
			t.Fatal(err)
		}
		entries = append(entries, state.Entry{
			Name: []byte("note-link"), Kind: state.EntrySymlink, Mode: 0o777,
			Size: int64(len(linkTarget)), ObjectID: linkID,
		})
	}
	rootBytes, err := state.CanonicalBytes(state.NewTree(entries))
	if err != nil {
		t.Fatal(err)
	}
	rootID, err := cas.PutObject(store.ObjectTree, rootBytes)
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
	now := time.Now().UTC()
	if err := db.CreateSession(ctx, persistence.SessionStart{
		ID: sessionID, Command: []string{"test"}, CWD: t.TempDir(), StartedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.PublishSnapshot(ctx, cas, persistence.PublishSnapshotRequest{
		StateID: stateID, SessionID: sessionID, RootTreeID: rootID, Role: persistence.SnapshotInitial,
		WallTimeUTC: now, MonotonicNS: 1, Source: "test",
	}); err != nil {
		t.Fatal(err)
	}
	return Dependencies{DB: db, CAS: cas}, stateID, chunked
}
