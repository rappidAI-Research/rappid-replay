package state

import (
	"bytes"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/rappidAI-Research/rappid-replay/internal/store"
)

func TestSnapshotCaptureIsDeterministicAndVerifiable(t *testing.T) {
	workspace := t.TempDir()
	if err := os.Mkdir(filepath.Join(workspace, "nested"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workspace, "root.txt"), []byte("alpha"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workspace, "nested", "child.txt"), []byte("beta"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workspace, "empty"), nil, 0o644); err != nil {
		t.Fatal(err)
	}

	cas, err := store.NewLocalStore(t.TempDir(), bytes.Repeat([]byte{0x42}, 32))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cas.Close() })

	snapshotter := Snapshotter{CAS: cas}
	first, err := snapshotter.Capture(workspace)
	if err != nil {
		t.Fatal(err)
	}
	second, err := snapshotter.Capture(workspace)
	if err != nil {
		t.Fatal(err)
	}
	if first.RootTreeID != second.RootTreeID {
		t.Fatalf("unchanged workspace root IDs differ: %s != %s", first.RootTreeID, second.RootTreeID)
	}
	if first.Files != 3 || first.Directories != 1 || first.FileBytes != 9 {
		t.Fatalf("unexpected snapshot stats: %+v", first)
	}

	verified, err := VerifySnapshot(cas, first.RootTreeID)
	if err != nil {
		t.Fatal(err)
	}
	if verified.Files != first.Files || verified.Directories != first.Directories || verified.FileBytes != first.FileBytes {
		t.Fatalf("verification stats %+v do not match snapshot %+v", verified, first)
	}

	if err := os.WriteFile(filepath.Join(workspace, "root.txt"), []byte("changed"), 0o644); err != nil {
		t.Fatal(err)
	}
	third, err := snapshotter.Capture(workspace)
	if err != nil {
		t.Fatal(err)
	}
	if third.RootTreeID == first.RootTreeID {
		t.Fatal("workspace mutation did not change root tree ID")
	}
}

func TestSnapshotStoresSymlinkTargetWithoutFollowing(t *testing.T) {
	workspace := t.TempDir()
	if err := os.WriteFile(filepath.Join(workspace, "target.txt"), []byte("target contents"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("target.txt", filepath.Join(workspace, "link")); err != nil {
		if runtime.GOOS == "windows" {
			t.Skipf("symlink creation unavailable on Windows runner: %v", err)
		}
		t.Fatal(err)
	}

	cas, err := store.NewLocalStore(t.TempDir(), bytes.Repeat([]byte{0x24}, 32))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cas.Close() })

	snapshot, err := (Snapshotter{CAS: cas}).Capture(workspace)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Symlinks != 1 {
		t.Fatalf("symlinks = %d, want 1", snapshot.Symlinks)
	}
	if _, err := VerifySnapshot(cas, snapshot.RootTreeID); err != nil {
		t.Fatal(err)
	}

	rootObj, err := cas.GetObject(snapshot.RootTreeID)
	if err != nil {
		t.Fatal(err)
	}
	rootTree, err := ParseCanonicalTree(rootObj.Payload)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range rootTree.Entries {
		if string(entry.Name) != "link" {
			continue
		}
		linkObj, err := cas.GetObject(entry.ObjectID)
		if err != nil {
			t.Fatal(err)
		}
		if linkObj.Kind != store.ObjectLink || string(linkObj.Payload) != "target.txt" {
			t.Fatalf("unexpected link object: kind=%q payload=%q", linkObj.Kind, linkObj.Payload)
		}
		return
	}
	t.Fatal("link entry not found in root tree")
}
