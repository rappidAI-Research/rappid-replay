package state

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/rappidAI-Research/rappid-replay/internal/store"
)

func TestInspectSnapshotReturnsEveryUniqueReachableObject(t *testing.T) {
	workspace := t.TempDir()
	if err := os.WriteFile(filepath.Join(workspace, "a.txt"), []byte("alpha"), 0o640); err != nil {
		t.Fatalf("write a.txt: %v", err)
	}
	if err := os.Mkdir(filepath.Join(workspace, "nested"), 0o750); err != nil {
		t.Fatalf("mkdir nested: %v", err)
	}
	if err := os.WriteFile(filepath.Join(workspace, "nested", "b.txt"), []byte("beta"), 0o600); err != nil {
		t.Fatalf("write b.txt: %v", err)
	}

	cas, err := store.NewLocalStore(t.TempDir(), bytes.Repeat([]byte{0x52}, 32))
	if err != nil {
		t.Fatalf("NewLocalStore() error = %v", err)
	}
	t.Cleanup(func() { _ = cas.Close() })

	snapshot, err := (Snapshotter{CAS: cas}).Capture(workspace)
	if err != nil {
		t.Fatalf("Capture() error = %v", err)
	}
	inspection, err := InspectSnapshot(cas, snapshot.RootTreeID)
	if err != nil {
		t.Fatalf("InspectSnapshot() error = %v", err)
	}
	if inspection.Verification.Trees != 2 {
		t.Fatalf("tree count = %d, want 2", inspection.Verification.Trees)
	}
	if inspection.Verification.Files != 2 {
		t.Fatalf("file count = %d, want 2", inspection.Verification.Files)
	}
	if len(inspection.Objects) != 4 {
		t.Fatalf("object count = %d, want 4", len(inspection.Objects))
	}
	for i, metadata := range inspection.Objects {
		if metadata.PlaintextSize <= 0 || metadata.StoredSize <= 0 {
			t.Fatalf("object %s has invalid sizes: %+v", metadata.ID, metadata)
		}
		if i > 0 && inspection.Objects[i-1].ID.String() >= metadata.ID.String() {
			t.Fatalf("objects are not strictly sorted by ID")
		}
	}
}

func TestInspectSnapshotCountsRepeatedSubtreeButCatalogsUniqueObjects(t *testing.T) {
	cas, err := store.NewLocalStore(t.TempDir(), bytes.Repeat([]byte{0x53}, 32))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cas.Close() })

	blobID, err := cas.PutObject(store.ObjectBlob, []byte("x"))
	if err != nil {
		t.Fatal(err)
	}
	childBytes, err := CanonicalBytes(NewTree([]Entry{{
		Name: []byte("same.txt"), Kind: EntryFile, Mode: 0o600, Size: 1, ObjectID: blobID,
	}}))
	if err != nil {
		t.Fatal(err)
	}
	childID, err := cas.PutObject(store.ObjectTree, childBytes)
	if err != nil {
		t.Fatal(err)
	}
	rootBytes, err := CanonicalBytes(NewTree([]Entry{
		{Name: []byte("a"), Kind: EntryDir, Mode: 0o700, ObjectID: childID},
		{Name: []byte("b"), Kind: EntryDir, Mode: 0o700, ObjectID: childID},
	}))
	if err != nil {
		t.Fatal(err)
	}
	rootID, err := cas.PutObject(store.ObjectTree, rootBytes)
	if err != nil {
		t.Fatal(err)
	}

	inspection, err := InspectSnapshot(cas, rootID)
	if err != nil {
		t.Fatal(err)
	}
	if inspection.Verification.Trees != 3 || inspection.Verification.Directories != 2 || inspection.Verification.Files != 2 || inspection.Verification.FileBytes != 2 {
		t.Fatalf("logical verification = %+v", inspection.Verification)
	}
	if len(inspection.Objects) != 3 {
		t.Fatalf("unique object count = %d, want 3", len(inspection.Objects))
	}
}
