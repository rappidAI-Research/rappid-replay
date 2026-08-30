package diff

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/rappidAI-Research/rappid-replay/internal/state"
	"github.com/rappidAI-Research/rappid-replay/internal/store"
)

func TestDiffTreesDetectsPathLevelChangesAndTruncatesDetails(t *testing.T) {
	cas, err := store.NewLocalStore(t.TempDir(), bytes.Repeat([]byte{0x71}, 32))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cas.Close() })

	leftDir := t.TempDir()
	rightDir := t.TempDir()
	writeTestFile(t, leftDir, "a.txt", "alpha")
	writeTestFile(t, leftDir, "old.txt", "old")
	writeTestFile(t, rightDir, "a.txt", "beta")
	writeTestFile(t, rightDir, "new.txt", "new")

	left, err := (state.Snapshotter{CAS: cas}).Capture(leftDir)
	if err != nil {
		t.Fatal(err)
	}
	right, err := (state.Snapshotter{CAS: cas}).Capture(rightDir)
	if err != nil {
		t.Fatal(err)
	}

	result, err := diffTrees(cas, left.RootTreeID, right.RootTreeID, 2)
	if err != nil {
		t.Fatalf("diffTrees() error = %v", err)
	}
	if result.Equal {
		t.Fatal("diffTrees() reported different trees equal")
	}
	if result.Added != 1 || result.Removed != 1 || result.Modified != 1 || result.TypeChanged != 0 {
		t.Fatalf("change counts = %+v", result)
	}
	if result.TotalChanges != 3 {
		t.Fatalf("TotalChanges = %d, want 3", result.TotalChanges)
	}
	if len(result.Changes) != 2 || !result.ChangesTruncated {
		t.Fatalf("retained changes = %d truncated=%t, want 2/true", len(result.Changes), result.ChangesTruncated)
	}
	for _, change := range result.Changes {
		if len(change.PathComponentsB64) != 1 || change.DisplayPath == "" {
			t.Fatalf("invalid path representation: %+v", change)
		}
	}
}

func TestDiffTreesPrunesEqualContentAddressedSubtrees(t *testing.T) {
	cas, err := store.NewLocalStore(t.TempDir(), bytes.Repeat([]byte{0x72}, 32))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cas.Close() })

	blobID, err := cas.PutObject(store.ObjectBlob, []byte("same"))
	if err != nil {
		t.Fatal(err)
	}
	childBytes, err := state.CanonicalBytes(state.NewTree([]state.Entry{{
		Name: []byte("same.txt"), Kind: state.EntryFile, Mode: 0o600, Size: 4, ObjectID: blobID,
	}}))
	if err != nil {
		t.Fatal(err)
	}
	childID, err := cas.PutObject(store.ObjectTree, childBytes)
	if err != nil {
		t.Fatal(err)
	}

	leftRootBytes, err := state.CanonicalBytes(state.NewTree([]state.Entry{{
		Name: []byte("nested"), Kind: state.EntryDir, Mode: 0o700, ObjectID: childID,
	}}))
	if err != nil {
		t.Fatal(err)
	}
	leftRoot, err := cas.PutObject(store.ObjectTree, leftRootBytes)
	if err != nil {
		t.Fatal(err)
	}
	rightRootBytes, err := state.CanonicalBytes(state.NewTree([]state.Entry{{
		Name: []byte("nested"), Kind: state.EntryDir, Mode: 0o755, ObjectID: childID,
	}}))
	if err != nil {
		t.Fatal(err)
	}
	rightRoot, err := cas.PutObject(store.ObjectTree, rightRootBytes)
	if err != nil {
		t.Fatal(err)
	}

	result, err := diffTrees(cas, leftRoot, rightRoot, 10)
	if err != nil {
		t.Fatal(err)
	}
	if result.Modified != 1 || result.TotalChanges != 1 || len(result.Changes) != 1 {
		t.Fatalf("result = %+v", result)
	}
	if result.Changes[0].DisplayPath != "nested" || result.Changes[0].Reason != "mode" {
		t.Fatalf("change = %+v", result.Changes[0])
	}
}

func writeTestFile(t *testing.T, root, name, content string) {
	t.Helper()
	path := filepath.Join(root, name)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
}
