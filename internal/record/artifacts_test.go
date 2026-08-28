package record

import (
	"bytes"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/rappidAI-Research/rappid-replay/internal/persistence"
	"github.com/rappidAI-Research/rappid-replay/internal/state"
	"github.com/rappidAI-Research/rappid-replay/internal/store"
)

func TestDiscoverArtifactDeltaFindsCreatedModifiedAndReplacedFiles(t *testing.T) {
	cas, err := store.NewLocalStore(t.TempDir(), bytes.Repeat([]byte{0x7a}, 32))
	if err != nil {
		t.Fatal(err)
	}
	defer cas.Close()
	workspace := t.TempDir()

	write := func(name, content string) {
		t.Helper()
		path := filepath.Join(workspace, name)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(content), 0o640); err != nil {
			t.Fatal(err)
		}
	}
	write("unchanged.txt", "same")
	write("modified.txt", "before")
	write("removed.txt", "delete me")
	write(filepath.Join("replace", "child.txt"), "old tree")

	snapshotter := state.Snapshotter{CAS: cas}
	before, err := snapshotter.Capture(workspace)
	if err != nil {
		t.Fatal(err)
	}

	write("modified.txt", "after")
	if err := os.Remove(filepath.Join(workspace, "removed.txt")); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(filepath.Join(workspace, "replace")); err != nil {
		t.Fatal(err)
	}
	write("replace", "new file")
	write(filepath.Join("generated", "result.bin"), "artifact")
	write("z-last.txt", "last")

	after, err := snapshotter.Capture(workspace)
	if err != nil {
		t.Fatal(err)
	}
	artifacts, err := discoverArtifactDelta(cas, before.RootTreeID, after.RootTreeID)
	if err != nil {
		t.Fatalf("discoverArtifactDelta() error = %v", err)
	}

	wantPaths := []string{"generated/result.bin", "modified.txt", "replace", "z-last.txt"}
	wantKinds := []persistence.ArtifactChangeKind{
		persistence.ArtifactCreated,
		persistence.ArtifactModified,
		persistence.ArtifactReplaced,
		persistence.ArtifactCreated,
	}
	if len(artifacts) != len(wantPaths) {
		t.Fatalf("artifact count = %d, want %d: %+v", len(artifacts), len(wantPaths), artifacts)
	}
	for i, artifact := range artifacts {
		if string(artifact.Path) != wantPaths[i] || artifact.ChangeKind != wantKinds[i] {
			t.Fatalf("artifact[%d] = %q/%s, want %q/%s", i, artifact.Path, artifact.ChangeKind, wantPaths[i], wantKinds[i])
		}
		if artifact.ObjectID == "" || artifact.Size < 0 {
			t.Fatalf("artifact[%d] missing object metadata: %+v", i, artifact)
		}
	}
	if artifacts[0].PreviousObjectID != "" || artifacts[3].PreviousObjectID != "" {
		t.Fatal("created artifacts unexpectedly have previous object ids")
	}
	if artifacts[1].PreviousObjectID == "" || artifacts[2].PreviousObjectID == "" {
		t.Fatal("modified/replaced artifacts missing previous object ids")
	}
}

func TestDiscoverArtifactDeltaPreservesRawPathBytes(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("arbitrary non-UTF-8 filename bytes are exercised on Linux")
	}
	cas, err := store.NewLocalStore(t.TempDir(), bytes.Repeat([]byte{0x7b}, 32))
	if err != nil {
		t.Fatal(err)
	}
	defer cas.Close()
	workspace := t.TempDir()
	snapshotter := state.Snapshotter{CAS: cas}
	before, err := snapshotter.Capture(workspace)
	if err != nil {
		t.Fatal(err)
	}

	name := []byte{0xff, 'r', 'a', 'w', '.', 'b', 'i', 'n'}
	if err := os.WriteFile(filepath.Join(workspace, string(name)), []byte("raw"), 0o600); err != nil {
		t.Fatal(err)
	}
	after, err := snapshotter.Capture(workspace)
	if err != nil {
		t.Fatal(err)
	}
	artifacts, err := discoverArtifactDelta(cas, before.RootTreeID, after.RootTreeID)
	if err != nil {
		t.Fatal(err)
	}
	if len(artifacts) != 1 || !bytes.Equal(artifacts[0].Path, name) {
		t.Fatalf("raw artifact path = %v, want %v", artifacts, name)
	}
}
