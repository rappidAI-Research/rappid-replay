package state

import (
	"bytes"
	"strings"
	"testing"

	"github.com/rappidAI-Research/rappid-replay/internal/store"
)

func TestValidateTreeForOSWindowsRejectsReinterpretedNames(t *testing.T) {
	id := store.SumObject([]byte("payload"))
	tests := []struct {
		name []byte
		want string
	}{
		{name: []byte(`a\b`), want: "reserved character"},
		{name: []byte("a:b"), want: "reserved character"},
		{name: []byte("NUL.txt"), want: "reserved device"},
		{name: []byte("name."), want: "must not end"},
		{name: []byte("name "), want: "must not end"},
		{name: []byte{'b', 'a', 'd', 0xff}, want: "valid UTF-8"},
	}

	for _, test := range tests {
		t.Run(string(test.name), func(t *testing.T) {
			tree := NewTree([]Entry{{Name: test.name, Kind: EntryFile, Size: 1, ObjectID: id}})
			err := ValidateTreeForOS(tree, "windows")
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("ValidateTreeForOS() error = %v, want substring %q", err, test.want)
			}
		})
	}
}

func TestValidateTreeForOSWindowsRejectsCaseCollision(t *testing.T) {
	first := store.SumObject([]byte("first"))
	second := store.SumObject([]byte("second"))
	tree := NewTree([]Entry{
		{Name: []byte("README"), Kind: EntryFile, Size: 1, ObjectID: first},
		{Name: []byte("readme"), Kind: EntryFile, Size: 1, ObjectID: second},
	})

	if err := ValidateTreeForOS(tree, "windows"); err == nil || !strings.Contains(err.Error(), "collision") {
		t.Fatalf("ValidateTreeForOS() error = %v, want collision", err)
	}
	if err := ValidateTreeForOS(tree, "linux"); err != nil {
		t.Fatalf("Linux portability unexpectedly failed: %v", err)
	}
}

func TestValidateTreeForOSLinuxPreservesBackslashAsFilenameByte(t *testing.T) {
	id := store.SumObject([]byte("payload"))
	tree := NewTree([]Entry{{Name: []byte(`a\b`), Kind: EntryFile, Size: 1, ObjectID: id}})
	if err := ValidateTreeForOS(tree, "linux"); err != nil {
		t.Fatalf("ValidateTreeForOS(linux) error = %v", err)
	}
}

func TestValidateSnapshotForOSWalksNestedTrees(t *testing.T) {
	cas, err := store.NewLocalStore(t.TempDir(), bytes.Repeat([]byte{0x71}, 32))
	if err != nil {
		t.Fatal(err)
	}
	defer cas.Close()

	fileID, err := cas.PutObject(store.ObjectBlob, []byte("x"))
	if err != nil {
		t.Fatal(err)
	}
	childBytes, err := CanonicalBytes(NewTree([]Entry{{Name: []byte("CON.txt"), Kind: EntryFile, Size: 1, ObjectID: fileID}}))
	if err != nil {
		t.Fatal(err)
	}
	childID, err := cas.PutObject(store.ObjectTree, childBytes)
	if err != nil {
		t.Fatal(err)
	}
	rootBytes, err := CanonicalBytes(NewTree([]Entry{{Name: []byte("nested"), Kind: EntryDir, ObjectID: childID}}))
	if err != nil {
		t.Fatal(err)
	}
	rootID, err := cas.PutObject(store.ObjectTree, rootBytes)
	if err != nil {
		t.Fatal(err)
	}

	if err := ValidateSnapshotForOS(cas, rootID, "windows"); err == nil || !strings.Contains(err.Error(), "nested") {
		t.Fatalf("ValidateSnapshotForOS(windows) error = %v, want nested portability failure", err)
	}
	if err := ValidateSnapshotForOS(cas, rootID, "linux"); err != nil {
		t.Fatalf("ValidateSnapshotForOS(linux) error = %v", err)
	}
}

func TestValidateTreeForOSRejectsUnknownTarget(t *testing.T) {
	if err := ValidateTreeForOS(Tree{}, "plan9"); err == nil {
		t.Fatal("ValidateTreeForOS() accepted unsupported target OS")
	}
}
