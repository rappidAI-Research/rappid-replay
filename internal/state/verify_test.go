package state

import (
	"bytes"
	"testing"

	"github.com/rappidAI-Research/rappid-replay/internal/store"
)

func TestVerifySnapshotCountsRepeatedSubtreeAtEachLogicalPath(t *testing.T) {
	cas, err := store.NewLocalStore(t.TempDir(), bytes.Repeat([]byte{0x72}, 32))
	if err != nil {
		t.Fatal(err)
	}
	defer cas.Close()
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

	got, err := VerifySnapshot(cas, rootID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Trees != 3 || got.Directories != 2 || got.Files != 2 || got.FileBytes != 2 {
		t.Fatalf("VerifySnapshot() = %+v, want logical duplicate counts", got)
	}
}
