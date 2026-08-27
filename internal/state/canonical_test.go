package state

import (
	"bytes"
	"testing"

	"github.com/rappidAI-Research/rappid-replay/internal/store"
)

func TestCanonicalTreeIgnoresInputOrder(t *testing.T) {
	idA := store.SumObject([]byte("a"))
	idB := store.SumObject([]byte("b"))
	first := NewTree([]Entry{
		{Name: []byte("z.txt"), Kind: EntryFile, Mode: 0o644, Size: 1, ObjectID: idA},
		{Name: []byte("a.txt"), Kind: EntryFile, Mode: 0o644, Size: 1, ObjectID: idB},
	})
	second := NewTree([]Entry{
		{Name: []byte("a.txt"), Kind: EntryFile, Mode: 0o644, Size: 1, ObjectID: idB},
		{Name: []byte("z.txt"), Kind: EntryFile, Mode: 0o644, Size: 1, ObjectID: idA},
	})

	a, err := CanonicalBytes(first)
	if err != nil {
		t.Fatal(err)
	}
	b, err := CanonicalBytes(second)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(a, b) {
		t.Fatalf("canonical trees differ:\n%s\n%s", a, b)
	}
}

func TestCanonicalTreePreservesRawFilenameBytes(t *testing.T) {
	id := store.SumObject([]byte("payload"))
	name := []byte{0xff, 'x'}
	encoded, err := CanonicalBytes(NewTree([]Entry{{Name: name, Kind: EntryFile, Mode: 0o600, Size: 7, ObjectID: id}}))
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := ParseCanonicalTree(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if len(parsed.Entries) != 1 || !bytes.Equal(parsed.Entries[0].Name, name) {
		t.Fatalf("filename bytes were not preserved: %v", parsed.Entries)
	}
}

func TestParseCanonicalTreeRejectsNonCanonicalJSON(t *testing.T) {
	id := store.SumObject([]byte("payload"))
	encoded, err := CanonicalBytes(NewTree([]Entry{{Name: []byte("file"), Kind: EntryFile, Mode: 0o644, Size: 7, ObjectID: id}}))
	if err != nil {
		t.Fatal(err)
	}
	nonCanonical := append([]byte(" "), encoded...)
	if _, err := ParseCanonicalTree(nonCanonical); err == nil {
		t.Fatal("ParseCanonicalTree accepted non-canonical whitespace")
	}
}

func TestCanonicalTreeRejectsDuplicateNames(t *testing.T) {
	id := store.SumObject([]byte("payload"))
	tree := NewTree([]Entry{
		{Name: []byte("same"), Kind: EntryFile, Mode: 0o644, ObjectID: id},
		{Name: []byte("same"), Kind: EntryFile, Mode: 0o644, ObjectID: id},
	})
	if _, err := CanonicalBytes(tree); err == nil {
		t.Fatal("CanonicalBytes accepted duplicate names")
	}
}
