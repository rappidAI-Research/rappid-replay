package state

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"sort"

	"github.com/rappidAI-Research/rappid-replay/internal/store"
)

type treeWire struct {
	Schema  string          `json:"schema"`
	Entries []treeEntryWire `json:"entries"`
}

type treeEntryWire struct {
	NameB64  string    `json:"name_b64"`
	Kind     EntryKind `json:"kind"`
	Mode     uint32    `json:"mode"`
	Size     int64     `json:"size,omitempty"`
	ObjectID string    `json:"object_id"`
}

// CanonicalBytes serializes a directory tree deterministically. Entries are
// ordered by raw filename bytes, filenames are base64 encoded without lossy
// Unicode normalization, and no map values participate in identity.
func CanonicalBytes(tree Tree) ([]byte, error) {
	entries := make([]Entry, len(tree.Entries))
	for i, entry := range tree.Entries {
		entries[i] = entry
		entries[i].Name = append([]byte(nil), entry.Name...)
	}
	sort.Slice(entries, func(i, j int) bool {
		return bytes.Compare(entries[i].Name, entries[j].Name) < 0
	})

	wire := treeWire{Schema: TreeSchemaV1, Entries: make([]treeEntryWire, 0, len(entries))}
	for i, entry := range entries {
		if err := validateEntry(entry); err != nil {
			return nil, fmt.Errorf("tree entry %d: %w", i, err)
		}
		if i > 0 && bytes.Equal(entries[i-1].Name, entry.Name) {
			return nil, fmt.Errorf("tree contains duplicate filename %q", entry.Name)
		}
		wire.Entries = append(wire.Entries, treeEntryWire{
			NameB64:  base64.StdEncoding.EncodeToString(entry.Name),
			Kind:     entry.Kind,
			Mode:     entry.Mode,
			Size:     entry.Size,
			ObjectID: entry.ObjectID.String(),
		})
	}

	encoded, err := json.Marshal(wire)
	if err != nil {
		return nil, fmt.Errorf("encode canonical tree: %w", err)
	}
	return encoded, nil
}

// ParseCanonicalTree accepts only the exact canonical representation emitted by
// CanonicalBytes. Semantically equivalent but reordered or whitespace-modified
// JSON is rejected so a tree object has one byte representation.
func ParseCanonicalTree(encoded []byte) (Tree, error) {
	dec := json.NewDecoder(bytes.NewReader(encoded))
	dec.DisallowUnknownFields()
	var wire treeWire
	if err := dec.Decode(&wire); err != nil {
		return Tree{}, fmt.Errorf("decode tree: %w", err)
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		if err == nil {
			return Tree{}, fmt.Errorf("decode tree: trailing JSON value")
		}
		return Tree{}, fmt.Errorf("decode tree trailing data: %w", err)
	}
	if wire.Schema != TreeSchemaV1 {
		return Tree{}, fmt.Errorf("tree schema = %q, want %q", wire.Schema, TreeSchemaV1)
	}

	entries := make([]Entry, 0, len(wire.Entries))
	for i, item := range wire.Entries {
		name, err := base64.StdEncoding.DecodeString(item.NameB64)
		if err != nil {
			return Tree{}, fmt.Errorf("tree entry %d name: %w", i, err)
		}
		objectID, err := store.ParseObjectID(item.ObjectID)
		if err != nil {
			return Tree{}, fmt.Errorf("tree entry %d object id: %w", i, err)
		}
		entry := Entry{
			Name:     name,
			Kind:     item.Kind,
			Mode:     item.Mode,
			Size:     item.Size,
			ObjectID: objectID,
		}
		if err := validateEntry(entry); err != nil {
			return Tree{}, fmt.Errorf("tree entry %d: %w", i, err)
		}
		entries = append(entries, entry)
	}

	tree := NewTree(entries)
	canonical, err := CanonicalBytes(tree)
	if err != nil {
		return Tree{}, err
	}
	if !bytes.Equal(encoded, canonical) {
		return Tree{}, fmt.Errorf("tree object is not canonically encoded")
	}
	return tree, nil
}

func validateEntry(entry Entry) error {
	if len(entry.Name) == 0 {
		return fmt.Errorf("filename is empty")
	}
	if bytes.Equal(entry.Name, []byte(".")) || bytes.Equal(entry.Name, []byte("..")) {
		return fmt.Errorf("filename %q is reserved", entry.Name)
	}
	if bytes.IndexByte(entry.Name, 0) >= 0 {
		return fmt.Errorf("filename contains NUL")
	}
	if bytes.IndexByte(entry.Name, '/') >= 0 {
		return fmt.Errorf("filename contains path separator")
	}
	if entry.Size < 0 {
		return fmt.Errorf("size must be non-negative")
	}
	if _, err := store.ParseObjectID(entry.ObjectID.String()); err != nil {
		return fmt.Errorf("invalid object id: %w", err)
	}
	switch entry.Kind {
	case EntryFile, EntrySymlink:
		return nil
	case EntryDir:
		if entry.Size != 0 {
			return fmt.Errorf("directory size must be zero")
		}
		return nil
	default:
		return fmt.Errorf("unknown entry kind %q", entry.Kind)
	}
}
