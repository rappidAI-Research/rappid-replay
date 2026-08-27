// Package state defines canonical workspace-state structures.
package state

import "github.com/rappidAI-Research/rappid-replay/internal/store"

const TreeSchemaV1 = "rappid.replay.tree-object/1"

type EntryKind string

const (
	EntryFile    EntryKind = "file"
	EntryDir     EntryKind = "dir"
	EntrySymlink EntryKind = "symlink"
)

// Tree is the immutable logical content of one directory. Child directories are
// represented by their own tree objects, producing a recursive Merkle tree in
// the content-addressed store.
type Tree struct {
	Entries []Entry
}

// Entry stores a single path component as raw bytes so canonical state identity
// does not depend on UTF-8 validity. ObjectID references a typed CAS object:
// blob/chunk_list for files, tree for directories, and link for symlinks.
type Entry struct {
	Name     []byte
	Kind     EntryKind
	Mode     uint32
	Size     int64
	ObjectID store.ObjectID
}

func NewTree(entries []Entry) Tree {
	return Tree{Entries: append([]Entry(nil), entries...)}
}
