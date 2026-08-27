// Package state defines canonical workspace-state structures.
package state

const TreeSchemaV1 = "rappid.replay.state-tree/1"

type EntryKind string

const (
	EntryFile    EntryKind = "file"
	EntryDir     EntryKind = "dir"
	EntrySymlink EntryKind = "symlink"
)

// Tree is an immutable logical workspace tree. Canonical serialization and
// hashing are implemented separately so identity never depends on Go map order.
type Tree struct {
	Schema  string  `json:"schema"`
	Entries []Entry `json:"entries"`
}

// Entry contains platform-common metadata. Platform extensions are deliberately
// kept separate from the stable common fields.
type Entry struct {
	Path       string            `json:"path"`
	Kind       EntryKind         `json:"kind"`
	Mode       uint32            `json:"mode"`
	Size       int64             `json:"size,omitempty"`
	ObjectID   string            `json:"object_id,omitempty"`
	Target     string            `json:"target,omitempty"`
	Extensions map[string]string `json:"extensions,omitempty"`
}

func NewTree(entries []Entry) Tree {
	return Tree{Schema: TreeSchemaV1, Entries: entries}
}
