package state

import (
	"fmt"

	"github.com/rappidAI-Research/rappid-replay/internal/store"
)

// Verification describes the reachable, authenticated object graph rooted at a
// snapshot tree.
type Verification struct {
	Trees       int
	Files       int
	Directories int
	Symlinks    int
	FileBytes   int64
}

// VerifySnapshot walks the Merkle tree, authenticates every CAS object through
// ObjectStore.GetObject, enforces object-domain types, and re-validates canonical
// tree serialization. It performs no filesystem writes.
func VerifySnapshot(cas ObjectStore, root store.ObjectID) (Verification, error) {
	if cas == nil {
		return Verification{}, fmt.Errorf("snapshot CAS is required")
	}
	if _, err := store.ParseObjectID(root.String()); err != nil {
		return Verification{}, fmt.Errorf("invalid root tree id: %w", err)
	}
	visited := make(map[store.ObjectID]struct{})
	return verifyTree(cas, root, visited)
}

func verifyTree(cas ObjectStore, id store.ObjectID, visited map[store.ObjectID]struct{}) (Verification, error) {
	if _, ok := visited[id]; ok {
		return Verification{}, nil
	}
	visited[id] = struct{}{}

	obj, err := cas.GetObject(id)
	if err != nil {
		return Verification{}, fmt.Errorf("load tree %s: %w", id, err)
	}
	if obj.Kind != store.ObjectTree {
		return Verification{}, fmt.Errorf("object %s kind = %q, want %q", id, obj.Kind, store.ObjectTree)
	}
	tree, err := ParseCanonicalTree(obj.Payload)
	if err != nil {
		return Verification{}, fmt.Errorf("parse tree %s: %w", id, err)
	}

	result := Verification{Trees: 1}
	for _, entry := range tree.Entries {
		switch entry.Kind {
		case EntryFile:
			child, err := cas.GetObject(entry.ObjectID)
			if err != nil {
				return Verification{}, fmt.Errorf("load file object %s: %w", entry.ObjectID, err)
			}
			if child.Kind != store.ObjectBlob && child.Kind != store.ObjectChunkList {
				return Verification{}, fmt.Errorf("file object %s kind = %q", entry.ObjectID, child.Kind)
			}
			if child.Kind == store.ObjectBlob && int64(len(child.Payload)) != entry.Size {
				return Verification{}, fmt.Errorf("file object %s size = %d, tree declares %d", entry.ObjectID, len(child.Payload), entry.Size)
			}
			result.Files++
			result.FileBytes += entry.Size

		case EntrySymlink:
			child, err := cas.GetObject(entry.ObjectID)
			if err != nil {
				return Verification{}, fmt.Errorf("load link object %s: %w", entry.ObjectID, err)
			}
			if child.Kind != store.ObjectLink {
				return Verification{}, fmt.Errorf("link object %s kind = %q, want %q", entry.ObjectID, child.Kind, store.ObjectLink)
			}
			if int64(len(child.Payload)) != entry.Size {
				return Verification{}, fmt.Errorf("link object %s size = %d, tree declares %d", entry.ObjectID, len(child.Payload), entry.Size)
			}
			result.Symlinks++

		case EntryDir:
			childResult, err := verifyTree(cas, entry.ObjectID, visited)
			if err != nil {
				return Verification{}, err
			}
			result.Trees += childResult.Trees
			result.Files += childResult.Files
			result.Directories += childResult.Directories + 1
			result.Symlinks += childResult.Symlinks
			result.FileBytes += childResult.FileBytes

		default:
			return Verification{}, fmt.Errorf("unsupported tree entry kind %q", entry.Kind)
		}
	}
	return result, nil
}
