package state

import (
	"fmt"

	"github.com/rappidAI-Research/rappid-replay/internal/store"
)

// Verification describes the logical workspace graph rooted at a snapshot tree
// after all referenced CAS objects have been authenticated.
type Verification struct {
	Trees       int
	Files       int
	Directories int
	Symlinks    int
	FileBytes   int64
}

// VerifySnapshot walks the Merkle tree, authenticates every CAS object through
// ObjectStore.GetObject, enforces object-domain types, and re-validates canonical
// tree and chunk-list serialization. It performs no filesystem writes.
//
// Object reads are memoized by content ID, while Verification counts remain
// logical-path counts. If the same subtree object is referenced at two directory
// paths, both paths therefore contribute to Files/Directories/FileBytes.
func VerifySnapshot(cas ObjectStore, root store.ObjectID) (Verification, error) {
	if cas == nil {
		return Verification{}, fmt.Errorf("snapshot CAS is required")
	}
	if _, err := store.ParseObjectID(root.String()); err != nil {
		return Verification{}, fmt.Errorf("invalid root tree id: %w", err)
	}
	verifier := snapshotVerifier{
		cas:       cas,
		objects:   make(map[store.ObjectID]store.Object),
		treeStats: make(map[store.ObjectID]Verification),
	}
	return verifier.verifyTree(root)
}

type snapshotVerifier struct {
	cas       ObjectStore
	objects   map[store.ObjectID]store.Object
	treeStats map[store.ObjectID]Verification
}

func (v *snapshotVerifier) getObject(id store.ObjectID) (store.Object, error) {
	if cached, ok := v.objects[id]; ok {
		return cached, nil
	}
	obj, err := v.cas.GetObject(id)
	if err != nil {
		return store.Object{}, err
	}
	v.objects[id] = obj
	return obj, nil
}

func (v *snapshotVerifier) verifyTree(id store.ObjectID) (Verification, error) {
	if cached, ok := v.treeStats[id]; ok {
		return cached, nil
	}
	obj, err := v.getObject(id)
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
			if err := v.verifyFileObject(entry.ObjectID, entry.Size); err != nil {
				return Verification{}, err
			}
			result.Files++
			result.FileBytes += entry.Size

		case EntrySymlink:
			child, err := v.getObject(entry.ObjectID)
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
			childResult, err := v.verifyTree(entry.ObjectID)
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
	v.treeStats[id] = result
	return result, nil
}

func (v *snapshotVerifier) verifyFileObject(id store.ObjectID, declaredSize int64) error {
	child, err := v.getObject(id)
	if err != nil {
		return fmt.Errorf("load file object %s: %w", id, err)
	}
	switch child.Kind {
	case store.ObjectBlob:
		if int64(len(child.Payload)) != declaredSize {
			return fmt.Errorf("file object %s size = %d, tree declares %d", id, len(child.Payload), declaredSize)
		}
		return nil

	case store.ObjectChunkList:
		list, err := DecodeChunkList(child.Payload)
		if err != nil {
			return fmt.Errorf("parse chunk-list object %s: %w", id, err)
		}
		if list.Size != declaredSize {
			return fmt.Errorf("chunk-list object %s size = %d, tree declares %d", id, list.Size, declaredSize)
		}
		for index, ref := range list.Chunks {
			chunk, err := v.getObject(ref.ObjectID)
			if err != nil {
				return fmt.Errorf("load chunk %d %s: %w", index, ref.ObjectID, err)
			}
			if chunk.Kind != store.ObjectBlob {
				return fmt.Errorf("chunk %d %s kind = %q, want %q", index, ref.ObjectID, chunk.Kind, store.ObjectBlob)
			}
			if len(chunk.Payload) != int(ref.Size) {
				return fmt.Errorf("chunk %d %s size = %d, list declares %d", index, ref.ObjectID, len(chunk.Payload), ref.Size)
			}
		}
		return nil

	default:
		return fmt.Errorf("file object %s kind = %q", id, child.Kind)
	}
}
