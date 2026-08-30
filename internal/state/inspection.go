package state

import (
	"fmt"
	"sort"

	"github.com/rappidAI-Research/rappid-replay/internal/store"
)

// InspectableObjectStore extends the snapshot CAS boundary with verified object
// metadata used when a state is atomically published into SQLite.
type InspectableObjectStore interface {
	ObjectStore
	InspectObject(id store.ObjectID) (store.ObjectMetadata, error)
}

// Inspection is a verified snapshot graph plus deterministic metadata for every
// unique reachable CAS object. Objects is sorted by object ID. Verification
// counts are logical-path counts even when identical subtrees are deduplicated
// to the same CAS object.
type Inspection struct {
	Verification Verification
	Objects      []store.ObjectMetadata
}

// InspectSnapshot authenticates the complete reachable object graph and returns
// the metadata required for durable publication, including chunks referenced by
// large-file chunk lists.
func InspectSnapshot(cas InspectableObjectStore, root store.ObjectID) (Inspection, error) {
	if cas == nil {
		return Inspection{}, fmt.Errorf("snapshot CAS is required")
	}
	if _, err := store.ParseObjectID(root.String()); err != nil {
		return Inspection{}, fmt.Errorf("invalid root tree id: %w", err)
	}

	treeStats := make(map[store.ObjectID]Verification)
	objects := make(map[store.ObjectID]store.ObjectMetadata)
	verification, err := inspectTree(cas, root, treeStats, objects)
	if err != nil {
		return Inspection{}, err
	}

	metadata := make([]store.ObjectMetadata, 0, len(objects))
	for _, item := range objects {
		metadata = append(metadata, item)
	}
	sort.Slice(metadata, func(i, j int) bool {
		return metadata[i].ID.String() < metadata[j].ID.String()
	})
	return Inspection{Verification: verification, Objects: metadata}, nil
}

func inspectTree(
	cas InspectableObjectStore,
	id store.ObjectID,
	treeStats map[store.ObjectID]Verification,
	objects map[store.ObjectID]store.ObjectMetadata,
) (Verification, error) {
	if cached, ok := treeStats[id]; ok {
		return cached, nil
	}

	obj, err := cas.GetObject(id)
	if err != nil {
		return Verification{}, fmt.Errorf("load tree %s: %w", id, err)
	}
	if obj.Kind != store.ObjectTree {
		return Verification{}, fmt.Errorf("object %s kind = %q, want %q", id, obj.Kind, store.ObjectTree)
	}
	if err := addInspectedObject(cas, id, store.ObjectTree, objects); err != nil {
		return Verification{}, err
	}
	tree, err := ParseCanonicalTree(obj.Payload)
	if err != nil {
		return Verification{}, fmt.Errorf("parse tree %s: %w", id, err)
	}

	result := Verification{Trees: 1}
	for _, entry := range tree.Entries {
		switch entry.Kind {
		case EntryFile:
			if err := inspectFileObject(cas, entry.ObjectID, entry.Size, objects); err != nil {
				return Verification{}, err
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
			if err := addInspectedObject(cas, entry.ObjectID, store.ObjectLink, objects); err != nil {
				return Verification{}, err
			}
			result.Symlinks++

		case EntryDir:
			childResult, err := inspectTree(cas, entry.ObjectID, treeStats, objects)
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
	treeStats[id] = result
	return result, nil
}

func inspectFileObject(
	cas InspectableObjectStore,
	id store.ObjectID,
	declaredSize int64,
	objects map[store.ObjectID]store.ObjectMetadata,
) error {
	child, err := cas.GetObject(id)
	if err != nil {
		return fmt.Errorf("load file object %s: %w", id, err)
	}

	switch child.Kind {
	case store.ObjectBlob:
		if int64(len(child.Payload)) != declaredSize {
			return fmt.Errorf("file object %s size = %d, tree declares %d", id, len(child.Payload), declaredSize)
		}
		return addInspectedObject(cas, id, store.ObjectBlob, objects)

	case store.ObjectChunkList:
		list, err := DecodeChunkList(child.Payload)
		if err != nil {
			return fmt.Errorf("parse chunk-list object %s: %w", id, err)
		}
		if list.Size != declaredSize {
			return fmt.Errorf("chunk-list object %s size = %d, tree declares %d", id, list.Size, declaredSize)
		}
		if err := addInspectedObject(cas, id, store.ObjectChunkList, objects); err != nil {
			return err
		}
		for index, ref := range list.Chunks {
			chunk, err := cas.GetObject(ref.ObjectID)
			if err != nil {
				return fmt.Errorf("load chunk %d %s: %w", index, ref.ObjectID, err)
			}
			if chunk.Kind != store.ObjectBlob {
				return fmt.Errorf("chunk %d %s kind = %q, want %q", index, ref.ObjectID, chunk.Kind, store.ObjectBlob)
			}
			if len(chunk.Payload) != int(ref.Size) {
				return fmt.Errorf("chunk %d %s size = %d, list declares %d", index, ref.ObjectID, len(chunk.Payload), ref.Size)
			}
			if err := addInspectedObject(cas, ref.ObjectID, store.ObjectBlob, objects); err != nil {
				return err
			}
		}
		return nil

	default:
		return fmt.Errorf("file object %s kind = %q", id, child.Kind)
	}
}

func addInspectedObject(
	cas InspectableObjectStore,
	id store.ObjectID,
	expected store.ObjectKind,
	objects map[store.ObjectID]store.ObjectMetadata,
) error {
	if existing, ok := objects[id]; ok {
		if existing.Kind != expected {
			return fmt.Errorf("object %s reused with incompatible kinds %q and %q", id, existing.Kind, expected)
		}
		return nil
	}
	metadata, err := cas.InspectObject(id)
	if err != nil {
		return fmt.Errorf("inspect object %s: %w", id, err)
	}
	if metadata.Kind != expected {
		return fmt.Errorf("object %s metadata kind = %q, want %q", id, metadata.Kind, expected)
	}
	objects[id] = metadata
	return nil
}
