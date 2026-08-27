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
// unique reachable CAS object. Objects is sorted by object ID.
type Inspection struct {
	Verification Verification
	Objects      []store.ObjectMetadata
}

// InspectSnapshot authenticates the complete reachable object graph and returns
// the metadata required for durable publication. Chunk-list traversal is
// deliberately rejected until the large-file chunk format is implemented, so a
// state can never be published with untracked reachable chunks.
func InspectSnapshot(cas InspectableObjectStore, root store.ObjectID) (Inspection, error) {
	if cas == nil {
		return Inspection{}, fmt.Errorf("snapshot CAS is required")
	}
	if _, err := store.ParseObjectID(root.String()); err != nil {
		return Inspection{}, fmt.Errorf("invalid root tree id: %w", err)
	}

	visited := make(map[store.ObjectID]struct{})
	objects := make(map[store.ObjectID]store.ObjectMetadata)
	verification, err := inspectTree(cas, root, visited, objects)
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
	visited map[store.ObjectID]struct{},
	objects map[store.ObjectID]store.ObjectMetadata,
) (Verification, error) {
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
			child, err := cas.GetObject(entry.ObjectID)
			if err != nil {
				return Verification{}, fmt.Errorf("load file object %s: %w", entry.ObjectID, err)
			}
			switch child.Kind {
			case store.ObjectBlob:
				if int64(len(child.Payload)) != entry.Size {
					return Verification{}, fmt.Errorf("file object %s size = %d, tree declares %d", entry.ObjectID, len(child.Payload), entry.Size)
				}
			case store.ObjectChunkList:
				return Verification{}, fmt.Errorf("chunk-list object %s cannot be published before chunk traversal is implemented", entry.ObjectID)
			default:
				return Verification{}, fmt.Errorf("file object %s kind = %q", entry.ObjectID, child.Kind)
			}
			if err := addInspectedObject(cas, entry.ObjectID, child.Kind, objects); err != nil {
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
			childResult, err := inspectTree(cas, entry.ObjectID, visited, objects)
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
