package replayformat

import (
	"fmt"

	"github.com/rappidAI-Research/rappid-replay/internal/state"
	"github.com/rappidAI-Research/rappid-replay/internal/store"
)

// ValidateObjectGraphs proves that every exported state is self-contained in the
// archive and that every reachable typed object has the kind and declared sizes
// required by the canonical tree/chunk-list model.
func ValidateObjectGraphs(bundle Bundle) error {
	objects := make(map[store.ObjectID]store.Object, len(bundle.Objects))
	for rawID, framed := range bundle.Objects {
		id, err := store.ParseObjectID(rawID)
		if err != nil {
			return fmt.Errorf("invalid object id %q: %w", rawID, err)
		}
		if store.SumObject(framed) != id {
			return fmt.Errorf("object %s content hash mismatch", id)
		}
		object, err := store.DecodeObject(framed)
		if err != nil {
			return fmt.Errorf("decode object %s: %w", id, err)
		}
		objects[id] = object
	}
	for _, session := range bundle.Sessions {
		for _, stateRecord := range session.States {
			root, err := store.ParseObjectID(stateRecord.RootTreeID)
			if err != nil {
				return fmt.Errorf("state %s root object id: %w", stateRecord.ID, err)
			}
			if err := validateTreeGraph(objects, root, make(map[store.ObjectID]bool)); err != nil {
				return fmt.Errorf("state %s: %w", stateRecord.ID, err)
			}
		}
	}
	return nil
}

func validateTreeGraph(objects map[store.ObjectID]store.Object, id store.ObjectID, active map[store.ObjectID]bool) error {
	if active[id] {
		return fmt.Errorf("object graph contains cycle at %s", id)
	}
	object, ok := objects[id]
	if !ok {
		return fmt.Errorf("archive omits reachable object %s", id)
	}
	if object.Kind != store.ObjectTree {
		return fmt.Errorf("object %s kind = %q, want %q", id, object.Kind, store.ObjectTree)
	}
	tree, err := state.ParseCanonicalTree(object.Payload)
	if err != nil {
		return fmt.Errorf("parse tree %s: %w", id, err)
	}
	active[id] = true
	defer delete(active, id)
	for _, entry := range tree.Entries {
		child, ok := objects[entry.ObjectID]
		if !ok {
			return fmt.Errorf("tree %s omits child object %s", id, entry.ObjectID)
		}
		switch entry.Kind {
		case state.EntryDir:
			if err := validateTreeGraph(objects, entry.ObjectID, active); err != nil {
				return err
			}
		case state.EntrySymlink:
			if child.Kind != store.ObjectLink || int64(len(child.Payload)) != entry.Size {
				return fmt.Errorf("link object %s does not match tree declaration", entry.ObjectID)
			}
		case state.EntryFile:
			if err := validateFileGraph(objects, entry.ObjectID, entry.Size); err != nil {
				return err
			}
		default:
			return fmt.Errorf("tree %s contains unsupported entry kind %q", id, entry.Kind)
		}
	}
	return nil
}

func validateFileGraph(objects map[store.ObjectID]store.Object, id store.ObjectID, declaredSize int64) error {
	object, ok := objects[id]
	if !ok {
		return fmt.Errorf("archive omits file object %s", id)
	}
	switch object.Kind {
	case store.ObjectBlob:
		if int64(len(object.Payload)) != declaredSize {
			return fmt.Errorf("blob %s size = %d, tree declares %d", id, len(object.Payload), declaredSize)
		}
		return nil
	case store.ObjectChunkList:
		list, err := state.DecodeChunkList(object.Payload)
		if err != nil {
			return fmt.Errorf("decode chunk list %s: %w", id, err)
		}
		if list.Size != declaredSize {
			return fmt.Errorf("chunk list %s size = %d, tree declares %d", id, list.Size, declaredSize)
		}
		for index, ref := range list.Chunks {
			chunk, ok := objects[ref.ObjectID]
			if !ok {
				return fmt.Errorf("chunk list %s omits chunk %d %s", id, index, ref.ObjectID)
			}
			if chunk.Kind != store.ObjectBlob || len(chunk.Payload) != int(ref.Size) {
				return fmt.Errorf("chunk %d %s does not match chunk-list declaration", index, ref.ObjectID)
			}
		}
		return nil
	default:
		return fmt.Errorf("file object %s kind = %q", id, object.Kind)
	}
}
