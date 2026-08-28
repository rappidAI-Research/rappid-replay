package store

import "fmt"

// ObjectMetadata is the verified metadata needed by Replay's durable object
// catalog. PlaintextSize is the size of canonical typed plaintext (the exact
// bytes hashed for ObjectID); StoredSize is the encrypted on-disk payload size.
type ObjectMetadata struct {
	ID            ObjectID
	Kind          ObjectKind
	PlaintextSize int64
	StoredSize    int64
}

// InspectObject authenticates and decodes an object before reporting metadata.
// StoredSize is obtained from a second bounded, identity-stable read rather than
// a path-only Stat, so a local replacement race cannot silently change catalog
// metadata after the object itself was verified.
func (s *LocalStore) InspectObject(id ObjectID) (ObjectMetadata, error) {
	framed, err := s.Get(id)
	if err != nil {
		return ObjectMetadata{}, err
	}
	obj, err := DecodeObject(framed)
	if err != nil {
		return ObjectMetadata{}, fmt.Errorf("decode object %s: %w", id, err)
	}
	objectPath, err := s.objectPath(id)
	if err != nil {
		return ObjectMetadata{}, err
	}
	storedPayload, err := readStableStoredFile(objectPath, maxStoredObjectBytes)
	if err != nil {
		return ObjectMetadata{}, fmt.Errorf("inspect stored object %s: %w", id, err)
	}
	return ObjectMetadata{
		ID:            id,
		Kind:          obj.Kind,
		PlaintextSize: int64(len(framed)),
		StoredSize:    int64(len(storedPayload)),
	}, nil
}
