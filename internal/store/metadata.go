package store

import (
	"fmt"
	"os"
)

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
// Metadata is therefore never derived from an unverified encrypted file alone.
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
	info, err := os.Stat(objectPath)
	if err != nil {
		return ObjectMetadata{}, fmt.Errorf("stat object %s: %w", id, err)
	}
	if !info.Mode().IsRegular() {
		return ObjectMetadata{}, fmt.Errorf("object %s is not a regular file", id)
	}
	return ObjectMetadata{
		ID:            id,
		Kind:          obj.Kind,
		PlaintextSize: int64(len(framed)),
		StoredSize:    info.Size(),
	}, nil
}
