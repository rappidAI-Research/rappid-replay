package store

import (
	"bytes"
	"testing"
)

func TestInspectObjectReportsVerifiedTypedMetadata(t *testing.T) {
	key := bytes.Repeat([]byte{0x41}, 32)
	cas, err := NewLocalStore(t.TempDir(), key)
	if err != nil {
		t.Fatalf("NewLocalStore() error = %v", err)
	}
	t.Cleanup(func() { _ = cas.Close() })

	payload := []byte("hello replay")
	objectID, err := cas.PutObject(ObjectBlob, payload)
	if err != nil {
		t.Fatalf("PutObject() error = %v", err)
	}
	metadata, err := cas.InspectObject(objectID)
	if err != nil {
		t.Fatalf("InspectObject() error = %v", err)
	}
	framed, err := EncodeObject(ObjectBlob, payload)
	if err != nil {
		t.Fatalf("EncodeObject() error = %v", err)
	}
	if metadata.ID != objectID {
		t.Fatalf("metadata ID = %q, want %q", metadata.ID, objectID)
	}
	if metadata.Kind != ObjectBlob {
		t.Fatalf("metadata kind = %q, want %q", metadata.Kind, ObjectBlob)
	}
	if metadata.PlaintextSize != int64(len(framed)) {
		t.Fatalf("plaintext size = %d, want %d", metadata.PlaintextSize, len(framed))
	}
	if metadata.StoredSize <= 0 {
		t.Fatalf("stored size = %d, want > 0", metadata.StoredSize)
	}
}
