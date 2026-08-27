package store

import (
	"bytes"
	"testing"
)

func TestTypedObjectFramingSeparatesDomains(t *testing.T) {
	payload := []byte("same bytes")
	blobFrame, err := EncodeObject(ObjectBlob, payload)
	if err != nil {
		t.Fatal(err)
	}
	treeFrame, err := EncodeObject(ObjectTree, payload)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(blobFrame, treeFrame) {
		t.Fatal("typed frames must differ across object kinds")
	}
	if SumObject(blobFrame) == SumObject(treeFrame) {
		t.Fatal("typed object IDs must differ across object kinds")
	}
}

func TestTypedObjectRoundTrip(t *testing.T) {
	payload := []byte{0, 1, 2, 3, 255}
	framed, err := EncodeObject(ObjectLink, payload)
	if err != nil {
		t.Fatal(err)
	}
	obj, err := DecodeObject(framed)
	if err != nil {
		t.Fatal(err)
	}
	if obj.Kind != ObjectLink {
		t.Fatalf("kind = %q, want %q", obj.Kind, ObjectLink)
	}
	if !bytes.Equal(obj.Payload, payload) {
		t.Fatalf("payload = %v, want %v", obj.Payload, payload)
	}
}

func TestTypedObjectRejectsLengthMismatch(t *testing.T) {
	framed, err := EncodeObject(ObjectBlob, []byte("payload"))
	if err != nil {
		t.Fatal(err)
	}
	framed = framed[:len(framed)-1]
	if _, err := DecodeObject(framed); err == nil {
		t.Fatal("DecodeObject accepted truncated payload")
	}
}
