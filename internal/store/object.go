package store

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// ObjectKind is part of Replay's canonical object framing. Domain separation
// ensures identical payload bytes used as different object types receive
// different content IDs.
type ObjectKind string

const (
	ObjectBlob      ObjectKind = "blob"
	ObjectTree      ObjectKind = "tree"
	ObjectChunkList ObjectKind = "chunk_list"
	ObjectLink      ObjectKind = "link"
)

var objectFrameMagic = []byte{'R', 'P', 'O', 'B', 'J', 0, 1}

const objectFrameLengthSize = 8

// Object is a verified typed Replay object.
type Object struct {
	Kind    ObjectKind
	Payload []byte
}

func objectKindCode(kind ObjectKind) (byte, error) {
	switch kind {
	case ObjectBlob:
		return 1, nil
	case ObjectTree:
		return 2, nil
	case ObjectChunkList:
		return 3, nil
	case ObjectLink:
		return 4, nil
	default:
		return 0, fmt.Errorf("unknown object kind %q", kind)
	}
}

func objectKindFromCode(code byte) (ObjectKind, error) {
	switch code {
	case 1:
		return ObjectBlob, nil
	case 2:
		return ObjectTree, nil
	case 3:
		return ObjectChunkList, nil
	case 4:
		return ObjectLink, nil
	default:
		return "", fmt.Errorf("unknown object kind code %d", code)
	}
}

// EncodeObject returns canonical typed plaintext for the CAS. The object ID is
// computed over this framing, not over payload bytes alone.
func EncodeObject(kind ObjectKind, payload []byte) ([]byte, error) {
	code, err := objectKindCode(kind)
	if err != nil {
		return nil, err
	}

	headerLen := len(objectFrameMagic) + 1 + objectFrameLengthSize
	maxPayload := maxDecodedObjectBytes - headerLen
	if len(payload) > maxPayload {
		return nil, fmt.Errorf("object payload is %d bytes, maximum is %d", len(payload), maxPayload)
	}

	framed := make([]byte, headerLen+len(payload))
	copy(framed, objectFrameMagic)
	framed[len(objectFrameMagic)] = code
	binary.BigEndian.PutUint64(framed[len(objectFrameMagic)+1:headerLen], uint64(len(payload)))
	copy(framed[headerLen:], payload)
	return framed, nil
}

// DecodeObject validates canonical typed plaintext and returns an independent
// payload buffer.
func DecodeObject(framed []byte) (Object, error) {
	headerLen := len(objectFrameMagic) + 1 + objectFrameLengthSize
	if len(framed) < headerLen {
		return Object{}, fmt.Errorf("object frame is truncated")
	}
	if len(framed) > maxDecodedObjectBytes {
		return Object{}, fmt.Errorf("object frame exceeds %d-byte limit", maxDecodedObjectBytes)
	}
	if !bytes.Equal(framed[:len(objectFrameMagic)], objectFrameMagic) {
		return Object{}, fmt.Errorf("object frame magic/version mismatch")
	}
	kind, err := objectKindFromCode(framed[len(objectFrameMagic)])
	if err != nil {
		return Object{}, err
	}
	declared := binary.BigEndian.Uint64(framed[len(objectFrameMagic)+1 : headerLen])
	actual := uint64(len(framed) - headerLen)
	if declared != actual {
		return Object{}, fmt.Errorf("object payload length = %d, declared %d", actual, declared)
	}
	payload := append([]byte(nil), framed[headerLen:]...)
	return Object{Kind: kind, Payload: payload}, nil
}

// PutObject stores a typed canonical object.
func (s *LocalStore) PutObject(kind ObjectKind, payload []byte) (ObjectID, error) {
	framed, err := EncodeObject(kind, payload)
	if err != nil {
		return "", err
	}
	return s.Put(framed)
}

// GetObject authenticates, decompresses, hash-verifies, and decodes a typed
// object from the CAS.
func (s *LocalStore) GetObject(id ObjectID) (Object, error) {
	framed, err := s.Get(id)
	if err != nil {
		return Object{}, err
	}
	obj, err := DecodeObject(framed)
	if err != nil {
		return Object{}, fmt.Errorf("decode object %s: %w", id, err)
	}
	return obj, nil
}

// VerifyObject additionally checks the expected domain type.
func (s *LocalStore) VerifyObject(id ObjectID, expected ObjectKind) error {
	obj, err := s.GetObject(id)
	if err != nil {
		return err
	}
	if obj.Kind != expected {
		return fmt.Errorf("object %s kind = %q, want %q", id, obj.Kind, expected)
	}
	return nil
}
