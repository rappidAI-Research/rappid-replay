package store

import (
	"encoding/hex"
	"fmt"
	"strings"

	"lukechampine.com/blake3"
)

const objectIDPrefix = "b3:"

// ObjectID is the content identity of a canonical plaintext Replay object.
// Compression and encryption never participate in this identity.
type ObjectID string

// SumObject computes Replay's BLAKE3-256 content identifier.
func SumObject(plaintext []byte) ObjectID {
	sum := blake3.Sum256(plaintext)
	return ObjectID(objectIDPrefix + hex.EncodeToString(sum[:]))
}

// ParseObjectID validates the canonical lowercase b3:<64 hex> representation.
func ParseObjectID(s string) (ObjectID, error) {
	if !strings.HasPrefix(s, objectIDPrefix) {
		return "", fmt.Errorf("object id must start with %q", objectIDPrefix)
	}
	hexPart := strings.TrimPrefix(s, objectIDPrefix)
	if len(hexPart) != 64 {
		return "", fmt.Errorf("object id digest length = %d, want 64 hex characters", len(hexPart))
	}
	if hexPart != strings.ToLower(hexPart) {
		return "", fmt.Errorf("object id must use lowercase hex")
	}
	if _, err := hex.DecodeString(hexPart); err != nil {
		return "", fmt.Errorf("decode object id digest: %w", err)
	}
	return ObjectID(s), nil
}

func (id ObjectID) String() string { return string(id) }
