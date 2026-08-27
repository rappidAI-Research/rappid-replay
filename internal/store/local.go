package store

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// LocalStore persists encrypted content-addressed objects below a local root.
// The directory layout shards BLAKE3 digests as b3/ab/cd/<remaining hex>.
type LocalStore struct {
	root  string
	codec *Codec
}

// NewLocalStore initializes an encrypted local object store. The supplied root
// should be Replay's objects/ directory, not the overall Replay data root.
func NewLocalStore(root string, key []byte) (*LocalStore, error) {
	if root == "" {
		return nil, fmt.Errorf("object store root is required")
	}
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("resolve object store root: %w", err)
	}
	if err := os.MkdirAll(filepath.Join(abs, "b3"), 0o700); err != nil {
		return nil, fmt.Errorf("create object store root: %w", err)
	}
	codec, err := NewCodec(key)
	if err != nil {
		return nil, err
	}
	return &LocalStore{root: abs, codec: codec}, nil
}

// Put stores plaintext if its content ID is not already present. An existing
// object is verified before it is reused; corruption is never silently
// overwritten with a replacement object.
func (s *LocalStore) Put(plaintext []byte) (ObjectID, error) {
	id, payload, err := s.codec.Seal(plaintext)
	if err != nil {
		return "", err
	}
	target, err := s.objectPath(id)
	if err != nil {
		return "", err
	}
	if _, err := os.Stat(target); err == nil {
		if _, err := s.Get(id); err != nil {
			return "", fmt.Errorf("existing object %s failed verification: %w", id, err)
		}
		return id, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("stat object %s: %w", id, err)
	}

	dir := filepath.Dir(target)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", fmt.Errorf("create object shard: %w", err)
	}
	tmp, err := os.CreateTemp(dir, ".rpo-staging-*")
	if err != nil {
		return "", fmt.Errorf("create object staging file: %w", err)
	}
	tmpName := tmp.Name()
	keepTemp := true
	defer func() {
		if keepTemp {
			_ = os.Remove(tmpName)
		}
	}()

	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return "", fmt.Errorf("set staging permissions: %w", err)
	}
	if _, err := tmp.Write(payload); err != nil {
		_ = tmp.Close()
		return "", fmt.Errorf("write object staging file: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return "", fmt.Errorf("sync object staging file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return "", fmt.Errorf("close object staging file: %w", err)
	}

	// Avoid replacing an object that appeared while we were writing staging
	// bytes. This also makes same-process dedup races cheap in the common case.
	if _, err := os.Stat(target); err == nil {
		if _, err := s.Get(id); err != nil {
			return "", fmt.Errorf("raced object %s failed verification: %w", id, err)
		}
		return id, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("recheck object %s: %w", id, err)
	}

	if err := os.Rename(tmpName, target); err != nil {
		// Windows does not replace an existing destination. If another writer won
		// the race, verify that object and treat it as successful deduplication.
		if _, statErr := os.Stat(target); statErr == nil {
			if _, verifyErr := s.Get(id); verifyErr != nil {
				return "", fmt.Errorf("concurrent object %s failed verification: %w", id, verifyErr)
			}
			return id, nil
		}
		return "", fmt.Errorf("commit object %s: %w", id, err)
	}
	keepTemp = false
	return id, nil
}

// Get returns verified canonical plaintext for id.
func (s *LocalStore) Get(id ObjectID) ([]byte, error) {
	path, err := s.objectPath(id)
	if err != nil {
		return nil, err
	}
	payload, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read object %s: %w", id, err)
	}
	plaintext, err := s.codec.Open(id, payload)
	if err != nil {
		return nil, fmt.Errorf("verify object %s: %w", id, err)
	}
	return plaintext, nil
}

// Verify checks that an object can be authenticated, decompressed, and hashed
// back to its requested content ID.
func (s *LocalStore) Verify(id ObjectID) error {
	_, err := s.Get(id)
	return err
}

func (s *LocalStore) objectPath(id ObjectID) (string, error) {
	parsed, err := ParseObjectID(id.String())
	if err != nil {
		return "", err
	}
	digest := strings.TrimPrefix(parsed.String(), objectIDPrefix)
	return filepath.Join(s.root, "b3", digest[:2], digest[2:4], digest[4:]), nil
}

func (s *LocalStore) Close() error { return s.codec.Close() }
