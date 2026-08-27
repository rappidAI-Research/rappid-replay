package store

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"
)

var (
	// ErrCorruptObject marks a CAS object whose encrypted payload authenticated
	// or decoded incorrectly under a key that already passed the store key check.
	ErrCorruptObject = errors.New("corrupt Replay CAS object")
	// ErrStoreKeyCheck means Replay cannot prove that the supplied master key
	// belongs to this object store. The store is not opened and no objects are
	// mutated in this condition.
	ErrStoreKeyCheck = errors.New("Replay CAS master-key check failed")
	errMalformedStoredFile = errors.New("malformed Replay CAS storage file")
	errStoredFileChanged   = errors.New("Replay CAS storage file changed while reading")
)

const keyCheckFileName = ".key-check-v1"

var keyCheckPlaintext = []byte("rappidAI Replay local CAS key check v1")

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
	if err := os.MkdirAll(abs, 0o700); err != nil {
		return nil, fmt.Errorf("create object store root: %w", err)
	}
	if err := os.Chmod(abs, 0o700); err != nil {
		return nil, fmt.Errorf("set object store root permissions: %w", err)
	}
	if parent := filepath.Dir(abs); parent != abs {
		if err := syncDir(parent); err != nil {
			return nil, fmt.Errorf("persist object store root: %w", err)
		}
	}
	b3Root := filepath.Join(abs, "b3")
	if err := os.MkdirAll(b3Root, 0o700); err != nil {
		return nil, fmt.Errorf("create object store hash root: %w", err)
	}
	if err := os.Chmod(b3Root, 0o700); err != nil {
		return nil, fmt.Errorf("set object store hash root permissions: %w", err)
	}
	if err := syncDir(abs); err != nil {
		return nil, fmt.Errorf("persist object store hash root: %w", err)
	}

	codec, err := NewCodec(key)
	if err != nil {
		return nil, err
	}
	store := &LocalStore{root: abs, codec: codec}
	if err := store.verifyOrInitializeKeyCheck(); err != nil {
		_ = codec.Close()
		return nil, err
	}
	return store, nil
}

// Put stores plaintext if its content ID is not already present. An existing
// object is verified before it is reused; corruption is quarantined and remains
// blocked until a future explicit repair flow clears its quarantine marker.
func (s *LocalStore) Put(plaintext []byte) (ObjectID, error) {
	id, payload, err := s.codec.Seal(plaintext)
	if err != nil {
		return "", err
	}
	if blocked, err := s.isQuarantined(id); err != nil {
		return "", err
	} else if blocked {
		return "", fmt.Errorf("%w: object %s is quarantined and requires explicit repair", ErrCorruptObject, id)
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

	committed, err := s.commitPayloadNoReplace(target, payload)
	if err != nil {
		return "", fmt.Errorf("commit object %s: %w", id, err)
	}
	if committed {
		return id, nil
	}

	// Another writer won the immutable-object race. Verify its bytes before
	// treating the operation as successful deduplication.
	if _, err := s.Get(id); err != nil {
		return "", fmt.Errorf("concurrent object %s failed verification: %w", id, err)
	}
	return id, nil
}

// Get returns verified canonical plaintext for id. Authentication/hash failures
// are quarantined only after store-open key verification has succeeded, which
// avoids mistaking a wrong master key for per-object corruption.
func (s *LocalStore) Get(id ObjectID) ([]byte, error) {
	if blocked, err := s.isQuarantined(id); err != nil {
		return nil, err
	} else if blocked {
		return nil, fmt.Errorf("%w: object %s is quarantined", ErrCorruptObject, id)
	}

	path, err := s.objectPath(id)
	if err != nil {
		return nil, err
	}
	payload, err := readStableStoredFile(path, maxStoredObjectBytes)
	if err != nil {
		if errors.Is(err, errMalformedStoredFile) {
			quarantinePath, quarantineErr := s.quarantineCorruptObject(id, path)
			if quarantineErr != nil {
				return nil, fmt.Errorf("%w: object %s storage is malformed: %v; quarantine failed: %v", ErrCorruptObject, id, err, quarantineErr)
			}
			return nil, fmt.Errorf("%w: object %s malformed storage was quarantined at %s: %v", ErrCorruptObject, id, quarantinePath, err)
		}
		return nil, fmt.Errorf("read object %s: %w", id, err)
	}
	plaintext, err := s.codec.Open(id, payload)
	if err == nil {
		return plaintext, nil
	}

	quarantinePath, quarantineErr := s.quarantineCorruptObject(id, path)
	if quarantineErr != nil {
		return nil, fmt.Errorf("%w: object %s verification failed: %v; quarantine failed: %v", ErrCorruptObject, id, err, quarantineErr)
	}
	return nil, fmt.Errorf("%w: object %s verification failed and was quarantined at %s: %v", ErrCorruptObject, id, quarantinePath, err)
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

func (s *LocalStore) verifyOrInitializeKeyCheck() error {
	checkPath := filepath.Join(s.root, keyCheckFileName)
	if payload, err := readStableStoredFile(checkPath, maxStoredObjectBytes); err == nil {
		if err := s.verifyKeyCheckPayload(payload); err != nil {
			return err
		}
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("read CAS key check: %w", err)
	}

	// Stores created before the key-check file was introduced can be migrated
	// without rewriting objects. Prove the supplied key against one existing CAS
	// object first; never create a new key check over an unreadable legacy store.
	if sampleID, ok, err := s.firstObjectID(); err != nil {
		return err
	} else if ok {
		samplePath, err := s.objectPath(sampleID)
		if err != nil {
			return err
		}
		payload, err := readStableStoredFile(samplePath, maxStoredObjectBytes)
		if err != nil {
			return fmt.Errorf("read legacy CAS sample %s: %w", sampleID, err)
		}
		if _, err := s.codec.Open(sampleID, payload); err != nil {
			return fmt.Errorf("%w: existing CAS object %s is unreadable with supplied key: %v", ErrStoreKeyCheck, sampleID, err)
		}
	}

	expectedID := SumObject(keyCheckPlaintext)
	id, payload, err := s.codec.Seal(keyCheckPlaintext)
	if err != nil {
		return fmt.Errorf("encode CAS key check: %w", err)
	}
	if id != expectedID {
		return fmt.Errorf("encode CAS key check produced unexpected object id %s", id)
	}
	committed, err := s.commitPayloadNoReplace(checkPath, payload)
	if err != nil {
		return fmt.Errorf("persist CAS key check: %w", err)
	}
	if !committed {
		existing, err := readStableStoredFile(checkPath, maxStoredObjectBytes)
		if err != nil {
			return fmt.Errorf("read raced CAS key check: %w", err)
		}
		if err := s.verifyKeyCheckPayload(existing); err != nil {
			return err
		}
	}
	return nil
}

func (s *LocalStore) verifyKeyCheckPayload(payload []byte) error {
	expectedID := SumObject(keyCheckPlaintext)
	plaintext, err := s.codec.Open(expectedID, payload)
	if err != nil {
		return fmt.Errorf("%w: key-check payload could not be authenticated: %v", ErrStoreKeyCheck, err)
	}
	if !bytes.Equal(plaintext, keyCheckPlaintext) {
		return fmt.Errorf("%w: key-check plaintext mismatch", ErrStoreKeyCheck)
	}
	return nil
}

func (s *LocalStore) firstObjectID() (ObjectID, bool, error) {
	b3Root := filepath.Join(s.root, "b3")
	var found ObjectID
	errStop := errors.New("object found")
	err := filepath.WalkDir(b3Root, func(name string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(b3Root, name)
		if err != nil {
			return err
		}
		parts := strings.Split(filepath.ToSlash(rel), "/")
		if len(parts) != 3 || len(parts[0]) != 2 || len(parts[1]) != 2 || len(parts[2]) != 60 {
			return nil
		}
		id, err := ParseObjectID("b3:" + strings.Join(parts, ""))
		if err != nil {
			return nil
		}
		found = id
		return errStop
	})
	if errors.Is(err, errStop) {
		return found, true, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("scan existing CAS objects: %w", err)
	}
	return "", false, nil
}

func readStableStoredFile(name string, limit int64) ([]byte, error) {
	before, err := os.Lstat(name)
	if err != nil {
		return nil, err
	}
	if before.Mode()&os.ModeSymlink != 0 || !before.Mode().IsRegular() {
		return nil, fmt.Errorf("%w: %q is not a regular non-symlink file", errMalformedStoredFile, name)
	}
	if before.Size() < 0 || before.Size() > limit {
		return nil, fmt.Errorf("%w: %q size %d exceeds limit %d", errMalformedStoredFile, name, before.Size(), limit)
	}

	file, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	opened, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	if !opened.Mode().IsRegular() || !sameStoredFileInfo(before, opened) {
		_ = file.Close()
		return nil, fmt.Errorf("%w: %q was replaced while opening", errStoredFileChanged, name)
	}

	data, readErr := io.ReadAll(io.LimitReader(file, limit+1))
	afterHandle, statErr := file.Stat()
	closeErr := file.Close()
	if readErr != nil {
		return nil, readErr
	}
	if statErr != nil {
		return nil, statErr
	}
	if closeErr != nil {
		return nil, closeErr
	}
	if int64(len(data)) > limit {
		return nil, fmt.Errorf("%w: %q grew beyond limit %d", errMalformedStoredFile, name, limit)
	}
	if !sameStoredFileInfo(opened, afterHandle) || int64(len(data)) != afterHandle.Size() {
		return nil, fmt.Errorf("%w: %q changed while reading", errStoredFileChanged, name)
	}
	current, err := os.Lstat(name)
	if err != nil {
		return nil, err
	}
	if !sameStoredFileInfo(opened, current) {
		return nil, fmt.Errorf("%w: %q path was replaced while reading", errStoredFileChanged, name)
	}
	return data, nil
}

func sameStoredFileInfo(a, b fs.FileInfo) bool {
	if a == nil || b == nil || !os.SameFile(a, b) {
		return false
	}
	return a.Mode() == b.Mode() && a.Size() == b.Size() && a.ModTime().Equal(b.ModTime())
}

func (s *LocalStore) commitPayloadNoReplace(target string, payload []byte) (bool, error) {
	if len(payload) > maxStoredObjectBytes {
		return false, fmt.Errorf("stored payload is %d bytes, maximum is %d", len(payload), maxStoredObjectBytes)
	}
	dir := filepath.Dir(target)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return false, fmt.Errorf("create object directory: %w", err)
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		return false, fmt.Errorf("set object directory permissions: %w", err)
	}
	if err := s.syncDirectoryChain(dir); err != nil {
		return false, err
	}

	tmp, err := os.CreateTemp(dir, ".rpo-staging-*")
	if err != nil {
		return false, fmt.Errorf("create object staging file: %w", err)
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
		return false, fmt.Errorf("set staging permissions: %w", err)
	}
	written, err := io.Copy(tmp, bytes.NewReader(payload))
	if err != nil {
		_ = tmp.Close()
		return false, fmt.Errorf("write object staging file: %w", err)
	}
	if written != int64(len(payload)) {
		_ = tmp.Close()
		return false, fmt.Errorf("write object staging file: wrote %d bytes, want %d", written, len(payload))
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return false, fmt.Errorf("sync object staging file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return false, fmt.Errorf("close object staging file: %w", err)
	}

	// A hard-link commit is an atomic no-replace operation because staging and
	// destination live in the same directory/filesystem. Unlike os.Rename on
	// Unix, it can never overwrite an immutable CAS object that appeared during
	// the race window.
	if err := os.Link(tmpName, target); err != nil {
		if errors.Is(err, fs.ErrExist) {
			return false, nil
		}
		if _, statErr := os.Stat(target); statErr == nil {
			return false, nil
		}
		return false, fmt.Errorf("link staging file into place without replacement: %w", err)
	}
	if err := syncDir(dir); err != nil {
		return true, fmt.Errorf("persist committed object directory entry: %w", err)
	}
	if err := os.Remove(tmpName); err != nil {
		return true, fmt.Errorf("remove committed staging link: %w", err)
	}
	keepTemp = false
	if err := syncDir(dir); err != nil {
		return true, fmt.Errorf("persist staging cleanup: %w", err)
	}
	return true, nil
}

func (s *LocalStore) syncDirectoryChain(dir string) error {
	root := filepath.Clean(s.root)
	current := filepath.Clean(dir)
	for {
		if err := syncDir(current); err != nil {
			return fmt.Errorf("sync object directory chain: %w", err)
		}
		if current == root {
			return nil
		}
		parent := filepath.Dir(current)
		if parent == current || !strings.HasPrefix(current, root+string(os.PathSeparator)) {
			return fmt.Errorf("object directory %q escapes store root %q", dir, root)
		}
		current = parent
	}
}

func (s *LocalStore) quarantineBlockPath(id ObjectID) string {
	digest := strings.TrimPrefix(id.String(), objectIDPrefix)
	return filepath.Join(s.root, "quarantine", "blocked", digest+".blocked")
}

func (s *LocalStore) isQuarantined(id ObjectID) (bool, error) {
	marker := s.quarantineBlockPath(id)
	_, err := os.Stat(marker)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return false, fmt.Errorf("stat quarantine marker for %s: %w", id, err)
}

func (s *LocalStore) quarantineCorruptObject(id ObjectID, source string) (string, error) {
	quarantineDir := filepath.Join(s.root, "quarantine")
	blockedDir := filepath.Join(quarantineDir, "blocked")
	if err := os.MkdirAll(blockedDir, 0o700); err != nil {
		return "", fmt.Errorf("create quarantine directory: %w", err)
	}
	if err := os.Chmod(quarantineDir, 0o700); err != nil {
		return "", fmt.Errorf("set quarantine directory permissions: %w", err)
	}
	if err := os.Chmod(blockedDir, 0o700); err != nil {
		return "", fmt.Errorf("set quarantine marker directory permissions: %w", err)
	}
	if err := s.syncDirectoryChain(blockedDir); err != nil {
		return "", err
	}

	// Persist the block marker before moving the bad bytes. A crash after the
	// quarantine decision therefore cannot make a subsequent Put silently heal
	// the object and hide that a historical state experienced corruption.
	marker := s.quarantineBlockPath(id)
	if _, err := s.commitPayloadNoReplace(marker, []byte("quarantined\n")); err != nil {
		return "", fmt.Errorf("persist quarantine marker: %w", err)
	}

	digest := strings.TrimPrefix(id.String(), objectIDPrefix)
	target := filepath.Join(quarantineDir, fmt.Sprintf("%s-%d.rpo", digest, time.Now().UTC().UnixNano()))
	if err := os.Rename(source, target); err != nil {
		return "", fmt.Errorf("move corrupt object to quarantine: %w", err)
	}
	if err := syncDir(filepath.Dir(source)); err != nil {
		return "", fmt.Errorf("persist corrupt-object removal: %w", err)
	}
	if err := syncDir(quarantineDir); err != nil {
		return "", fmt.Errorf("persist quarantined object: %w", err)
	}
	return target, nil
}

func (s *LocalStore) Close() error { return s.codec.Close() }
