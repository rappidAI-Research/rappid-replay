package state

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"

	"github.com/rappidAI-Research/rappid-replay/internal/store"
)

// ErrWorkspaceChanged means a stable snapshot could not be proven because the
// workspace mutated while it was being read. Callers should retry the snapshot
// rather than publishing a potentially inconsistent state.
var ErrWorkspaceChanged = errors.New("workspace changed during snapshot")

// ObjectStore is the typed CAS surface required by snapshot and verification.
type ObjectStore interface {
	PutObject(kind store.ObjectKind, payload []byte) (store.ObjectID, error)
	GetObject(id store.ObjectID) (store.Object, error)
}

// ExcludeFunc returns true when relPath should not be included. relPath always
// uses '/' separators regardless of host OS.
type ExcludeFunc func(relPath string, entry fs.DirEntry) bool

// Snapshotter captures a workspace as a recursive Merkle tree in the CAS.
type Snapshotter struct {
	CAS     ObjectStore
	Exclude ExcludeFunc
}

// Snapshot identifies a fully persisted workspace state. RootTreeID is visible
// only after every reachable child object has been written successfully.
type Snapshot struct {
	RootTreeID store.ObjectID
	Files      int
	Directories int
	Symlinks   int
	FileBytes  int64
}

func (s Snapshotter) Capture(root string) (Snapshot, error) {
	if s.CAS == nil {
		return Snapshot{}, fmt.Errorf("snapshot CAS is required")
	}
	if root == "" {
		return Snapshot{}, fmt.Errorf("workspace root is required")
	}
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return Snapshot{}, fmt.Errorf("resolve workspace root: %w", err)
	}
	info, err := os.Lstat(absRoot)
	if err != nil {
		return Snapshot{}, fmt.Errorf("stat workspace root: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return Snapshot{}, fmt.Errorf("workspace root must be a directory, not a symlink or special file")
	}

	rootID, stats, err := s.captureDir(absRoot, "")
	if err != nil {
		return Snapshot{}, err
	}
	stats.RootTreeID = rootID
	return stats, nil
}

func (s Snapshotter) captureDir(absDir, relDir string) (store.ObjectID, Snapshot, error) {
	before, err := os.ReadDir(absDir)
	if err != nil {
		return "", Snapshot{}, fmt.Errorf("read directory %q: %w", relDir, err)
	}
	sortDirEntries(before)

	entries := make([]Entry, 0, len(before))
	stats := Snapshot{}
	for _, dirEntry := range before {
		name := dirEntry.Name()
		relPath := path.Join(relDir, name)
		if s.Exclude != nil && s.Exclude(relPath, dirEntry) {
			continue
		}

		fullPath := filepath.Join(absDir, name)
		info, err := os.Lstat(fullPath)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return "", Snapshot{}, fmt.Errorf("%w: %q disappeared", ErrWorkspaceChanged, relPath)
			}
			return "", Snapshot{}, fmt.Errorf("lstat %q: %w", relPath, err)
		}
		mode := uint32(info.Mode().Perm())

		switch {
		case info.Mode()&os.ModeSymlink != 0:
			target, err := readStableLink(fullPath)
			if err != nil {
				return "", Snapshot{}, fmt.Errorf("snapshot symlink %q: %w", relPath, err)
			}
			objectID, err := s.CAS.PutObject(store.ObjectLink, target)
			if err != nil {
				return "", Snapshot{}, fmt.Errorf("store symlink %q: %w", relPath, err)
			}
			entries = append(entries, Entry{Name: []byte(name), Kind: EntrySymlink, Mode: mode, Size: int64(len(target)), ObjectID: objectID})
			stats.Symlinks++

		case info.IsDir():
			childID, childStats, err := s.captureDir(fullPath, relPath)
			if err != nil {
				return "", Snapshot{}, err
			}
			entries = append(entries, Entry{Name: []byte(name), Kind: EntryDir, Mode: mode, ObjectID: childID})
			stats.Files += childStats.Files
			stats.Directories += childStats.Directories + 1
			stats.Symlinks += childStats.Symlinks
			stats.FileBytes += childStats.FileBytes

		case info.Mode().IsRegular():
			data, stableInfo, err := readStableFile(fullPath)
			if err != nil {
				return "", Snapshot{}, fmt.Errorf("snapshot file %q: %w", relPath, err)
			}
			objectID, err := s.CAS.PutObject(store.ObjectBlob, data)
			if err != nil {
				return "", Snapshot{}, fmt.Errorf("store file %q: %w", relPath, err)
			}
			entries = append(entries, Entry{Name: []byte(name), Kind: EntryFile, Mode: uint32(stableInfo.Mode().Perm()), Size: int64(len(data)), ObjectID: objectID})
			stats.Files++
			stats.FileBytes += int64(len(data))

		default:
			return "", Snapshot{}, fmt.Errorf("unsupported special file %q with mode %s", relPath, info.Mode())
		}
	}

	after, err := os.ReadDir(absDir)
	if err != nil {
		return "", Snapshot{}, fmt.Errorf("re-read directory %q: %w", relDir, err)
	}
	sortDirEntries(after)
	if !sameDirectoryListing(before, after) {
		return "", Snapshot{}, fmt.Errorf("%w: directory %q changed", ErrWorkspaceChanged, relDir)
	}

	canonical, err := CanonicalBytes(NewTree(entries))
	if err != nil {
		return "", Snapshot{}, fmt.Errorf("encode tree %q: %w", relDir, err)
	}
	treeID, err := s.CAS.PutObject(store.ObjectTree, canonical)
	if err != nil {
		return "", Snapshot{}, fmt.Errorf("store tree %q: %w", relDir, err)
	}
	return treeID, stats, nil
}

func readStableFile(name string) ([]byte, fs.FileInfo, error) {
	const attempts = 3
	for attempt := 0; attempt < attempts; attempt++ {
		file, err := os.Open(name)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil, nil, ErrWorkspaceChanged
			}
			return nil, nil, err
		}
		before, err := file.Stat()
		if err != nil {
			_ = file.Close()
			return nil, nil, err
		}
		if !before.Mode().IsRegular() {
			_ = file.Close()
			return nil, nil, ErrWorkspaceChanged
		}
		data, readErr := io.ReadAll(file)
		after, statErr := file.Stat()
		closeErr := file.Close()
		if readErr != nil {
			return nil, nil, readErr
		}
		if statErr != nil {
			return nil, nil, statErr
		}
		if closeErr != nil {
			return nil, nil, closeErr
		}
		if before.Size() == after.Size() && before.ModTime() == after.ModTime() && before.Mode() == after.Mode() && int64(len(data)) == after.Size() {
			return data, after, nil
		}
	}
	return nil, nil, ErrWorkspaceChanged
}

func readStableLink(name string) ([]byte, error) {
	before, err := os.Lstat(name)
	if err != nil {
		return nil, err
	}
	if before.Mode()&os.ModeSymlink == 0 {
		return nil, ErrWorkspaceChanged
	}
	target, err := os.Readlink(name)
	if err != nil {
		return nil, err
	}
	after, err := os.Lstat(name)
	if err != nil {
		return nil, err
	}
	if before.ModTime() != after.ModTime() || before.Mode() != after.Mode() {
		return nil, ErrWorkspaceChanged
	}
	return []byte(target), nil
}

func sortDirEntries(entries []fs.DirEntry) {
	sort.Slice(entries, func(i, j int) bool {
		return string([]byte(entries[i].Name())) < string([]byte(entries[j].Name()))
	})
}

func sameDirectoryListing(a, b []fs.DirEntry) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Name() != b[i].Name() || a[i].Type() != b[i].Type() {
			return false
		}
	}
	return true
}
