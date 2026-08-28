package state

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
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
	RootTreeID  store.ObjectID
	Files       int
	Directories int
	Symlinks    int
	FileBytes   int64
}

type directoryItem struct {
	name string
	info fs.FileInfo
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

	beforeRoot, err := os.Lstat(absRoot)
	if err != nil {
		return Snapshot{}, fmt.Errorf("stat workspace root: %w", err)
	}
	if beforeRoot.Mode()&os.ModeSymlink != 0 || !beforeRoot.IsDir() {
		return Snapshot{}, fmt.Errorf("workspace root must be a directory, not a symlink or special file")
	}

	// os.Root constrains every subsequent path resolution to the opened
	// workspace tree. This removes the path-based Lstat -> Open escape race: a
	// link introduced by the recorded process can never redirect a read outside
	// the workspace root. Identity checks below additionally reject in-root
	// symlink/replacement races rather than silently following them.
	rootHandle, err := os.OpenRoot(absRoot)
	if err != nil {
		return Snapshot{}, fmt.Errorf("open protected workspace root: %w", err)
	}
	defer rootHandle.Close()

	openedRoot, err := rootHandle.Stat(".")
	if err != nil {
		return Snapshot{}, fmt.Errorf("stat opened workspace root: %w", err)
	}
	if !sameStableInfo(beforeRoot, openedRoot) || !openedRoot.IsDir() {
		return Snapshot{}, fmt.Errorf("%w: workspace root was replaced while opening", ErrWorkspaceChanged)
	}

	rootID, stats, err := s.captureDir(rootHandle, "")
	if err != nil {
		return Snapshot{}, err
	}

	afterRoot, err := os.Lstat(absRoot)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return Snapshot{}, fmt.Errorf("%w: workspace root disappeared", ErrWorkspaceChanged)
		}
		return Snapshot{}, fmt.Errorf("re-stat workspace root: %w", err)
	}
	if !sameStableInfo(beforeRoot, afterRoot) {
		return Snapshot{}, fmt.Errorf("%w: workspace root changed while capturing", ErrWorkspaceChanged)
	}

	stats.RootTreeID = rootID
	return stats, nil
}

func (s Snapshotter) captureDir(root *os.Root, relDir string) (store.ObjectID, Snapshot, error) {
	before, err := s.readDirectoryItems(root, relDir)
	if err != nil {
		return "", Snapshot{}, err
	}

	entries := make([]Entry, 0, len(before))
	stats := Snapshot{}
	for _, item := range before {
		name := item.name
		relPath := path.Join(relDir, name)
		info := item.info
		mode := uint32(info.Mode().Perm())

		switch {
		case info.Mode()&os.ModeSymlink != 0:
			target, err := readStableLink(root, name, info)
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
			childRoot, err := root.OpenRoot(name)
			if err != nil {
				if errors.Is(err, os.ErrNotExist) {
					return "", Snapshot{}, fmt.Errorf("%w: directory %q disappeared", ErrWorkspaceChanged, relPath)
				}
				return "", Snapshot{}, fmt.Errorf("open directory %q: %w", relPath, err)
			}
			childOpened, err := childRoot.Stat(".")
			if err != nil {
				_ = childRoot.Close()
				return "", Snapshot{}, fmt.Errorf("stat opened directory %q: %w", relPath, err)
			}
			if !sameStableInfo(info, childOpened) || !childOpened.IsDir() {
				_ = childRoot.Close()
				return "", Snapshot{}, fmt.Errorf("%w: directory %q was replaced while opening", ErrWorkspaceChanged, relPath)
			}

			childID, childStats, captureErr := s.captureDir(childRoot, relPath)
			closeErr := childRoot.Close()
			if captureErr != nil {
				return "", Snapshot{}, captureErr
			}
			if closeErr != nil {
				return "", Snapshot{}, fmt.Errorf("close directory %q: %w", relPath, closeErr)
			}
			current, err := root.Lstat(name)
			if err != nil {
				if errors.Is(err, os.ErrNotExist) {
					return "", Snapshot{}, fmt.Errorf("%w: directory %q disappeared", ErrWorkspaceChanged, relPath)
				}
				return "", Snapshot{}, fmt.Errorf("re-stat directory %q: %w", relPath, err)
			}
			if !sameStableInfo(info, current) {
				return "", Snapshot{}, fmt.Errorf("%w: directory %q changed while capturing", ErrWorkspaceChanged, relPath)
			}

			entries = append(entries, Entry{Name: []byte(name), Kind: EntryDir, Mode: mode, ObjectID: childID})
			stats.Files += childStats.Files
			stats.Directories += childStats.Directories + 1
			stats.Symlinks += childStats.Symlinks
			stats.FileBytes += childStats.FileBytes

		case info.Mode().IsRegular():
			objectID, stableInfo, fileBytes, err := s.captureRegularFile(root, name, relPath, info)
			if err != nil {
				return "", Snapshot{}, err
			}
			entries = append(entries, Entry{
				Name:     []byte(name),
				Kind:     EntryFile,
				Mode:     uint32(stableInfo.Mode().Perm()),
				Size:     fileBytes,
				ObjectID: objectID,
			})
			stats.Files++
			stats.FileBytes += fileBytes

		default:
			return "", Snapshot{}, fmt.Errorf("unsupported special file %q with mode %s", relPath, info.Mode())
		}
	}

	after, err := s.readDirectoryItems(root, relDir)
	if err != nil {
		return "", Snapshot{}, err
	}
	if !sameDirectoryItems(before, after) {
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

func (s Snapshotter) readDirectoryItems(root *os.Root, relDir string) ([]directoryItem, error) {
	dirEntries, err := fs.ReadDir(root.FS(), ".")
	if err != nil {
		return nil, fmt.Errorf("read directory %q: %w", relDir, err)
	}
	sort.Slice(dirEntries, func(i, j int) bool {
		return bytes.Compare([]byte(dirEntries[i].Name()), []byte(dirEntries[j].Name())) < 0
	})

	items := make([]directoryItem, 0, len(dirEntries))
	for _, entry := range dirEntries {
		name := entry.Name()
		// .git is a reserved exclusion at the snapshot boundary itself. It is
		// never dependent on callers remembering to install an ignore policy.
		if name == ".git" {
			continue
		}
		relPath := path.Join(relDir, name)
		if s.Exclude != nil && s.Exclude(relPath, entry) {
			continue
		}
		info, err := root.Lstat(name)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil, fmt.Errorf("%w: %q disappeared", ErrWorkspaceChanged, relPath)
			}
			return nil, fmt.Errorf("lstat %q: %w", relPath, err)
		}
		items = append(items, directoryItem{name: name, info: info})
	}
	return items, nil
}

func (s Snapshotter) captureRegularFile(root *os.Root, name, relPath string, initial fs.FileInfo) (store.ObjectID, fs.FileInfo, int64, error) {
	file, err := root.Open(name)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", nil, 0, fmt.Errorf("%w: file %q disappeared", ErrWorkspaceChanged, relPath)
		}
		return "", nil, 0, fmt.Errorf("open file %q: %w", relPath, err)
	}

	opened, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return "", nil, 0, fmt.Errorf("stat opened file %q: %w", relPath, err)
	}
	if !opened.Mode().IsRegular() || !sameStableInfo(initial, opened) {
		_ = file.Close()
		return "", nil, 0, fmt.Errorf("%w: file %q was replaced while opening", ErrWorkspaceChanged, relPath)
	}

	objectID, readBytes, readErr := s.storeFileFromReader(file, opened.Size())
	afterHandle, statErr := file.Stat()
	closeErr := file.Close()
	if readErr != nil {
		return "", nil, 0, fmt.Errorf("snapshot file %q: %w", relPath, readErr)
	}
	if statErr != nil {
		return "", nil, 0, fmt.Errorf("re-stat open file %q: %w", relPath, statErr)
	}
	if closeErr != nil {
		return "", nil, 0, fmt.Errorf("close file %q: %w", relPath, closeErr)
	}
	if readBytes != opened.Size() || !sameStableInfo(opened, afterHandle) {
		return "", nil, 0, fmt.Errorf("%w: file %q changed while reading", ErrWorkspaceChanged, relPath)
	}

	current, err := root.Lstat(name)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", nil, 0, fmt.Errorf("%w: file %q disappeared", ErrWorkspaceChanged, relPath)
		}
		return "", nil, 0, fmt.Errorf("re-stat file %q: %w", relPath, err)
	}
	if !sameStableInfo(opened, current) {
		return "", nil, 0, fmt.Errorf("%w: file path %q was replaced while reading", ErrWorkspaceChanged, relPath)
	}
	return objectID, afterHandle, readBytes, nil
}

func (s Snapshotter) storeFileFromReader(r io.Reader, expectedSize int64) (store.ObjectID, int64, error) {
	if expectedSize < 0 {
		return "", 0, fmt.Errorf("file has negative size %d", expectedSize)
	}

	if expectedSize <= LargeFileThreshold {
		data, err := io.ReadAll(io.LimitReader(r, LargeFileThreshold+1))
		if err != nil {
			return "", 0, err
		}
		if int64(len(data)) != expectedSize {
			return "", int64(len(data)), ErrWorkspaceChanged
		}
		id, err := s.CAS.PutObject(store.ObjectBlob, data)
		return id, int64(len(data)), err
	}

	limit := expectedSize
	if expectedSize < math.MaxInt64 {
		limit++
	}
	refs := make([]ChunkRef, 0, int(expectedSize/ChunkTargetSize)+1)
	total, err := StreamContentDefinedChunks(io.LimitReader(r, limit), func(chunk []byte) error {
		id, err := s.CAS.PutObject(store.ObjectBlob, chunk)
		if err != nil {
			return err
		}
		refs = append(refs, ChunkRef{ObjectID: id, Size: uint32(len(chunk))})
		return nil
	})
	if err != nil {
		return "", total, err
	}
	if total != expectedSize {
		return "", total, ErrWorkspaceChanged
	}

	payload, err := EncodeChunkList(ChunkList{Size: expectedSize, Chunks: refs})
	if err != nil {
		return "", total, fmt.Errorf("encode chunk list: %w", err)
	}
	id, err := s.CAS.PutObject(store.ObjectChunkList, payload)
	return id, total, err
}

func readStableLink(root *os.Root, name string, before fs.FileInfo) ([]byte, error) {
	if before.Mode()&os.ModeSymlink == 0 {
		return nil, ErrWorkspaceChanged
	}
	target, err := root.Readlink(name)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, ErrWorkspaceChanged
		}
		return nil, err
	}
	after, err := root.Lstat(name)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, ErrWorkspaceChanged
		}
		return nil, err
	}
	if !sameStableInfo(before, after) || after.Mode()&os.ModeSymlink == 0 {
		return nil, ErrWorkspaceChanged
	}
	return []byte(target), nil
}

func sameStableInfo(a, b fs.FileInfo) bool {
	if a == nil || b == nil || !os.SameFile(a, b) {
		return false
	}
	return a.Mode() == b.Mode() && a.Size() == b.Size() && a.ModTime().Equal(b.ModTime())
}

func sameDirectoryItems(a, b []directoryItem) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].name != b[i].name || !sameStableInfo(a[i].info, b[i].info) {
			return false
		}
	}
	return true
}
