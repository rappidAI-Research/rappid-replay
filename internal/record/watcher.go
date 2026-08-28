package record

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/fsnotify/fsnotify"
	"github.com/rappidAI-Research/rappid-replay/internal/ignore"
)

const watcherEventBuffer = 256

// workspaceChangeSource is intentionally weaker than a filesystem journal. It
// only tells the recorder that a reconciliation scan should run. Snapshot
// capture remains the source of truth for state identity and contents.
type workspaceChangeSource interface {
	Changes() <-chan struct{}
	Errors() <-chan error
	Close() error
}

type workspaceWatcher struct {
	root    string
	policy  ignore.Policy
	watcher *fsnotify.Watcher

	changes chan struct{}
	errors  chan error
	done    chan struct{}

	closeOnce sync.Once
	wg        sync.WaitGroup
}

func newWorkspaceWatcher(root string, policy ignore.Policy) (*workspaceWatcher, error) {
	watcher, err := fsnotify.NewBufferedWatcher(watcherEventBuffer)
	if err != nil {
		return nil, fmt.Errorf("create filesystem watcher: %w", err)
	}
	item := &workspaceWatcher{
		root:    filepath.Clean(root),
		policy:  policy,
		watcher: watcher,
		changes: make(chan struct{}, 1),
		errors:  make(chan error, 8),
		done:    make(chan struct{}),
	}
	if err := item.addTree(item.root); err != nil {
		_ = watcher.Close()
		return nil, fmt.Errorf("watch workspace tree: %w", err)
	}
	item.wg.Add(1)
	go item.run()
	return item, nil
}

func (w *workspaceWatcher) Changes() <-chan struct{} { return w.changes }
func (w *workspaceWatcher) Errors() <-chan error     { return w.errors }

func (w *workspaceWatcher) Close() error {
	var closeErr error
	w.closeOnce.Do(func() {
		close(w.done)
		closeErr = w.watcher.Close()
		w.wg.Wait()
	})
	if errors.Is(closeErr, fsnotify.ErrClosed) {
		return nil
	}
	return closeErr
}

func (w *workspaceWatcher) run() {
	defer w.wg.Done()
	defer close(w.changes)
	defer close(w.errors)

	for {
		select {
		case <-w.done:
			return
		case event, ok := <-w.watcher.Events:
			if !ok {
				return
			}
			w.handleEvent(event)
		case err, ok := <-w.watcher.Errors:
			if !ok {
				return
			}
			if err != nil {
				w.signalError(err)
				// Queue overflow and backend errors are not state evidence, but a
				// full reconciliation after them can recover the current state.
				w.signalChange()
			}
		}
	}
}

func (w *workspaceWatcher) handleEvent(event fsnotify.Event) {
	if event.Name == "" {
		return
	}
	logical, inside := w.logicalPath(event.Name)
	if !inside {
		return
	}
	if logical != "." {
		info, statErr := os.Lstat(event.Name)
		if statErr == nil {
			if w.policy.Match(logical, info.IsDir()) {
				return
			}
			if event.Has(fsnotify.Create) && info.IsDir() {
				if err := w.addTree(event.Name); err != nil && !errors.Is(err, fs.ErrNotExist) {
					w.signalError(fmt.Errorf("watch created directory %q: %w", logical, err))
				}
			}
		} else if !errors.Is(statErr, fs.ErrNotExist) {
			w.signalError(fmt.Errorf("inspect changed path %q: %w", logical, statErr))
		} else if w.policy.Match(logical, false) {
			// Removed paths cannot be statted. File-pattern and reserved .git
			// exclusions are still deterministic here; a directory-only pattern
			// may cause one harmless extra reconciliation after removal.
			return
		}
	}
	w.signalChange()
}

func (w *workspaceWatcher) addTree(start string) error {
	return filepath.WalkDir(start, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			if errors.Is(walkErr, fs.ErrNotExist) {
				return nil
			}
			return walkErr
		}
		if !entry.IsDir() {
			return nil
		}
		logical, inside := w.logicalPath(path)
		if !inside {
			return fs.SkipDir
		}
		if logical != "." && w.policy.Match(logical, true) {
			return fs.SkipDir
		}
		if err := w.watcher.Add(path); err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}
		return nil
	})
}

func (w *workspaceWatcher) logicalPath(name string) (string, bool) {
	rel, err := filepath.Rel(w.root, filepath.Clean(name))
	if err != nil {
		return "", false
	}
	if rel == "." {
		return ".", true
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", false
	}
	return filepath.ToSlash(rel), true
}

func (w *workspaceWatcher) signalChange() {
	select {
	case w.changes <- struct{}{}:
	default:
	}
}

func (w *workspaceWatcher) signalError(err error) {
	select {
	case w.errors <- err:
	default:
		// The change channel still forces reconciliation. Dropping duplicate
		// diagnostic strings is preferable to blocking the OS event consumer.
	}
}
