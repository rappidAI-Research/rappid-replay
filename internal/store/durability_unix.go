//go:build !windows

package store

import (
	"fmt"
	"os"
)

// syncDir persists directory-entry changes on platforms where fsync on an open
// directory is supported. Callers already fsync file contents before invoking
// this helper.
func syncDir(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open directory %q for sync: %w", path, err)
	}
	defer dir.Close()
	if err := dir.Sync(); err != nil {
		return fmt.Errorf("sync directory %q: %w", path, err)
	}
	return nil
}
