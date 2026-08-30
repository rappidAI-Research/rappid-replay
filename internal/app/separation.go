package app

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// ValidateMaterializationSeparation rejects any overlap between a restore
// destination and Replay's durable data root. A restore must never replace,
// contain, or be contained by the SQLite/CAS storage it is reading from.
func ValidateMaterializationSeparation(destination, dataRoot string) error {
	if destination == "" || dataRoot == "" {
		return fmt.Errorf("restore destination and Replay data root are required")
	}
	destinationPath, err := canonicalExistingOrAbsolute(destination)
	if err != nil {
		return fmt.Errorf("resolve restore destination separation path: %w", err)
	}
	dataPath, err := canonicalExistingOrAbsolute(dataRoot)
	if err != nil {
		return fmt.Errorf("resolve Replay data separation path: %w", err)
	}
	if pathContains(destinationPath, dataPath) || pathContains(dataPath, destinationPath) {
		return fmt.Errorf("restore destination %q must not overlap Replay data directory %q", destinationPath, dataPath)
	}
	return nil
}

func pathContains(parent, child string) bool {
	rel, err := filepath.Rel(parent, child)
	if err != nil {
		return false
	}
	return rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(os.PathSeparator)))
}
