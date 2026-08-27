package state

import (
	"fmt"
	"strings"
	"unicode/utf16"
	"unicode/utf8"

	"github.com/rappidAI-Research/rappid-replay/internal/store"
)

// ValidateSnapshotForOS verifies that every directory entry in a canonical
// snapshot can be represented without path reinterpretation on targetOS. It is
// deliberately read-only and does not weaken the canonical tree format: raw
// names remain the source of truth, while portability is a materialization
// precondition for cross-platform restore.
func ValidateSnapshotForOS(cas ObjectStore, root store.ObjectID, targetOS string) error {
	if cas == nil {
		return fmt.Errorf("snapshot CAS is required")
	}
	if _, err := store.ParseObjectID(root.String()); err != nil {
		return fmt.Errorf("invalid root tree id: %w", err)
	}
	if err := validateTargetOS(targetOS); err != nil {
		return err
	}
	visited := make(map[store.ObjectID]struct{})
	return validateTreeObjectForOS(cas, root, targetOS, "", visited)
}

func validateTreeObjectForOS(cas ObjectStore, id store.ObjectID, targetOS, relDir string, visited map[store.ObjectID]struct{}) error {
	if _, ok := visited[id]; ok {
		return nil
	}
	visited[id] = struct{}{}

	obj, err := cas.GetObject(id)
	if err != nil {
		return fmt.Errorf("load tree %s for portability validation: %w", id, err)
	}
	if obj.Kind != store.ObjectTree {
		return fmt.Errorf("object %s kind = %q, want %q", id, obj.Kind, store.ObjectTree)
	}
	tree, err := ParseCanonicalTree(obj.Payload)
	if err != nil {
		return fmt.Errorf("parse tree %s for portability validation: %w", id, err)
	}
	if err := ValidateTreeForOS(tree, targetOS); err != nil {
		if relDir == "" {
			return fmt.Errorf("root tree is not portable to %s: %w", targetOS, err)
		}
		return fmt.Errorf("directory %q is not portable to %s: %w", relDir, targetOS, err)
	}

	for _, entry := range tree.Entries {
		if entry.Kind != EntryDir {
			continue
		}
		name := string(entry.Name)
		childPath := name
		if relDir != "" {
			childPath = relDir + "/" + name
		}
		if err := validateTreeObjectForOS(cas, entry.ObjectID, targetOS, childPath, visited); err != nil {
			return err
		}
	}
	return nil
}

// ValidateTreeForOS validates one canonical directory against the filename and
// collision semantics needed for an exact materialization on targetOS. Linux
// and Darwin use Replay's canonical component rules directly. Windows adds the
// stricter Win32/NTFS constraints that would otherwise reinterpret or reject a
// valid Unix filename.
func ValidateTreeForOS(tree Tree, targetOS string) error {
	if err := validateTargetOS(targetOS); err != nil {
		return err
	}

	windowsNames := make(map[string][]byte)
	for index, entry := range tree.Entries {
		if err := validateEntry(entry); err != nil {
			return fmt.Errorf("entry %d: %w", index, err)
		}
		if targetOS != "windows" {
			continue
		}
		if err := validateWindowsComponent(entry.Name); err != nil {
			return fmt.Errorf("entry %q: %w", entry.Name, err)
		}

		// Default Windows filesystems are case-insensitive. Conservatively treat
		// Unicode simple-case equivalents as colliding so Replay never restores
		// two source entries into one destination path.
		folded := strings.ToUpper(string(entry.Name))
		if previous, ok := windowsNames[folded]; ok {
			return fmt.Errorf("Windows case-insensitive collision between %q and %q", previous, entry.Name)
		}
		windowsNames[folded] = append([]byte(nil), entry.Name...)
	}
	return nil
}

func validateTargetOS(targetOS string) error {
	switch targetOS {
	case "linux", "darwin", "windows":
		return nil
	default:
		return fmt.Errorf("unsupported target OS %q", targetOS)
	}
}

func validateWindowsComponent(nameBytes []byte) error {
	if !utf8.Valid(nameBytes) {
		return fmt.Errorf("Windows filenames must be valid UTF-8")
	}
	name := string(nameBytes)
	if name == "" {
		return fmt.Errorf("Windows filename is empty")
	}
	if strings.HasSuffix(name, " ") || strings.HasSuffix(name, ".") {
		return fmt.Errorf("Windows filename must not end with a space or period")
	}
	for _, r := range name {
		if r < 0x20 {
			return fmt.Errorf("Windows filename contains control character U+%04X", r)
		}
		switch r {
		case '<', '>', ':', '"', '/', '\\', '|', '?', '*':
			return fmt.Errorf("Windows filename contains reserved character %q", r)
		}
	}
	if len(utf16.Encode([]rune(name))) > 255 {
		return fmt.Errorf("Windows filename exceeds 255 UTF-16 code units")
	}

	base := name
	if dot := strings.IndexByte(base, '.'); dot >= 0 {
		base = base[:dot]
	}
	upper := strings.ToUpper(base)
	if isWindowsDeviceName(upper) {
		return fmt.Errorf("Windows filename uses reserved device name %q", base)
	}
	return nil
}

func isWindowsDeviceName(name string) bool {
	switch name {
	case "CON", "PRN", "AUX", "NUL", "CLOCK$":
		return true
	}
	if len(name) == 4 {
		prefix := name[:3]
		digit := name[3]
		if digit >= '1' && digit <= '9' && (prefix == "COM" || prefix == "LPT") {
			return true
		}
	}
	return false
}
