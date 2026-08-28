// Package app wires Replay's deterministic local subsystems together.
package app

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/rappidAI-Research/rappid-replay/internal/persistence"
	"github.com/rappidAI-Research/rappid-replay/internal/store"
)

// Layout is Replay's local runtime directory structure.
type Layout struct {
	Root      string
	Database  string
	Objects   string
	Artifacts string
	Models    string
	Temp      string
	Logs      string
}

// DefaultDataDir selects a per-user local data directory without placing Replay
// evidence inside the recorded project. An explicit CLI --data-dir remains the
// deterministic override for tests, portable installations, and operators.
func DefaultDataDir() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolve user home for Replay data: %w", err)
	}
	if home == "" {
		return "", fmt.Errorf("user home for Replay data is empty")
	}

	switch runtime.GOOS {
	case "windows":
		if local := os.Getenv("LOCALAPPDATA"); local != "" {
			return filepath.Join(local, "rappidAI", "Replay"), nil
		}
		return filepath.Join(home, "AppData", "Local", "rappidAI", "Replay"), nil
	case "darwin":
		return filepath.Join(home, "Library", "Application Support", "rappidAI", "Replay"), nil
	default:
		if xdg := os.Getenv("XDG_DATA_HOME"); xdg != "" {
			return filepath.Join(xdg, "rappidAI", "replay"), nil
		}
		return filepath.Join(home, ".local", "share", "rappidAI", "replay"), nil
	}
}

// ResolveLayout constructs the architecture-defined local storage paths.
func ResolveLayout(root string) (Layout, error) {
	if root == "" {
		var err error
		root, err = DefaultDataDir()
		if err != nil {
			return Layout{}, err
		}
	}
	abs, err := filepath.Abs(root)
	if err != nil {
		return Layout{}, fmt.Errorf("resolve Replay data directory: %w", err)
	}
	return Layout{
		Root:      abs,
		Database:  filepath.Join(abs, "replay.db"),
		Objects:   filepath.Join(abs, "objects"),
		Artifacts: filepath.Join(abs, "artifacts"),
		Models:    filepath.Join(abs, "models"),
		Temp:      filepath.Join(abs, "temp"),
		Logs:      filepath.Join(abs, "logs"),
	}, nil
}

// ValidateWorkspaceSeparation rejects a Replay data root that is the workspace
// itself or one of its descendants. Otherwise recording Replay's own SQLite/CAS
// mutations could recursively perturb the state being captured.
func ValidateWorkspaceSeparation(workspace, dataRoot string) error {
	if workspace == "" || dataRoot == "" {
		return fmt.Errorf("workspace and Replay data root are required")
	}
	workspacePath, err := canonicalExistingOrAbsolute(workspace)
	if err != nil {
		return fmt.Errorf("resolve workspace separation path: %w", err)
	}
	dataPath, err := canonicalExistingOrAbsolute(dataRoot)
	if err != nil {
		return fmt.Errorf("resolve Replay data separation path: %w", err)
	}
	rel, err := filepath.Rel(workspacePath, dataPath)
	if err != nil {
		return fmt.Errorf("compare workspace and Replay data paths: %w", err)
	}
	if rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(os.PathSeparator))) {
		return fmt.Errorf("Replay data directory %q must not be inside recorded workspace %q", dataPath, workspacePath)
	}
	return nil
}

func canonicalExistingOrAbsolute(name string) (string, error) {
	abs, err := filepath.Abs(name)
	if err != nil {
		return "", err
	}
	if resolved, err := filepath.EvalSymlinks(abs); err == nil {
		return filepath.Clean(resolved), nil
	} else if !os.IsNotExist(err) {
		return "", err
	}
	return filepath.Clean(abs), nil
}

// Runtime owns the durable local stores used by command handlers.
type Runtime struct {
	Layout Layout
	DB     *persistence.DB
	CAS    *store.LocalStore
}

// OpenRuntime opens SQLite and the encrypted CAS, creating private runtime
// directories as needed. No network access or AI subsystem is started here.
func OpenRuntime(ctx context.Context, dataDir string) (*Runtime, error) {
	layout, err := ResolveLayout(dataDir)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(layout.Root, 0o700); err != nil {
		return nil, fmt.Errorf("create Replay data directory: %w", err)
	}
	if err := os.Chmod(layout.Root, 0o700); err != nil {
		return nil, fmt.Errorf("set Replay data directory permissions: %w", err)
	}
	for _, dir := range []string{layout.Artifacts, layout.Models, layout.Temp, layout.Logs} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return nil, fmt.Errorf("create Replay runtime directory %q: %w", dir, err)
		}
		if err := os.Chmod(dir, 0o700); err != nil {
			return nil, fmt.Errorf("set Replay runtime directory permissions %q: %w", dir, err)
		}
	}

	db, err := persistence.Open(ctx, layout.Database)
	if err != nil {
		return nil, err
	}
	cas, err := store.OpenSystemLocalStore(ctx, layout.Objects)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	return &Runtime{Layout: layout, DB: db, CAS: cas}, nil
}

// Close releases runtime resources. Both close operations are attempted.
func (r *Runtime) Close() error {
	if r == nil {
		return nil
	}
	var first error
	if r.CAS != nil {
		if err := r.CAS.Close(); err != nil {
			first = err
		}
	}
	if r.DB != nil {
		if err := r.DB.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}
