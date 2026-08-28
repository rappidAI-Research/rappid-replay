package app

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestValidateWorkspaceSeparationRejectsNestedDataDirectory(t *testing.T) {
	workspace := t.TempDir()
	dataRoot := filepath.Join(workspace, ".replay-data")
	if err := ValidateWorkspaceSeparation(workspace, dataRoot); err == nil {
		t.Fatal("ValidateWorkspaceSeparation() accepted data root inside workspace")
	}
}

func TestValidateWorkspaceSeparationRejectsNonexistentDataRootThroughSymlinkAncestor(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink creation may require elevated privileges on Windows")
	}
	parent := t.TempDir()
	workspace := filepath.Join(parent, "workspace")
	if err := os.MkdirAll(workspace, 0o700); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(parent, "workspace-alias")
	if err := os.Symlink(workspace, alias); err != nil {
		t.Fatal(err)
	}
	dataRoot := filepath.Join(alias, "not-created-yet", "replay-data")
	if err := ValidateWorkspaceSeparation(workspace, dataRoot); err == nil {
		t.Fatal("ValidateWorkspaceSeparation() accepted nonexistent data root through symlink ancestor")
	}
}

func TestValidateWorkspaceSeparationAcceptsSiblingDataDirectory(t *testing.T) {
	parent := t.TempDir()
	workspace := filepath.Join(parent, "workspace")
	dataRoot := filepath.Join(parent, "replay-data")
	if err := os.MkdirAll(workspace, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := ValidateWorkspaceSeparation(workspace, dataRoot); err != nil {
		t.Fatalf("ValidateWorkspaceSeparation() error = %v", err)
	}
}

func TestResolveLayoutUsesArchitectureStorageNames(t *testing.T) {
	layout, err := ResolveLayout(filepath.Join(t.TempDir(), "data"))
	if err != nil {
		t.Fatal(err)
	}
	if filepath.Base(layout.Database) != "replay.db" || filepath.Base(layout.Objects) != "objects" || filepath.Base(layout.Artifacts) != "artifacts" || filepath.Base(layout.Models) != "models" || filepath.Base(layout.Temp) != "temp" || filepath.Base(layout.Logs) != "logs" {
		t.Fatalf("unexpected layout: %+v", layout)
	}
}
