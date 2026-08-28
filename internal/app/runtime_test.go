package app

import (
	"os"
	"path/filepath"
	"testing"
)

func TestValidateWorkspaceSeparationRejectsNestedDataDirectory(t *testing.T) {
	workspace := t.TempDir()
	dataRoot := filepath.Join(workspace, ".replay-data")
	if err := ValidateWorkspaceSeparation(workspace, dataRoot); err == nil {
		t.Fatal("ValidateWorkspaceSeparation() accepted data root inside workspace")
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
