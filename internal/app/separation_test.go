package app

import (
	"os"
	"path/filepath"
	"testing"
)

func TestValidateMaterializationSeparationRejectsOverlapBothWays(t *testing.T) {
	root := t.TempDir()
	data := filepath.Join(root, "data")
	if err := os.MkdirAll(data, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := ValidateMaterializationSeparation(root, data); err == nil {
		t.Fatal("expected destination containing data root to be rejected")
	}
	if err := ValidateMaterializationSeparation(filepath.Join(data, "restore"), data); err == nil {
		t.Fatal("expected destination inside data root to be rejected")
	}
}

func TestValidateMaterializationSeparationAllowsDisjointPaths(t *testing.T) {
	root := t.TempDir()
	data := filepath.Join(root, "data")
	destination := filepath.Join(root, "restore")
	if err := os.MkdirAll(data, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := ValidateMaterializationSeparation(destination, data); err != nil {
		t.Fatalf("ValidateMaterializationSeparation() error = %v", err)
	}
}
