package config

import "testing"

func TestDefaultsPreserveArchitectureSafetyDefaults(t *testing.T) {
	cfg := Defaults()
	if cfg.Sandbox.Mode != "auto" {
		t.Fatalf("sandbox mode = %q, want auto", cfg.Sandbox.Mode)
	}
	if cfg.Sandbox.Network != "ask" {
		t.Fatalf("sandbox network = %q, want ask", cfg.Sandbox.Network)
	}
	if cfg.Privacy.ExportSecretScan != "block" {
		t.Fatalf("export secret scan = %q, want block", cfg.Privacy.ExportSecretScan)
	}
	if cfg.Intelligence.Enabled {
		t.Fatal("local intelligence must be disabled by default during foundations bootstrap")
	}
}
