package config

import (
	"strings"
	"testing"
)

func TestValidateRejectsUnknownSecuritySensitiveModes(t *testing.T) {
	tests := []struct {
		name   string
		field  string
		mutate func(*Config)
	}{
		{name: "terminal input", field: "record.terminal_input", mutate: func(cfg *Config) { cfg.Record.TerminalInput = "metadataonly" }},
		{name: "secret scan", field: "privacy.export_secret_scan", mutate: func(cfg *Config) { cfg.Privacy.ExportSecretScan = "blok" }},
		{name: "sandbox mode", field: "sandbox.mode", mutate: func(cfg *Config) { cfg.Sandbox.Mode = "native" }},
		{name: "sandbox network", field: "sandbox.network", mutate: func(cfg *Config) { cfg.Sandbox.Network = "prompt" }},
		{name: "intelligence profile", field: "intelligence.profile", mutate: func(cfg *Config) { cfg.Intelligence.Profile = "medium" }},
		{name: "intelligence provider", field: "intelligence.provider", mutate: func(cfg *Config) { cfg.Intelligence.Provider = "cloud" }},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := Defaults()
			test.mutate(&cfg)
			err := Validate(cfg)
			if err == nil || !strings.Contains(err.Error(), test.field) {
				t.Fatalf("Validate() error = %v, want field %q", err, test.field)
			}
		})
	}
}

func TestValidateAcceptsDefinedSecurityModes(t *testing.T) {
	terminalModes := []string{"metadata-only", "full", "off"}
	secretScanModes := []string{"block", "warn", "off"}
	sandboxModes := []string{"auto", "host", "container"}
	networkModes := []string{"ask", "deny", "allow"}
	profiles := []string{"lite", "standard", "enhanced"}
	providers := []string{"llamacpp", "ollama", "local-endpoint", "openai-compatible", "disabled"}

	for _, value := range terminalModes {
		cfg := Defaults()
		cfg.Record.TerminalInput = value
		if err := Validate(cfg); err != nil {
			t.Fatalf("record.terminal_input=%q rejected: %v", value, err)
		}
	}
	for _, value := range secretScanModes {
		cfg := Defaults()
		cfg.Privacy.ExportSecretScan = value
		if err := Validate(cfg); err != nil {
			t.Fatalf("privacy.export_secret_scan=%q rejected: %v", value, err)
		}
	}
	for _, value := range sandboxModes {
		cfg := Defaults()
		cfg.Sandbox.Mode = value
		if err := Validate(cfg); err != nil {
			t.Fatalf("sandbox.mode=%q rejected: %v", value, err)
		}
	}
	for _, value := range networkModes {
		cfg := Defaults()
		cfg.Sandbox.Network = value
		if err := Validate(cfg); err != nil {
			t.Fatalf("sandbox.network=%q rejected: %v", value, err)
		}
	}
	for _, value := range profiles {
		cfg := Defaults()
		cfg.Intelligence.Profile = value
		if err := Validate(cfg); err != nil {
			t.Fatalf("intelligence.profile=%q rejected: %v", value, err)
		}
	}
	for _, value := range providers {
		cfg := Defaults()
		cfg.Intelligence.Provider = value
		if err := Validate(cfg); err != nil {
			t.Fatalf("intelligence.provider=%q rejected: %v", value, err)
		}
	}
}
