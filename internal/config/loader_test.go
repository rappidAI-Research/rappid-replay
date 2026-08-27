package config

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestLoadAppliesNormativePrecedence(t *testing.T) {
	root := t.TempDir()
	userPath := filepath.Join(root, "user.yaml")
	projectPath := filepath.Join(root, "project.yaml")
	mustWriteConfig(t, userPath, `
record:
  ignore: ["user-cache/**"]
  terminal_input: full
sandbox:
  mode: host
  network: deny
intelligence:
  enabled: true
  profile: enhanced
  provider: local-endpoint
`)
	mustWriteConfig(t, projectPath, `
record:
  terminal_input: off
sandbox:
  mode: container
intelligence:
  enabled: false
`)

	terminal := "metadata-only"
	network := "ask"
	resolved, err := Load(LoadOptions{
		WorkingDir:        root,
		UserConfigPath:    userPath,
		ProjectConfigPath: projectPath,
		CLI: Overrides{
			Record:  RecordOverrides{TerminalInput: &terminal},
			Sandbox: SandboxOverrides{Network: &network},
		},
	})
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if !resolved.LoadedUser || !resolved.LoadedProject {
		t.Fatalf("loaded flags = user:%v project:%v, want both true", resolved.LoadedUser, resolved.LoadedProject)
	}
	if !reflect.DeepEqual(resolved.Config.Record.Ignore, []string{"user-cache/**"}) {
		t.Fatalf("record.ignore = %#v", resolved.Config.Record.Ignore)
	}
	if resolved.Config.Record.TerminalInput != "metadata-only" {
		t.Fatalf("terminal_input = %q, want CLI override", resolved.Config.Record.TerminalInput)
	}
	if resolved.Config.Sandbox.Mode != "container" {
		t.Fatalf("sandbox.mode = %q, want project override", resolved.Config.Sandbox.Mode)
	}
	if resolved.Config.Sandbox.Network != "ask" {
		t.Fatalf("sandbox.network = %q, want CLI override", resolved.Config.Sandbox.Network)
	}
	if resolved.Config.Intelligence.Enabled {
		t.Fatal("intelligence.enabled = true, want explicit project false to override user true")
	}
	if resolved.Config.Intelligence.Profile != "enhanced" || resolved.Config.Intelligence.Provider != "local-endpoint" {
		t.Fatalf("intelligence lower-precedence values were lost: %+v", resolved.Config.Intelligence)
	}
}

func TestLoadExplicitEmptyIgnoreReplacesDefaults(t *testing.T) {
	root := t.TempDir()
	userPath := filepath.Join(root, "user.yaml")
	mustWriteConfig(t, userPath, "record:\n  ignore: []\n")

	resolved, err := Load(LoadOptions{
		WorkingDir:        root,
		UserConfigPath:    userPath,
		ProjectConfigPath: filepath.Join(root, "missing-project.yaml"),
	})
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if resolved.Config.Record.Ignore == nil || len(resolved.Config.Record.Ignore) != 0 {
		t.Fatalf("record.ignore = %#v, want explicit empty non-nil list", resolved.Config.Record.Ignore)
	}
}

func TestLoadMissingFilesFallsBackToDefaults(t *testing.T) {
	root := t.TempDir()
	resolved, err := Load(LoadOptions{
		WorkingDir:        root,
		UserConfigPath:    filepath.Join(root, "missing-user.yaml"),
		ProjectConfigPath: filepath.Join(root, "missing-project.yaml"),
	})
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if resolved.LoadedUser || resolved.LoadedProject {
		t.Fatalf("loaded flags = user:%v project:%v, want both false", resolved.LoadedUser, resolved.LoadedProject)
	}
	if !reflect.DeepEqual(resolved.Config, Defaults()) {
		t.Fatalf("resolved config = %+v, want defaults %+v", resolved.Config, Defaults())
	}
}

func TestLoadRejectsUnknownFieldsAndMultipleDocuments(t *testing.T) {
	root := t.TempDir()
	tests := []struct {
		name    string
		content string
		want    string
	}{
		{name: "unknown field", content: "record:\n  unknown: true\n", want: "field unknown not found"},
		{name: "multiple documents", content: "record:\n  terminal_input: full\n---\nsandbox:\n  mode: host\n", want: "multiple YAML documents"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			configPath := filepath.Join(root, strings.ReplaceAll(test.name, " ", "-")+".yaml")
			mustWriteConfig(t, configPath, test.content)
			_, err := Load(LoadOptions{
				WorkingDir:        root,
				UserConfigPath:    configPath,
				ProjectConfigPath: filepath.Join(root, "missing-project.yaml"),
			})
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Load() error = %v, want substring %q", err, test.want)
			}
		})
	}
}

func TestLoadRejectsInvalidIgnorePattern(t *testing.T) {
	root := t.TempDir()
	userPath := filepath.Join(root, "user.yaml")
	mustWriteConfig(t, userPath, "record:\n  ignore: [\"../outside\"]\n")

	_, err := Load(LoadOptions{
		WorkingDir:        root,
		UserConfigPath:    userPath,
		ProjectConfigPath: filepath.Join(root, "missing-project.yaml"),
	})
	if err == nil || !strings.Contains(err.Error(), "record.ignore") {
		t.Fatalf("Load() error = %v, want invalid ignore error", err)
	}
}

func TestDefaultUserConfigPathUsesArchitectureLocation(t *testing.T) {
	path, err := DefaultUserConfigPath()
	if err != nil {
		t.Fatalf("DefaultUserConfigPath() error = %v", err)
	}
	wantSuffix := filepath.Join(".config", "rappidAI", "replay", "config.yaml")
	if !strings.HasSuffix(path, wantSuffix) {
		t.Fatalf("DefaultUserConfigPath() = %q, want suffix %q", path, wantSuffix)
	}
}

func mustWriteConfig(t *testing.T, name, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(name), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(name, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}
