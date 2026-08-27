package config

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/rappidAI-Research/rappid-replay/internal/ignore"
	"go.yaml.in/yaml/v3"
)

const maxConfigBytes = 1 << 20

// Overrides represents one sparse configuration layer. Pointer fields preserve
// the distinction between an omitted value and an explicit zero value such as
// intelligence.enabled: false or record.ignore: [].
type Overrides struct {
	Record       RecordOverrides       `yaml:"record"`
	Privacy      PrivacyOverrides      `yaml:"privacy"`
	Sandbox      SandboxOverrides      `yaml:"sandbox"`
	Intelligence IntelligenceOverrides `yaml:"intelligence"`
}

type RecordOverrides struct {
	Ignore        *[]string `yaml:"ignore"`
	TerminalInput *string   `yaml:"terminal_input"`
}

type PrivacyOverrides struct {
	ExportSecretScan *string `yaml:"export_secret_scan"`
}

type SandboxOverrides struct {
	Mode    *string `yaml:"mode"`
	Network *string `yaml:"network"`
}

type IntelligenceOverrides struct {
	Enabled  *bool   `yaml:"enabled"`
	Profile  *string `yaml:"profile"`
	Provider *string `yaml:"provider"`
}

// LoadOptions controls configuration discovery. Empty explicit paths select the
// architecture-defined defaults rather than disabling a layer.
type LoadOptions struct {
	WorkingDir        string
	UserConfigPath    string
	ProjectConfigPath string
	CLI               Overrides
}

// Resolution records the effective configuration and which filesystem layers
// participated. This is useful for future doctor/config diagnostics without
// making source discovery observable through the Config value itself.
type Resolution struct {
	Config            Config
	UserConfigPath    string
	ProjectConfigPath string
	LoadedUser        bool
	LoadedProject     bool
}

// Load resolves configuration with the normative precedence:
// CLI > project > user > defaults.
func Load(options LoadOptions) (Resolution, error) {
	workingDir := options.WorkingDir
	if workingDir == "" {
		cwd, err := os.Getwd()
		if err != nil {
			return Resolution{}, fmt.Errorf("resolve working directory: %w", err)
		}
		workingDir = cwd
	}
	absWorkingDir, err := filepath.Abs(workingDir)
	if err != nil {
		return Resolution{}, fmt.Errorf("resolve working directory %q: %w", workingDir, err)
	}

	userPath := options.UserConfigPath
	if userPath == "" {
		userPath, err = DefaultUserConfigPath()
		if err != nil {
			return Resolution{}, err
		}
	}
	projectPath := options.ProjectConfigPath
	if projectPath == "" {
		projectPath = filepath.Join(absWorkingDir, ".rappid", "replay.yaml")
	}

	result := Resolution{
		Config:            Defaults(),
		UserConfigPath:    userPath,
		ProjectConfigPath: projectPath,
	}

	userLayer, loaded, err := readOptionalLayer(userPath)
	if err != nil {
		return Resolution{}, fmt.Errorf("load user config %q: %w", userPath, err)
	}
	if loaded {
		apply(&result.Config, userLayer)
		result.LoadedUser = true
	}

	projectLayer, loaded, err := readOptionalLayer(projectPath)
	if err != nil {
		return Resolution{}, fmt.Errorf("load project config %q: %w", projectPath, err)
	}
	if loaded {
		apply(&result.Config, projectLayer)
		result.LoadedProject = true
	}

	apply(&result.Config, options.CLI)
	if err := Validate(result.Config); err != nil {
		return Resolution{}, fmt.Errorf("validate effective config: %w", err)
	}
	return result, nil
}

// DefaultUserConfigPath follows the Architecture v1.0 location exactly rather
// than delegating to OS-specific config-directory conventions.
func DefaultUserConfigPath() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolve user home directory: %w", err)
	}
	if home == "" {
		return "", fmt.Errorf("user home directory is empty")
	}
	return filepath.Join(home, ".config", "rappidAI", "replay", "config.yaml"), nil
}

// Validate checks invariants that must hold before configuration reaches the
// recorder, export, sandbox, or intelligence layers. Security-sensitive modes
// are strict enums: typos fail closed rather than silently becoming a new mode.
func Validate(cfg Config) error {
	if err := requireEnum("record.terminal_input", cfg.Record.TerminalInput, "metadata-only", "full", "off"); err != nil {
		return err
	}
	if err := requireEnum("privacy.export_secret_scan", cfg.Privacy.ExportSecretScan, "block", "warn", "off"); err != nil {
		return err
	}
	if err := requireEnum("sandbox.mode", cfg.Sandbox.Mode, "auto", "host", "container"); err != nil {
		return err
	}
	if err := requireEnum("sandbox.network", cfg.Sandbox.Network, "ask", "deny", "allow"); err != nil {
		return err
	}
	if err := requireEnum("intelligence.profile", cfg.Intelligence.Profile, "lite", "standard", "enhanced"); err != nil {
		return err
	}
	if err := requireEnum("intelligence.provider", cfg.Intelligence.Provider, "llamacpp", "ollama", "local-endpoint", "openai-compatible", "disabled"); err != nil {
		return err
	}
	if _, err := ignore.New(cfg.Record.Ignore); err != nil {
		return fmt.Errorf("record.ignore: %w", err)
	}
	return nil
}

func requireEnum(name, value string, allowed ...string) error {
	for _, candidate := range allowed {
		if value == candidate {
			return nil
		}
	}
	return fmt.Errorf("%s = %q, allowed values are %v", name, value, allowed)
}

func readOptionalLayer(name string) (Overrides, bool, error) {
	file, err := os.Open(name)
	if err != nil {
		if os.IsNotExist(err) {
			return Overrides{}, false, nil
		}
		return Overrides{}, false, err
	}
	defer file.Close()

	data, err := io.ReadAll(io.LimitReader(file, maxConfigBytes+1))
	if err != nil {
		return Overrides{}, false, fmt.Errorf("read: %w", err)
	}
	if len(data) > maxConfigBytes {
		return Overrides{}, false, fmt.Errorf("file exceeds %d-byte limit", maxConfigBytes)
	}

	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	var layer Overrides
	if err := decoder.Decode(&layer); err != nil {
		return Overrides{}, false, fmt.Errorf("parse strict YAML: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err != nil {
			return Overrides{}, false, fmt.Errorf("parse trailing YAML: %w", err)
		}
		return Overrides{}, false, fmt.Errorf("multiple YAML documents are not allowed")
	}
	return layer, true, nil
}

func apply(target *Config, layer Overrides) {
	if layer.Record.Ignore != nil {
		target.Record.Ignore = make([]string, len(*layer.Record.Ignore))
		copy(target.Record.Ignore, *layer.Record.Ignore)
	}
	if layer.Record.TerminalInput != nil {
		target.Record.TerminalInput = *layer.Record.TerminalInput
	}
	if layer.Privacy.ExportSecretScan != nil {
		target.Privacy.ExportSecretScan = *layer.Privacy.ExportSecretScan
	}
	if layer.Sandbox.Mode != nil {
		target.Sandbox.Mode = *layer.Sandbox.Mode
	}
	if layer.Sandbox.Network != nil {
		target.Sandbox.Network = *layer.Sandbox.Network
	}
	if layer.Intelligence.Enabled != nil {
		target.Intelligence.Enabled = *layer.Intelligence.Enabled
	}
	if layer.Intelligence.Profile != nil {
		target.Intelligence.Profile = *layer.Intelligence.Profile
	}
	if layer.Intelligence.Provider != nil {
		target.Intelligence.Provider = *layer.Intelligence.Provider
	}
}
