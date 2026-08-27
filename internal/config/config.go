// Package config defines Replay's typed configuration model, defaults, and
// deterministic layer resolution.
package config

// Config is the fully resolved effective configuration. Layering follows the
// Architecture v1.0 precedence: CLI flags > project config > user config >
// defaults. Secrets are intentionally absent from this project-visible model.
type Config struct {
	Record       RecordConfig       `json:"record" yaml:"record"`
	Privacy      PrivacyConfig      `json:"privacy" yaml:"privacy"`
	Sandbox      SandboxConfig      `json:"sandbox" yaml:"sandbox"`
	Intelligence IntelligenceConfig `json:"intelligence" yaml:"intelligence"`
}

type RecordConfig struct {
	Ignore        []string `json:"ignore" yaml:"ignore"`
	TerminalInput string   `json:"terminal_input" yaml:"terminal_input"`
}

type PrivacyConfig struct {
	ExportSecretScan string `json:"export_secret_scan" yaml:"export_secret_scan"`
}

type SandboxConfig struct {
	Mode    string `json:"mode" yaml:"mode"`
	Network string `json:"network" yaml:"network"`
}

type IntelligenceConfig struct {
	Enabled  bool   `json:"enabled" yaml:"enabled"`
	Profile  string `json:"profile" yaml:"profile"`
	Provider string `json:"provider" yaml:"provider"`
}

func Defaults() Config {
	return Config{
		Record: RecordConfig{
			Ignore:        []string{"node_modules/**", ".venv/**", "target/**", "build/**", "dist/**"},
			TerminalInput: "metadata-only",
		},
		Privacy:      PrivacyConfig{ExportSecretScan: "block"},
		Sandbox:      SandboxConfig{Mode: "auto", Network: "ask"},
		Intelligence: IntelligenceConfig{Enabled: false, Profile: "standard", Provider: "llamacpp"},
	}
}
