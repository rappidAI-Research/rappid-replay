// Package codex provides additive semantic enrichment for OpenAI Codex runs.
// The Generic Recorder remains authoritative for deterministic execution capture.
package codex

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/rappidAI-Research/rappid-replay/pkg/adapter"
)

const (
	ID      = "codex"
	Version = "1.0.0"
)

const defaultPollInterval = 150 * time.Millisecond

// Adapter observes Codex-owned local persistence. It never launches Codex,
// changes its arguments, or owns Replay persistence.
type Adapter struct {
	homeDir      func() (string, error)
	pollInterval time.Duration
}

func New() *Adapter {
	return &Adapter{homeDir: resolveCodexHome, pollInterval: defaultPollInterval}
}

func (*Adapter) Descriptor() adapter.Descriptor {
	return adapter.Descriptor{ID: ID, Version: Version}
}

func (*Adapter) Detect(_ context.Context, input adapter.DetectionInput) (adapter.Detection, error) {
	mode, ok := invocationMode(input.Command)
	if !ok {
		return adapter.Detection{}, nil
	}
	return adapter.Detection{
		Matched:    true,
		Confidence: 100,
		Reason:     "recognized Codex CLI invocation (" + mode + ")",
	}, nil
}

func (*Adapter) Capabilities() adapter.Capabilities {
	return adapter.Capabilities{
		Messages:   true,
		ToolCalls:  true,
		TokenUsage: true,
	}
}

func (*Adapter) EnrichProcess(_ context.Context, run adapter.RunContext, observation adapter.ProcessObservation) (adapter.ProcessEnrichment, error) {
	attributes := map[string]string{}
	if len(observation.Arguments) != 0 {
		if mode, ok := invocationMode(run.Command); ok {
			attributes["codex.role"] = "root"
			attributes["codex.mode"] = mode
		}
	} else if executableName(observation.Executable) == "codex" {
		attributes["codex.role"] = "codex-process"
	}
	if len(attributes) == 0 {
		return adapter.ProcessEnrichment{}, nil
	}
	return adapter.ProcessEnrichment{Attributes: attributes}, nil
}

func (a *Adapter) StreamEvents(ctx context.Context, run adapter.RunContext, emit adapter.EventEmitter) error {
	if a == nil {
		return nil
	}
	homeResolver := a.homeDir
	if homeResolver == nil {
		homeResolver = resolveCodexHome
	}
	home, err := homeResolver()
	if err != nil {
		return err
	}
	interval := a.pollInterval
	if interval <= 0 {
		interval = defaultPollInterval
	}
	return observeRollout(ctx, home, run, interval, emit)
}

func (*Adapter) Environment(_ context.Context, _ adapter.RunContext) (adapter.EnvironmentMetadata, error) {
	source := "default"
	if strings.TrimSpace(os.Getenv("CODEX_HOME")) != "" {
		source = "environment"
	}
	return adapter.EnvironmentMetadata{Attributes: map[string]string{
		"codex.integration": "local-rollout-observer",
		"codex.storage":     "state-db+rollout-jsonl",
		"codex.home_source": source,
	}}, nil
}

func (*Adapter) RedactionHints(_ context.Context, _ adapter.RunContext) ([]adapter.RedactionHint, error) {
	return []adapter.RedactionHint{
		{Kind: adapter.RedactEnvironmentName, Value: "OPENAI_API_KEY"},
		{Kind: adapter.RedactEnvironmentName, Value: "AZURE_OPENAI_API_KEY"},
		{Kind: adapter.RedactEnvironmentName, Value: "CODEX_API_KEY"},
	}, nil
}

func resolveCodexHome() (string, error) {
	if configured := strings.TrimSpace(os.Getenv("CODEX_HOME")); configured != "" {
		return filepath.Abs(configured)
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, ".codex"), nil
}

func invocationMode(command []string) (string, bool) {
	if len(command) == 0 {
		return "", false
	}

	index := -1
	switch executableName(command[0]) {
	case "codex":
		index = 0
	case "npx", "bunx":
		for i := 1; i < len(command); i++ {
			arg := strings.ToLower(strings.TrimSpace(command[i]))
			if strings.HasPrefix(arg, "-") {
				continue
			}
			if arg == "codex" || arg == "@openai/codex" {
				index = i
			}
			break
		}
	case "pnpm":
		for i := 1; i+1 < len(command); i++ {
			if strings.EqualFold(command[i], "dlx") {
				next := strings.ToLower(strings.TrimSpace(command[i+1]))
				if next == "codex" || next == "@openai/codex" {
					index = i + 1
				}
				break
			}
		}
	case "npm":
		for i := 1; i < len(command); i++ {
			if !strings.EqualFold(command[i], "exec") {
				continue
			}
			for j := i + 1; j < len(command); j++ {
				arg := strings.ToLower(strings.TrimSpace(command[j]))
				if arg == "--" || strings.HasPrefix(arg, "-") {
					continue
				}
				if arg == "codex" || arg == "@openai/codex" {
					index = j
				}
				break
			}
			break
		}
	}
	if index < 0 {
		return "", false
	}

	for _, raw := range command[index+1:] {
		arg := strings.ToLower(strings.TrimSpace(raw))
		if arg == "" || strings.HasPrefix(arg, "-") {
			continue
		}
		switch arg {
		case "exec", "resume", "fork", "app-server", "mcp-server":
			return arg, true
		default:
			return "interactive", true
		}
	}
	return "interactive", true
}

func executableName(value string) string {
	name := strings.ToLower(filepath.Base(strings.TrimSpace(value)))
	return strings.TrimSuffix(name, ".exe")
}
