package codex

import (
	"context"
	"testing"

	"github.com/rappidAI-Research/rappid-replay/pkg/adapter"
)

func TestDetectRecognizesSupportedCodexInvocations(t *testing.T) {
	tests := []struct {
		name    string
		command []string
		mode    string
	}{
		{name: "direct", command: []string{"codex"}, mode: "interactive"},
		{name: "direct exe", command: []string{"C:/Tools/codex.exe", "exec", "task"}, mode: "exec"},
		{name: "npx", command: []string{"npx", "-y", "@openai/codex", "resume"}, mode: "resume"},
		{name: "pnpm", command: []string{"pnpm", "dlx", "@openai/codex", "fork"}, mode: "fork"},
		{name: "bunx", command: []string{"bunx", "codex", "app-server"}, mode: "app-server"},
		{name: "npm exec", command: []string{"npm", "exec", "--", "@openai/codex", "mcp-server"}, mode: "mcp-server"},
	}

	instance := New()
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			detection, err := instance.Detect(context.Background(), adapter.DetectionInput{Command: test.command})
			if err != nil {
				t.Fatal(err)
			}
			if !detection.Matched || detection.Confidence != 100 {
				t.Fatalf("detection = %+v", detection)
			}
			mode, ok := invocationMode(test.command)
			if !ok || mode != test.mode {
				t.Fatalf("invocationMode() = %q, %v; want %q, true", mode, ok, test.mode)
			}
		})
	}
}

func TestDetectRejectsLookalikes(t *testing.T) {
	for _, command := range [][]string{
		{"my-codex-wrapper"},
		{"npx", "some-codex-package"},
		{"node", "codex"},
		{"npm", "install", "@openai/codex"},
	} {
		detection, err := New().Detect(context.Background(), adapter.DetectionInput{Command: command})
		if err != nil {
			t.Fatal(err)
		}
		if detection.Matched {
			t.Fatalf("unexpected match for %q: %+v", command, detection)
		}
	}
}

func TestProcessEnrichmentDoesNotMarkArbitraryChildren(t *testing.T) {
	instance := New()
	run := adapter.RunContext{Command: []string{"codex", "exec"}}

	root, err := instance.EnrichProcess(context.Background(), run, adapter.ProcessObservation{
		PID: 10, Executable: "codex", Arguments: []string{"codex", "exec"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if root.Attributes["codex.role"] != "root" || root.Attributes["codex.mode"] != "exec" {
		t.Fatalf("root enrichment = %+v", root)
	}

	child, err := instance.EnrichProcess(context.Background(), run, adapter.ProcessObservation{
		PID: 11, ParentPID: 10, Executable: "bash",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(child.Attributes) != 0 {
		t.Fatalf("arbitrary child enrichment = %+v", child)
	}

	codexChild, err := instance.EnrichProcess(context.Background(), run, adapter.ProcessObservation{
		PID: 12, ParentPID: 10, Executable: "/usr/local/bin/codex",
	})
	if err != nil {
		t.Fatal(err)
	}
	if codexChild.Attributes["codex.role"] != "codex-process" {
		t.Fatalf("Codex child enrichment = %+v", codexChild)
	}
}

func TestCapabilitiesAndRedactionHintsAreConservative(t *testing.T) {
	instance := New()
	capabilities := instance.Capabilities()
	if !capabilities.Messages || !capabilities.ToolCalls || !capabilities.TokenUsage {
		t.Fatalf("capabilities = %+v", capabilities)
	}
	if capabilities.ModelCalls || capabilities.CostMetadata || capabilities.OTel || capabilities.ExternalIO {
		t.Fatalf("adapter overclaims capabilities: %+v", capabilities)
	}

	hints, err := instance.RedactionHints(context.Background(), adapter.RunContext{})
	if err != nil {
		t.Fatal(err)
	}
	if len(hints) < 3 {
		t.Fatalf("redaction hints = %+v", hints)
	}
	for _, hint := range hints {
		if hint.Kind != adapter.RedactEnvironmentName {
			t.Fatalf("unexpected hint = %+v", hint)
		}
	}
}
