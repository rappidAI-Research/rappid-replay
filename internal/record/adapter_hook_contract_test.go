package record

import (
	"strings"
	"testing"

	"github.com/rappidAI-Research/rappid-replay/pkg/adapter"
)

func TestAdapterSelectionMetadataIsRedactedBeforePersistence(t *testing.T) {
	secret := "github_pat_ABCDEFGHIJKLMNOPQRSTUVWXYZ123456"
	selection := adapter.Selection{
		Descriptor: adapter.Descriptor{ID: "test-agent", Version: "1.0.0"},
		Detection:  adapter.Detection{Matched: true, Confidence: 100, Reason: "detected with " + secret},
		Diagnostics: []adapter.Diagnostic{{
			AdapterID: "other-agent",
			Stage:     "detect",
			Message:   "detector failed with " + secret,
		}},
	}

	redacted, changed := redactAdapterSelectionForPersistence(selection)
	if !changed {
		t.Fatal("adapter selection privacy filter did not report redaction")
	}
	if strings.Contains(redacted.Detection.Reason, secret) || !strings.Contains(redacted.Detection.Reason, "[REDACTED]") {
		t.Fatalf("detection reason leaked secret: %q", redacted.Detection.Reason)
	}
	if strings.Contains(redacted.Diagnostics[0].Message, secret) || !strings.Contains(redacted.Diagnostics[0].Message, "[REDACTED]") {
		t.Fatalf("diagnostic leaked secret: %q", redacted.Diagnostics[0].Message)
	}
	if selection.Diagnostics[0].Message != "detector failed with "+secret {
		t.Fatal("redaction mutated caller-owned diagnostics")
	}
}

func TestAdapterHookEventsUseAdapterSourceIdentity(t *testing.T) {
	bridge := &adapterHookBridge{desc: adapter.Descriptor{ID: "codex", Version: "1.0.0"}}
	if got := bridge.source(); got != "adapter.codex" {
		t.Fatalf("adapter source = %q, want adapter.codex", got)
	}
}
