package record

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/rappidAI-Research/rappid-replay/adapters/generic"
	"github.com/rappidAI-Research/rappid-replay/internal/persistence"
	"github.com/rappidAI-Research/rappid-replay/internal/store"
	"github.com/rappidAI-Research/rappid-replay/pkg/adapter"
)

const testAdapterSecret = "adapter-secret-value"

type recordingTestAdapter struct{}

func (recordingTestAdapter) Descriptor() adapter.Descriptor {
	return adapter.Descriptor{ID: "test-agent", Version: "3.2.1"}
}

func (recordingTestAdapter) Detect(context.Context, adapter.DetectionInput) (adapter.Detection, error) {
	return adapter.Detection{Matched: true, Confidence: 100, Reason: "test adapter"}, nil
}

func (recordingTestAdapter) Capabilities() adapter.Capabilities {
	return adapter.Capabilities{Messages: true}
}

func (recordingTestAdapter) EnrichProcess(context.Context, adapter.RunContext, adapter.ProcessObservation) (adapter.ProcessEnrichment, error) {
	return adapter.ProcessEnrichment{Attributes: map[string]string{
		"test-agent.role":   "observed",
		"test-agent.secret": testAdapterSecret,
	}}, nil
}

func (recordingTestAdapter) StreamEvents(ctx context.Context, _ adapter.RunContext, emit adapter.EventEmitter) error {
	return emit(ctx, adapter.AdapterEvent{
		Type:    "agent.message",
		Payload: json.RawMessage(`{"message":"adapter-secret-value"}`),
	})
}

func (recordingTestAdapter) Environment(context.Context, adapter.RunContext) (adapter.EnvironmentMetadata, error) {
	return adapter.EnvironmentMetadata{Attributes: map[string]string{
		"test-agent.mode": "integration-test",
	}}, nil
}

func (recordingTestAdapter) RedactionHints(context.Context, adapter.RunContext) ([]adapter.RedactionHint, error) {
	return []adapter.RedactionHint{
		{Kind: adapter.RedactEnvironmentName, Value: "TEST_AGENT_SECRET"},
		{Kind: adapter.RedactLiteral, Value: testAdapterSecret},
	}, nil
}

func TestGenericRecorderExecutesSelectedAdapterHooks(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "replay.db")
	db, err := persistence.Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	cas, err := store.NewLocalStore(t.TempDir(), make([]byte, 32))
	if err != nil {
		t.Fatal(err)
	}
	defer cas.Close()

	registry, err := adapter.NewRegistry(generic.New(), recordingTestAdapter{})
	if err != nil {
		t.Fatal(err)
	}
	workspace := t.TempDir()
	env := append(helperEnv(0), "TEST_AGENT_SECRET=plain-env-secret")
	result, err := Run(ctx, Dependencies{DB: db, CAS: cas, Adapters: registry}, Options{
		Command:       helperCommand(workspace),
		WorkingDir:    workspace,
		TerminalInput: "off",
		Env:           env,
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if result.AdapterID != "test-agent" || result.AdapterVersion != "3.2.1" {
		t.Fatalf("result adapter = %s@%s", result.AdapterID, result.AdapterVersion)
	}

	raw := openRawDB(t, dbPath)
	defer raw.Close()
	var adapterID, adapterVersion string
	if err := raw.QueryRowContext(ctx,
		"SELECT adapter_id, adapter_version FROM sessions WHERE id = ?", result.SessionID.String(),
	).Scan(&adapterID, &adapterVersion); err != nil {
		t.Fatal(err)
	}
	if adapterID != "test-agent" || adapterVersion != "3.2.1" {
		t.Fatalf("persisted adapter = %s@%s", adapterID, adapterVersion)
	}

	for _, eventType := range []string{"agent.environment", "agent.process.enriched", "agent.message"} {
		var count int
		if err := raw.QueryRowContext(ctx,
			"SELECT COUNT(1) FROM events WHERE session_id = ? AND type = ?", result.SessionID.String(), eventType,
		).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count == 0 {
			t.Fatalf("missing adapter hook event %s", eventType)
		}
	}

	var messagePayload, processPayload, environmentJSON []byte
	if err := raw.QueryRowContext(ctx,
		"SELECT payload_json FROM events WHERE session_id = ? AND type = 'agent.message' LIMIT 1", result.SessionID.String(),
	).Scan(&messagePayload); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(messagePayload), testAdapterSecret) || !strings.Contains(string(messagePayload), "[REDACTED]") {
		t.Fatalf("adapter event privacy filter failed: %s", messagePayload)
	}
	if err := raw.QueryRowContext(ctx,
		"SELECT payload_json FROM events WHERE session_id = ? AND type = 'agent.process.enriched' LIMIT 1", result.SessionID.String(),
	).Scan(&processPayload); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(processPayload), testAdapterSecret) || !strings.Contains(string(processPayload), "[REDACTED]") {
		t.Fatalf("process enrichment privacy filter failed: %s", processPayload)
	}
	if err := raw.QueryRowContext(ctx,
		"SELECT fingerprint_json FROM environments WHERE session_id = ?", result.SessionID.String(),
	).Scan(&environmentJSON); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(environmentJSON), "plain-env-secret") || !strings.Contains(string(environmentJSON), `"TEST_AGENT_SECRET"`) {
		t.Fatalf("adapter environment-name hint not applied: %s", environmentJSON)
	}
}

type failingHookAdapter struct{}

func (failingHookAdapter) Descriptor() adapter.Descriptor {
	return adapter.Descriptor{ID: "broken-agent", Version: "1.0.0"}
}
func (failingHookAdapter) Detect(context.Context, adapter.DetectionInput) (adapter.Detection, error) {
	return adapter.Detection{Matched: true, Confidence: 100}, nil
}
func (failingHookAdapter) Capabilities() adapter.Capabilities {
	return adapter.Capabilities{Messages: true}
}
func (failingHookAdapter) EnrichProcess(context.Context, adapter.RunContext, adapter.ProcessObservation) (adapter.ProcessEnrichment, error) {
	return adapter.ProcessEnrichment{}, errors.New("process hook failed")
}
func (failingHookAdapter) StreamEvents(context.Context, adapter.RunContext, adapter.EventEmitter) error {
	return errors.New("stream hook failed")
}
func (failingHookAdapter) Environment(context.Context, adapter.RunContext) (adapter.EnvironmentMetadata, error) {
	return adapter.EnvironmentMetadata{}, errors.New("environment hook failed")
}
func (failingHookAdapter) RedactionHints(context.Context, adapter.RunContext) ([]adapter.RedactionHint, error) {
	return nil, errors.New("redaction hook failed")
}

func TestAdapterHookFailuresDoNotGateGenericRecording(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "replay.db")
	db, err := persistence.Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	cas, err := store.NewLocalStore(t.TempDir(), make([]byte, 32))
	if err != nil {
		t.Fatal(err)
	}
	defer cas.Close()
	registry, err := adapter.NewRegistry(generic.New(), failingHookAdapter{})
	if err != nil {
		t.Fatal(err)
	}
	workspace := t.TempDir()
	result, err := Run(ctx, Dependencies{DB: db, CAS: cas, Adapters: registry}, Options{
		Command: helperCommand(workspace), WorkingDir: workspace, TerminalInput: "off", Env: helperEnv(0),
	})
	if err != nil {
		t.Fatalf("generic recording was gated by adapter hook failure: %v", err)
	}

	raw := openRawDB(t, dbPath)
	defer raw.Close()
	var status string
	if err := raw.QueryRowContext(ctx, "SELECT status FROM sessions WHERE id = ?", result.SessionID.String()).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "completed" {
		t.Fatalf("session status = %q, want completed", status)
	}
	var diagnostics int
	if err := raw.QueryRowContext(ctx,
		"SELECT COUNT(1) FROM events WHERE session_id = ? AND type = 'agent.adapter.error'", result.SessionID.String(),
	).Scan(&diagnostics); err != nil {
		t.Fatal(err)
	}
	if diagnostics < 3 {
		t.Fatalf("adapter hook diagnostics = %d, want at least 3", diagnostics)
	}
}
