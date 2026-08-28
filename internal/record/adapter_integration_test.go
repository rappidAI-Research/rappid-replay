package record

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/rappidAI-Research/rappid-replay/adapters/generic"
	"github.com/rappidAI-Research/rappid-replay/internal/persistence"
	"github.com/rappidAI-Research/rappid-replay/internal/store"
	"github.com/rappidAI-Research/rappid-replay/pkg/adapter"
)

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
	return adapter.ProcessEnrichment{}, nil
}

func (recordingTestAdapter) StreamEvents(context.Context, adapter.RunContext, adapter.EventEmitter) error {
	return nil
}

func (recordingTestAdapter) Environment(context.Context, adapter.RunContext) (adapter.EnvironmentMetadata, error) {
	return adapter.EnvironmentMetadata{}, nil
}

func (recordingTestAdapter) RedactionHints(context.Context, adapter.RunContext) ([]adapter.RedactionHint, error) {
	return nil, nil
}

func TestGenericRecorderPersistsSelectedAdapterIdentity(t *testing.T) {
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
	result, err := Run(ctx, Dependencies{DB: db, CAS: cas, Adapters: registry}, Options{
		Command:       helperCommand(workspace),
		WorkingDir:    workspace,
		TerminalInput: "off",
		Env:           helperEnv(0),
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

	var payloadBytes []byte
	if err := raw.QueryRowContext(ctx, `
SELECT payload_json FROM events
WHERE session_id = ? AND type = 'session.started'`, result.SessionID.String()).Scan(&payloadBytes); err != nil {
		t.Fatal(err)
	}
	var payload struct {
		Recorder         string             `json:"recorder"`
		Adapter          adapter.Descriptor `json:"adapter"`
		AdapterDetection adapter.Detection  `json:"adapter_detection"`
	}
	if err := json.Unmarshal(payloadBytes, &payload); err != nil {
		t.Fatal(err)
	}
	if payload.Recorder != "generic" || payload.Adapter.ID != "test-agent" || payload.AdapterDetection.Confidence != 100 {
		t.Fatalf("session.started adapter payload = %+v", payload)
	}
}
