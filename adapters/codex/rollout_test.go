package codex

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rappidAI-Research/rappid-replay/pkg/adapter"
)

type capturedEvent struct {
	typeName string
	payload  []byte
}

func TestRolloutNormalizationSkipsReasoningAndEncryptedArguments(t *testing.T) {
	now := time.Now().UTC()
	var captured []capturedEvent
	emit := func(_ context.Context, event adapter.AdapterEvent) error {
		captured = append(captured, capturedEvent{typeName: event.Type, payload: append([]byte(nil), event.Payload...)})
		return nil
	}

	lines := []map[string]any{
		{
			"timestamp": now.Format(time.RFC3339Nano), "type": "response_item",
			"payload": map[string]any{
				"type": "reasoning", "summary": []any{map[string]any{"type": "summary_text", "text": "private reasoning"}},
				"encrypted_content": "opaque-private-reasoning",
			},
		},
		{
			"timestamp": now.Format(time.RFC3339Nano), "type": "response_item",
			"payload": map[string]any{
				"type": "message", "role": "assistant", "phase": "final_answer",
				"content": []any{map[string]any{"type": "output_text", "text": "hello from Codex"}},
			},
		},
		{
			"timestamp": now.Format(time.RFC3339Nano), "type": "response_item",
			"payload": map[string]any{
				"type": "function_call", "name": "shell", "call_id": "call-1", "arguments": "{\"cmd\":\"go test ./...\"}",
				"encrypted_function_args": "never-persist-this",
			},
		},
		{
			"timestamp": now.Format(time.RFC3339Nano), "type": "event_msg",
			"payload": map[string]any{
				"type": "token_count",
				"info": map[string]any{"total_token_usage": map[string]any{
					"input_tokens": 12, "cached_input_tokens": 3, "output_tokens": 5, "reasoning_output_tokens": 2, "total_tokens": 17,
				}},
			},
		},
	}

	for _, value := range lines {
		raw, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		if err := emitRolloutLine(context.Background(), raw, now.Add(-time.Second), emit); err != nil {
			t.Fatal(err)
		}
	}

	if len(captured) != 3 {
		t.Fatalf("captured %d events, want 3: %+v", len(captured), captured)
	}
	if captured[0].typeName != "agent.message" || !strings.Contains(string(captured[0].payload), "hello from Codex") {
		t.Fatalf("message event = %+v", captured[0])
	}
	if captured[1].typeName != "agent.tool_call" || strings.Contains(string(captured[1].payload), "never-persist-this") {
		t.Fatalf("tool event leaked encrypted args: %+v", captured[1])
	}
	if captured[2].typeName != "agent.usage" || !strings.Contains(string(captured[2].payload), `"input_tokens":12`) {
		t.Fatalf("usage event = %+v", captured[2])
	}
	for _, event := range captured {
		text := string(event.payload)
		if strings.Contains(text, "private reasoning") || strings.Contains(text, "opaque-private-reasoning") {
			t.Fatalf("reasoning leaked in %s: %s", event.typeName, text)
		}
	}
}

func TestRolloutIgnoresOldAndUnknownRecords(t *testing.T) {
	now := time.Now().UTC()
	count := 0
	emit := func(context.Context, adapter.AdapterEvent) error { count++; return nil }
	for _, value := range []map[string]any{
		{"timestamp": now.Add(-time.Hour).Format(time.RFC3339Nano), "type": "response_item", "payload": map[string]any{"type": "message", "role": "assistant", "content": []any{map[string]any{"type": "output_text", "text": "old"}}}},
		{"timestamp": now.Format(time.RFC3339Nano), "type": "future_rollout_item", "payload": map[string]any{"anything": true}},
	} {
		raw, _ := json.Marshal(value)
		if err := emitRolloutLine(context.Background(), raw, now.Add(-time.Second), emit); err != nil {
			t.Fatal(err)
		}
	}
	if count != 0 {
		t.Fatalf("emitted %d events for old/unknown records", count)
	}
}

func TestValidateRolloutPathConfinesReadsToSessions(t *testing.T) {
	home := t.TempDir()
	sessions := filepath.Join(home, "sessions", "2026", "08", "29")
	if err := os.MkdirAll(sessions, 0o700); err != nil {
		t.Fatal(err)
	}
	inside := filepath.Join(sessions, "rollout-test.jsonl")
	if err := os.WriteFile(inside, []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	resolved, err := validateRolloutPath(home, inside)
	if err != nil {
		t.Fatalf("valid rollout rejected: %v", err)
	}
	expected, err := filepath.EvalSymlinks(inside)
	if err != nil {
		t.Fatal(err)
	}
	if filepath.Clean(resolved) != filepath.Clean(expected) {
		t.Fatalf("resolved path = %q, want %q", resolved, expected)
	}

	outside := filepath.Join(home, "outside.jsonl")
	if err := os.WriteFile(outside, []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := validateRolloutPath(home, outside); err == nil {
		t.Fatal("outside rollout path was accepted")
	}

	if runtime.GOOS != "windows" {
		link := filepath.Join(sessions, "escaped.jsonl")
		if err := os.Symlink(outside, link); err != nil {
			t.Fatal(err)
		}
		if _, err := validateRolloutPath(home, link); err == nil {
			t.Fatal("symlink escape was accepted")
		}
	}
}

func TestSelectThreadRejectsAmbiguousSameDirectoryCandidates(t *testing.T) {
	home := t.TempDir()
	cwd := t.TempDir()
	createCodexStateDB(t, home)
	now := time.Now().Unix()
	insertCodexThread(t, home, codexThread{ID: "thread-a", RolloutPath: filepath.Join(home, "sessions", "a.jsonl"), Source: "cli", ModelProvider: "openai", CreatedAt: now, UpdatedAt: now}, cwd)
	insertCodexThread(t, home, codexThread{ID: "thread-b", RolloutPath: filepath.Join(home, "sessions", "b.jsonl"), Source: "cli", ModelProvider: "openai", CreatedAt: now, UpdatedAt: now}, cwd)

	_, _, err := selectThread(home, cwd, "interactive", "", time.Now().Add(-time.Second))
	if err == nil || !strings.Contains(err.Error(), "ambiguous") {
		t.Fatalf("selectThread() error = %v, want ambiguity", err)
	}
}

func TestStreamEventsCorrelatesStateDBAndRollout(t *testing.T) {
	home := t.TempDir()
	cwd := t.TempDir()
	rolloutDir := filepath.Join(home, "sessions", "2026", "08", "29")
	if err := os.MkdirAll(rolloutDir, 0o700); err != nil {
		t.Fatal(err)
	}
	rollout := filepath.Join(rolloutDir, "rollout-test.jsonl")
	now := time.Now().UTC()
	line, err := json.Marshal(map[string]any{
		"timestamp": now.Format(time.RFC3339Nano), "type": "response_item",
		"payload": map[string]any{"type": "message", "role": "assistant", "content": []any{map[string]any{"type": "output_text", "text": "structured hello"}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(rollout, append(line, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
	createCodexStateDB(t, home)
	insertCodexThread(t, home, codexThread{
		ID: "018f1234-1234-7123-8123-123456789abc", RolloutPath: rollout, Source: "cli", ModelProvider: "openai",
		Model: "gpt-test", ReasoningEffort: "medium", CreatedAt: now.Unix(), UpdatedAt: now.Unix(),
	}, cwd)

	instance := New()
	instance.homeDir = func() (string, error) { return home, nil }
	instance.pollInterval = 5 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var mu sync.Mutex
	var captured []capturedEvent
	done := make(chan error, 1)
	go func() {
		done <- instance.StreamEvents(ctx, adapter.RunContext{Command: []string{"codex"}, WorkingDir: cwd}, func(_ context.Context, event adapter.AdapterEvent) error {
			mu.Lock()
			captured = append(captured, capturedEvent{typeName: event.Type, payload: append([]byte(nil), event.Payload...)})
			count := len(captured)
			mu.Unlock()
			if count >= 2 {
				cancel()
			}
			return nil
		})
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("StreamEvents did not stop")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(captured) < 2 || captured[0].typeName != "agent.codex.session" || captured[1].typeName != "agent.message" {
		t.Fatalf("captured events = %+v", captured)
	}
	if strings.Contains(string(captured[0].payload), home) || strings.Contains(string(captured[0].payload), rollout) {
		t.Fatalf("session metadata leaked local path: %s", captured[0].payload)
	}
}

func createCodexStateDB(t *testing.T, home string) {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(home, codexStateDBName))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, err = db.Exec(`
CREATE TABLE threads (
    id TEXT PRIMARY KEY,
    rollout_path TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    source TEXT NOT NULL,
    model_provider TEXT NOT NULL,
    cwd TEXT NOT NULL,
    archived INTEGER NOT NULL DEFAULT 0,
    model TEXT,
    reasoning_effort TEXT
);`)
	if err != nil {
		t.Fatal(err)
	}
}

func insertCodexThread(t *testing.T, home string, thread codexThread, cwd string) {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(home, codexStateDBName))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, err = db.Exec(`
INSERT INTO threads(id, rollout_path, created_at, updated_at, source, model_provider, cwd, archived, model, reasoning_effort)
VALUES(?, ?, ?, ?, ?, ?, ?, 0, ?, ?)`,
		thread.ID, thread.RolloutPath, thread.CreatedAt, thread.UpdatedAt, thread.Source, thread.ModelProvider, cwd, thread.Model, thread.ReasoningEffort,
	)
	if err != nil {
		t.Fatal(err)
	}
}
