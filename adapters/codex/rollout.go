package codex

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/rappidAI-Research/rappid-replay/pkg/adapter"
	_ "modernc.org/sqlite"
)

const (
	codexStateDBName     = "state_5.sqlite"
	maxRolloutLineBytes  = 4 << 20
	maxNormalizedText    = 256 << 10
	candidateClockWindow = 5 * time.Second
)

type codexThread struct {
	ID              string
	RolloutPath     string
	Source          string
	ModelProvider   string
	Model           string
	ReasoningEffort string
	CreatedAt       int64
	UpdatedAt       int64
}

type rolloutLine struct {
	Timestamp string          `json:"timestamp"`
	Type      string          `json:"type"`
	Payload   json.RawMessage `json:"payload"`
}

func observeRollout(ctx context.Context, codexHome string, run adapter.RunContext, interval time.Duration, emit adapter.EventEmitter) error {
	if emit == nil {
		return fmt.Errorf("Codex event emitter is required")
	}
	started := time.Now().UTC()
	mode, _ := invocationMode(run.Command)
	resumeID := explicitResumeThreadID(run.Command)

	thread, err := waitForThread(ctx, codexHome, run.WorkingDir, mode, resumeID, started, interval)
	if err != nil || thread == nil {
		return err
	}
	rolloutPath, err := validateRolloutPath(codexHome, thread.RolloutPath)
	if err != nil {
		return fmt.Errorf("validate Codex rollout path: %w", err)
	}

	if err := emit(ctx, adapter.AdapterEvent{Type: "agent.codex.session", Payload: mustJSON(struct {
		ThreadID        string `json:"thread_id"`
		Source          string `json:"source,omitempty"`
		ModelProvider   string `json:"model_provider,omitempty"`
		Model           string `json:"model,omitempty"`
		ReasoningEffort string `json:"reasoning_effort,omitempty"`
	}{
		ThreadID: thread.ID, Source: thread.Source, ModelProvider: thread.ModelProvider,
		Model: thread.Model, ReasoningEffort: thread.ReasoningEffort,
	})}); err != nil {
		return err
	}

	return tailRollout(ctx, rolloutPath, started.Add(-candidateClockWindow), interval, emit)
}

func waitForThread(ctx context.Context, home, cwd, mode, resumeID string, started time.Time, interval time.Duration) (*codexThread, error) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		thread, found, err := selectThread(home, cwd, mode, resumeID, started)
		if err != nil {
			return nil, err
		}
		if found {
			return thread, nil
		}
		select {
		case <-ctx.Done():
			return nil, nil
		case <-ticker.C:
		}
	}
}

func selectThread(home, cwd, mode, resumeID string, started time.Time) (*codexThread, bool, error) {
	dbPath := filepath.Join(home, codexStateDBName)
	if _, err := os.Stat(dbPath); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("stat Codex state database: %w", err)
	}

	db, err := sql.Open("sqlite", sqliteReadOnlyDSN(dbPath))
	if err != nil {
		return nil, false, fmt.Errorf("open Codex state database: %w", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	queryCtx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	if err := db.PingContext(queryCtx); err != nil {
		return nil, false, fmt.Errorf("read Codex state database: %w", err)
	}

	threshold := started.Add(-candidateClockWindow).Unix()
	if resumeID != "" {
		thread, found, err := queryExactThread(queryCtx, db, resumeID, cwd, threshold)
		return thread, found, err
	}

	threads, err := queryCandidateThreads(queryCtx, db, cwd, mode, threshold)
	if err != nil {
		return nil, false, err
	}
	if len(threads) == 0 {
		return nil, false, nil
	}
	if len(threads) > 1 {
		return nil, false, fmt.Errorf("Codex rollout correlation is ambiguous: %d recent threads match the working directory", len(threads))
	}
	return &threads[0], true, nil
}

func queryExactThread(ctx context.Context, db *sql.DB, id, cwd string, threshold int64) (*codexThread, bool, error) {
	row := db.QueryRowContext(ctx, `
SELECT id, rollout_path, source, model_provider, COALESCE(model, ''), COALESCE(reasoning_effort, ''), created_at, updated_at
FROM threads
WHERE id = ? AND cwd = ? AND archived = 0 AND updated_at >= ?
LIMIT 1`, id, cwd, threshold)
	thread, err := scanThread(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("query resumed Codex thread: %w", err)
	}
	return &thread, true, nil
}

func queryCandidateThreads(ctx context.Context, db *sql.DB, cwd, mode string, threshold int64) ([]codexThread, error) {
	predicate := "created_at >= ?"
	if mode == "resume" {
		predicate = "updated_at >= ?"
	}
	rows, err := db.QueryContext(ctx, `
SELECT id, rollout_path, source, model_provider, COALESCE(model, ''), COALESCE(reasoning_effort, ''), created_at, updated_at
FROM threads
WHERE cwd = ? AND archived = 0 AND `+predicate+`
ORDER BY updated_at DESC, id DESC
LIMIT 3`, cwd, threshold)
	if err != nil {
		return nil, fmt.Errorf("query Codex thread candidates: %w", err)
	}
	defer rows.Close()

	var out []codexThread
	for rows.Next() {
		thread, err := scanThread(rows)
		if err != nil {
			return nil, fmt.Errorf("scan Codex thread candidate: %w", err)
		}
		out = append(out, thread)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate Codex thread candidates: %w", err)
	}
	return out, nil
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanThread(row rowScanner) (codexThread, error) {
	var thread codexThread
	err := row.Scan(
		&thread.ID, &thread.RolloutPath, &thread.Source, &thread.ModelProvider,
		&thread.Model, &thread.ReasoningEffort, &thread.CreatedAt, &thread.UpdatedAt,
	)
	return thread, err
}

func sqliteReadOnlyDSN(path string) string {
	slashPath := filepath.ToSlash(path)
	if runtime.GOOS == "windows" && len(slashPath) >= 2 && slashPath[1] == ':' {
		slashPath = "/" + slashPath
	}
	u := url.URL{Scheme: "file", Path: slashPath}
	q := u.Query()
	q.Set("mode", "ro")
	q.Set("_busy_timeout", "250")
	u.RawQuery = q.Encode()
	return u.String()
}

func validateRolloutPath(home, raw string) (string, error) {
	candidate := strings.TrimSpace(raw)
	if strings.HasPrefix(candidate, `\\?\`) {
		candidate = strings.TrimPrefix(candidate, `\\?\`)
	}
	if !filepath.IsAbs(candidate) {
		return "", fmt.Errorf("rollout path is not absolute")
	}
	candidate = filepath.Clean(candidate)
	root, err := filepath.Abs(filepath.Join(home, "sessions"))
	if err != nil {
		return "", err
	}
	if !pathWithin(root, candidate) {
		return "", fmt.Errorf("rollout path escapes Codex sessions directory")
	}

	resolvedRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		return "", fmt.Errorf("resolve Codex sessions directory: %w", err)
	}
	resolvedCandidate, err := filepath.EvalSymlinks(candidate)
	if err != nil {
		return "", fmt.Errorf("resolve Codex rollout: %w", err)
	}
	if !pathWithin(resolvedRoot, resolvedCandidate) {
		return "", fmt.Errorf("resolved rollout path escapes Codex sessions directory")
	}
	info, err := os.Stat(resolvedCandidate)
	if err != nil {
		return "", err
	}
	if !info.Mode().IsRegular() {
		return "", fmt.Errorf("rollout path is not a regular file")
	}
	return resolvedCandidate, nil
}

func pathWithin(root, candidate string) bool {
	rel, err := filepath.Rel(root, candidate)
	if err != nil || rel == ".." || filepath.IsAbs(rel) {
		return false
	}
	return !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

func tailRollout(ctx context.Context, path string, notBefore time.Time, interval time.Duration, emit adapter.EventEmitter) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open Codex rollout: %w", err)
	}
	defer file.Close()

	buffer := make([]byte, 64<<10)
	pending := make([]byte, 0, 64<<10)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	drain := func(emitCtx context.Context) error {
		for {
			n, readErr := file.Read(buffer)
			if n > 0 {
				pending = append(pending, buffer[:n]...)
				if len(pending) > maxRolloutLineBytes && !bytes.Contains(pending, []byte{'\n'}) {
					return fmt.Errorf("Codex rollout line exceeds %d bytes", maxRolloutLineBytes)
				}
				for {
					newline := bytes.IndexByte(pending, '\n')
					if newline < 0 {
						break
					}
					line := bytes.Clone(bytes.TrimSpace(pending[:newline]))
					pending = append(pending[:0], pending[newline+1:]...)
					if len(line) == 0 {
						continue
					}
					if len(line) > maxRolloutLineBytes {
						return fmt.Errorf("Codex rollout line exceeds %d bytes", maxRolloutLineBytes)
					}
					if err := emitRolloutLine(emitCtx, line, notBefore, emit); err != nil {
						return err
					}
				}
			}
			if readErr == nil {
				continue
			}
			if errors.Is(readErr, io.EOF) {
				return nil
			}
			return fmt.Errorf("read Codex rollout: %w", readErr)
		}
	}

	if err := drain(ctx); err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			// The process has stopped. Perform one bounded final read so lines written
			// immediately before exit are not lost, then stop without waiting.
			return drain(context.WithoutCancel(ctx))
		case <-ticker.C:
			if err := drain(ctx); err != nil {
				return err
			}
		}
	}
}

func emitRolloutLine(ctx context.Context, raw []byte, notBefore time.Time, emit adapter.EventEmitter) error {
	var line rolloutLine
	if err := json.Unmarshal(raw, &line); err != nil {
		return fmt.Errorf("decode Codex rollout line: %w", err)
	}
	if line.Timestamp != "" {
		if timestamp, err := time.Parse(time.RFC3339Nano, line.Timestamp); err == nil && timestamp.Before(notBefore) {
			return nil
		}
	}

	switch line.Type {
	case "response_item":
		return emitResponseItem(ctx, line.Payload, emit)
	case "event_msg":
		return emitEventMessage(ctx, line.Payload, emit)
	default:
		// Rollout schemas evolve. Unknown or non-semantic records are ignored
		// instead of becoming a dependency of deterministic recording.
		return nil
	}
}

func emitResponseItem(ctx context.Context, payload json.RawMessage, emit adapter.EventEmitter) error {
	var item struct {
		Type                  string          `json:"type"`
		Role                  string          `json:"role"`
		Phase                 string          `json:"phase"`
		Content               []contentItem   `json:"content"`
		Name                  string          `json:"name"`
		Namespace             string          `json:"namespace"`
		Arguments             string          `json:"arguments"`
		Input                 string          `json:"input"`
		CallID                string          `json:"call_id"`
		Output                json.RawMessage `json:"output"`
		EncryptedFunctionArgs json.RawMessage `json:"encrypted_function_args"`
	}
	if err := json.Unmarshal(payload, &item); err != nil {
		return fmt.Errorf("decode Codex response item: %w", err)
	}

	switch item.Type {
	case "reasoning", "compaction", "context_compaction":
		// Never persist private reasoning, reasoning summaries, or encrypted
		// compaction state through the adapter.
		return nil
	case "message":
		if item.Role != "assistant" {
			return nil
		}
		var parts []string
		for _, content := range item.Content {
			if content.Type == "output_text" && content.Text != "" {
				parts = append(parts, content.Text)
			}
		}
		if len(parts) == 0 {
			return nil
		}
		text, truncated, original := truncateText(strings.Join(parts, "\n"))
		return emitJSON(ctx, emit, "agent.message", struct {
			Provider      string `json:"provider"`
			Role          string `json:"role"`
			Text          string `json:"text"`
			Phase         string `json:"phase,omitempty"`
			Truncated     bool   `json:"truncated,omitempty"`
			OriginalBytes int    `json:"original_bytes,omitempty"`
		}{Provider: "codex", Role: "assistant", Text: text, Phase: item.Phase, Truncated: truncated, OriginalBytes: original})
	case "function_call":
		arguments, truncated, original := truncateText(item.Arguments)
		return emitJSON(ctx, emit, "agent.tool_call", struct {
			Provider      string `json:"provider"`
			Kind          string `json:"kind"`
			Name          string `json:"name"`
			Namespace     string `json:"namespace,omitempty"`
			CallID        string `json:"call_id,omitempty"`
			Arguments     string `json:"arguments"`
			Truncated     bool   `json:"truncated,omitempty"`
			OriginalBytes int    `json:"original_bytes,omitempty"`
		}{Provider: "codex", Kind: "function_call", Name: item.Name, Namespace: item.Namespace, CallID: item.CallID, Arguments: arguments, Truncated: truncated, OriginalBytes: original})
	case "custom_tool_call":
		input, truncated, original := truncateText(item.Input)
		return emitJSON(ctx, emit, "agent.tool_call", struct {
			Provider      string `json:"provider"`
			Kind          string `json:"kind"`
			Name          string `json:"name"`
			CallID        string `json:"call_id,omitempty"`
			Input         string `json:"input"`
			Truncated     bool   `json:"truncated,omitempty"`
			OriginalBytes int    `json:"original_bytes,omitempty"`
		}{Provider: "codex", Kind: "custom_tool_call", Name: item.Name, CallID: item.CallID, Input: input, Truncated: truncated, OriginalBytes: original})
	case "function_call_output", "custom_tool_call_output":
		text := safeToolOutput(item.Output)
		if text == "" {
			return nil
		}
		text, truncated, original := truncateText(text)
		return emitJSON(ctx, emit, "agent.tool_result", struct {
			Provider      string `json:"provider"`
			Kind          string `json:"kind"`
			CallID        string `json:"call_id,omitempty"`
			Output        string `json:"output"`
			Truncated     bool   `json:"truncated,omitempty"`
			OriginalBytes int    `json:"original_bytes,omitempty"`
		}{Provider: "codex", Kind: item.Type, CallID: item.CallID, Output: text, Truncated: truncated, OriginalBytes: original})
	default:
		return nil
	}
}

type contentItem struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

func emitEventMessage(ctx context.Context, payload json.RawMessage, emit adapter.EventEmitter) error {
	var envelope struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return fmt.Errorf("decode Codex event message: %w", err)
	}
	if envelope.Type != "token_count" {
		return nil
	}

	var value any
	if err := json.Unmarshal(payload, &value); err != nil {
		return fmt.Errorf("decode Codex token usage: %w", err)
	}
	usage := map[string]int64{}
	collectTokenFields(value, usage)
	if len(usage) == 0 {
		return nil
	}
	return emitJSON(ctx, emit, "agent.usage", struct {
		Provider string           `json:"provider"`
		Tokens   map[string]int64 `json:"tokens"`
	}{Provider: "codex", Tokens: usage})
}

var allowedTokenFields = map[string]struct{}{
	"input_tokens": {}, "cached_input_tokens": {}, "cache_write_input_tokens": {},
	"output_tokens": {}, "reasoning_output_tokens": {}, "total_tokens": {},
	"model_context_window": {},
}

func collectTokenFields(value any, out map[string]int64) {
	switch typed := value.(type) {
	case map[string]any:
		keys := make([]string, 0, len(typed))
		for key := range typed {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			child := typed[key]
			if _, ok := allowedTokenFields[key]; ok {
				if number, ok := jsonNumberInt64(child); ok {
					out[key] = number
				}
			}
			collectTokenFields(child, out)
		}
	case []any:
		for _, child := range typed {
			collectTokenFields(child, out)
		}
	}
}

func jsonNumberInt64(value any) (int64, bool) {
	switch number := value.(type) {
	case float64:
		if number < 0 || number != float64(int64(number)) {
			return 0, false
		}
		return int64(number), true
	case json.Number:
		parsed, err := strconv.ParseInt(number.String(), 10, 64)
		return parsed, err == nil && parsed >= 0
	default:
		return 0, false
	}
}

func safeToolOutput(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var text string
	if json.Unmarshal(raw, &text) == nil {
		return text
	}
	var items []contentItem
	if json.Unmarshal(raw, &items) == nil {
		parts := make([]string, 0, len(items))
		for _, item := range items {
			if (item.Type == "input_text" || item.Type == "output_text") && item.Text != "" {
				parts = append(parts, item.Text)
			}
		}
		return strings.Join(parts, "\n")
	}
	return ""
}

func truncateText(value string) (string, bool, int) {
	original := len(value)
	if original <= maxNormalizedText {
		return value, false, 0
	}
	return value[:maxNormalizedText], true, original
}

func emitJSON(ctx context.Context, emit adapter.EventEmitter, eventType string, payload any) error {
	encoded, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return emit(ctx, adapter.AdapterEvent{Type: eventType, Payload: encoded})
}

func mustJSON(value any) json.RawMessage {
	encoded, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return encoded
}

func explicitResumeThreadID(command []string) string {
	for i, raw := range command {
		if !strings.EqualFold(strings.TrimSpace(raw), "resume") {
			continue
		}
		for _, candidate := range command[i+1:] {
			candidate = strings.TrimSpace(candidate)
			if candidate == "" || strings.HasPrefix(candidate, "-") {
				continue
			}
			if looksLikeThreadID(candidate) {
				return candidate
			}
			return ""
		}
	}
	return ""
}

func looksLikeThreadID(value string) bool {
	if len(value) < 32 || len(value) > 40 {
		return false
	}
	for _, r := range value {
		if (r >= '0' && r <= '9') || (r >= 'a' && r <= 'f') || (r >= 'A' && r <= 'F') || r == '-' {
			continue
		}
		return false
	}
	return true
}
